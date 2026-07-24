#!/usr/bin/env python3
"""12 aylık donmuş pump tanımı taraması; resmî Binance checksum zorunludur."""
from __future__ import annotations
import concurrent.futures as cf
import hashlib
import io
import json
import re
import sys
import time
import zipfile
from pathlib import Path
import numpy as np
import pandas as pd
import requests

BASE=Path(__file__).resolve().parent; DATA=BASE/'data'; ART=BASE/'artifacts'
DATA.mkdir(exist_ok=True); ART.mkdir(exist_ok=True)
S3_LIST='https://s3-ap-northeast-1.amazonaws.com/data.binance.vision'
DL='https://data.binance.vision/data/spot/monthly/klines/{sym}/5m/{sym}-5m-{month}.zip'
CHECKSUM=DL+'.CHECKSUM'
WARMUP_MONTHS=['2025-06']
SCAN_MONTHS=['2025-07','2025-08','2025-09','2025-10','2025-11','2025-12','2026-01','2026-02','2026-03','2026-04','2026-05','2026-06']
ALL_MONTHS=WARMUP_MONTHS+SCAN_MONTHS
SCAN_START_MS=1751328000000; SCAN_END_MS=1782864000000
BAR_MS=300_000; IGN_WIN=3; IGN_GAIN=.03; FWD_BARS=144; PRIOR_6H=72; PRIOR_24H=288
VOL_BLOCK=144; VOL_BLOCKS_TARGET=60; VOL_MIN_BLOCKS=45; BRAKE_MULT=3.; LIQ_MIN_USDT=100_000.
THRESHOLDS=[.05,.10,.15,.20,.30]; SUPPRESS_MS=12*3600*1000
KNOWN_LEV_CORES={'1INCH','AAVE','ADA','BCH','BNB','BTC','DOT','EOS','ETH','FIL','LINK','LTC','SUSHI','SXP','TRX','UNI','XLM','XRP','XTZ','YFI'}
BARE_LEV={'BULL','BEAR'}
STABLE_FIAT_BASES={'USDC','TUSD','BUSD','DAI','FDUSD','USDP','SUSD','UST','USTC','VAI','USDE','USDS','USDSB','USDSOLD','RLUSD','AEUR','EURI','EUR','GBP','AUD','BRL','TRY','RUB','UAH','NGN','ZAR','BIDR','IDRT','BKRW','USD1','XUSD','BFUSD','PYUSD'}

def _is_leveraged(base):
    if base in BARE_LEV:return True
    return any(base.endswith(s) and base[:-len(s)] in KNOWN_LEV_CORES for s in ('UP','DOWN','BULL','BEAR'))

def list_universe():
    from urllib.parse import quote
    syms=[]; marker=''
    while True:
        url=f'{S3_LIST}?delimiter=/&prefix=data/spot/monthly/klines/'
        if marker:url+='&marker='+quote(marker,safe='')
        r=requests.get(url,timeout=30); r.raise_for_status(); xml=r.text
        prefixes=re.findall(r'<Prefix>data/spot/monthly/klines/([^<]+)/</Prefix>',xml); syms+=prefixes
        if '<IsTruncated>true</IsTruncated>' not in xml:break
        if not prefixes:raise RuntimeError('bad S3 pagination')
        marker=f'data/spot/monthly/klines/{prefixes[-1]}/'
    usdt=[s for s in syms if s.endswith('USDT')]; kept=[]; lev=[]; stable=[]
    for s in usdt:
        b=s[:-4]
        if _is_leveraged(b):lev.append(s)
        elif b in STABLE_FIAT_BASES:stable.append(s)
        else:kept.append(s)
    out={'generated_utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),'total_archive_symbols':len(syms),'usdt':len(usdt),'kept':sorted(set(kept)),'excluded_leveraged':sorted(set(lev)),'excluded_stable_fiat':sorted(set(stable)),'source_type':'REAL_OBSERVED'}
    (ART/'universe.json').write_text(json.dumps(out,ensure_ascii=False,indent=2)); print('universe',len(out['kept'])); return out['kept']

def _checksum_text(url):
    try:
        r=requests.get(url,timeout=30)
        if r.status_code==404:return None,'checksum_missing'
        r.raise_for_status(); token=r.text.split()[0].strip().lower()
        if not re.fullmatch(r'[0-9a-f]{64}',token):return None,'checksum_invalid'
        return token,'ok'
    except Exception as e:return None,f'checksum_error:{type(e).__name__}'

def _dl_one(sym,month):
    dest=DATA/sym/f'{sym}-5m-{month}.zip'; url=DL.format(sym=sym,month=month); cu=CHECKSUM.format(sym=sym,month=month)
    rec={'symbol':sym,'month':month,'source_url':url,'checksum_url':cu,'local_path':str(dest.relative_to(BASE)),'expected_sha256':None,'actual_sha256':None,'bytes':0,'status':'unknown'}
    expected,cs=_checksum_text(cu)
    if expected is None:rec['status']='SOURCE_UNAVAILABLE' if cs=='checksum_missing' else cs; return rec
    rec['expected_sha256']=expected
    if dest.exists() and dest.stat().st_size>0:
        actual=hashlib.sha256(dest.read_bytes()).hexdigest(); rec.update(actual_sha256=actual,bytes=dest.stat().st_size)
        if actual==expected:rec['status']='verified_cached'; return rec
        dest.unlink(missing_ok=True)
    dest.parent.mkdir(exist_ok=True)
    for attempt in range(1,4):
        try:
            r=requests.get(url,timeout=90)
            if r.status_code==404:rec['status']='SOURCE_UNAVAILABLE'; return rec
            r.raise_for_status(); actual=hashlib.sha256(r.content).hexdigest(); rec.update(actual_sha256=actual,bytes=len(r.content))
            if actual!=expected:
                rec['status']='checksum_mismatch'
                if attempt<3:time.sleep(2*attempt); continue
                return rec
            dest.write_bytes(r.content); rec['status']='downloaded_verified'; return rec
        except Exception as e:
            rec['status']=f'download_error:{type(e).__name__}'
            if attempt<3:time.sleep(2*attempt)
    return rec

def download():
    uni=json.loads((ART/'universe.json').read_text())['kept']; jobs=[(s,m) for s in uni for m in ALL_MONTHS]
    manifest=[]; counts={}; t0=time.time()
    with cf.ThreadPoolExecutor(max_workers=12) as ex:
        futs=[ex.submit(_dl_one,s,m) for s,m in jobs]
        for i,f in enumerate(cf.as_completed(futs),1):
            rec=f.result(); manifest.append(rec); counts[rec['status']]=counts.get(rec['status'],0)+1
            if i%300==0:print('download',i,len(jobs),counts,round(time.time()-t0),flush=True)
    manifest.sort(key=lambda r:(r['symbol'],r['month']))
    hard=[r for r in manifest if r['status'] not in {'downloaded_verified','verified_cached','SOURCE_UNAVAILABLE'}]
    report={'jobs':len(jobs),'status':counts,'verified_files':counts.get('downloaded_verified',0)+counts.get('verified_cached',0),'source_unavailable':counts.get('SOURCE_UNAVAILABLE',0),'hard_failure_count':len(hard),'hard_failures':hard,'checksum_policy':'official Binance .CHECKSUM SHA-256 mandatory for cached and downloaded ZIPs'}
    (ART/'SOURCE_MANIFEST.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2)); (ART/'download_report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)); print(json.dumps(report,indent=2))
    if hard:raise RuntimeError(f'hard source failures {len(hard)}')

def _read_month(sym,month):
    p=DATA/sym/f'{sym}-5m-{month}.zip'
    if not p.exists() or not p.stat().st_size:return None
    try:
        with zipfile.ZipFile(p) as z:
            names=[n for n in z.namelist() if not n.endswith('/')]
            if len(names)!=1:return None
            with z.open(names[0]) as fh:df=pd.read_csv(io.BytesIO(fh.read()),header=None,usecols=[0,1,2,3,4,7],names=['ot','o','h','l','c','qv'])
    except Exception:return None
    num=pd.to_numeric(df['ot'],errors='coerce'); df=df[num.notna()].copy(); df['ot']=num[num.notna()].astype('int64')
    for c in ('o','h','l','c','qv'):df[c]=pd.to_numeric(df[c],errors='coerce')
    df=df.dropna(); df.loc[df['ot']>100_000_000_000_000,'ot']//=1000
    return df if len(df) else None

def _load_segments(sym):
    monthly={m:_read_month(sym,m) for m in ALL_MONTHS}; missing=[m for m,d in monthly.items() if d is None]; segments=[]; run=[]; total=filled=0
    def flush(frames):
        nonlocal total,filled
        if not frames:return
        df=pd.concat(frames,ignore_index=True).drop_duplicates('ot').sort_values('ot')
        if len(df)<IGN_WIN+1:return
        grid=np.arange(int(df.ot.iloc[0]),int(df.ot.iloc[-1])+BAR_MS,BAR_MS,dtype='int64'); df=df.set_index('ot').reindex(grid); n=int(df.c.isna().sum()); cf_=df.c.ffill()
        for c in ('o','h','l','c'):df[c]=df[c].fillna(cf_)
        df['qv']=df.qv.fillna(0.); segments.append(df.reset_index(names='ot')); total+=len(df); filled+=n
    for m in ALL_MONTHS:
        if monthly[m] is None:flush(run); run=[]
        else:run.append(monthly[m])
    flush(run); return segments,missing,round(filled/total,6) if total else 0.

def _directional_gain_before(h,l,end,lookback):
    start=max(0,end-lookback)
    if end<=start:return None
    low=float('inf'); best=0.
    for i in range(start,end):
        low=min(low,float(l[i])); best=max(best,float(h[i])/low-1.) if low>0 else best
    return best

def _scan_symbol(sym):
    segments,missing,fill=_load_segments(sym); counters={k:0 for k in ['ignition_candidates','censored','liq_unavailable','liq_below','cleanliness_unavailable','continuation','brake_unavailable','suppressed']}; events={str(int(t*100)):[] for t in THRESHOLDS}; last={t:-10**30 for t in THRESHOLDS}
    for df in segments:
        ot=df.ot.to_numpy(np.int64); o=df.o.to_numpy(float); h=df.h.to_numpy(float); l=df.l.to_numpy(float); qv=df.qv.to_numpy(float); n=len(df)
        for i in range(n):
            ts=int(ot[i])
            if ts<SCAN_START_MS or ts>=SCAN_END_MS:continue
            if i+IGN_WIN>n or i+FWD_BARS>n:counters['censored']+=1; continue
            ref=o[i]
            if ref<=0:continue
            ign=float(h[i:i+IGN_WIN].max()/ref-1.)
            if ign<IGN_GAIN:continue
            counters['ignition_candidates']+=1; peak=float(h[i:i+FWD_BARS].max()/ref-1.)
            if i<PRIOR_24H:counters['liq_unavailable']+=1; continue
            liq=float(qv[i-PRIOR_24H:i].sum())
            if liq<LIQ_MIN_USDT:counters['liq_below']+=1; continue
            ranges=[]
            for k in range(VOL_BLOCKS_TARGET):
                end=i-k*VOL_BLOCK; start=end-VOL_BLOCK
                if start<0:break
                if o[start]>0:ranges.append(float((h[start:end].max()-l[start:end].min())/o[start]))
            med=float(np.median(ranges)) if len(ranges)>=VOL_MIN_BLOCKS else None
            for thr in THRESHOLDS:
                if peak<thr:continue
                if ts<last[thr]+SUPPRESS_MS:counters['suppressed']+=1; continue
                clean=_directional_gain_before(h,l,i,PRIOR_6H)
                if clean is None:counters['cleanliness_unavailable']+=1; continue
                if clean>=thr:counters['continuation']+=1; continue
                brake=None if med is None else peak>=BRAKE_MULT*med
                if brake is None:counters['brake_unavailable']+=1
                events[str(int(thr*100))].append({'symbol':sym,'t_ref_ms':ts,'threshold':thr,'ignition_gain':ign,'peak_gain_12h':peak,'prior_24h_quote_volume':liq,'median_12h_range_prior30d':med,'brake_pass':brake,'fill_ratio':fill}); last[thr]=ts
    return {'symbol':sym,'missing_months':missing,'fill_ratio':fill,'counters':counters,'events':events}

def scan():
    uni=json.loads((ART/'universe.json').read_text())['kept']; per={}; counters={}; all_events={str(int(t*100)):[] for t in THRESHOLDS}
    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        futs={ex.submit(_scan_symbol,s):s for s in uni}
        for i,f in enumerate(cf.as_completed(futs),1):
            r=f.result(); per[r['symbol']]={k:r[k] for k in ('missing_months','fill_ratio','counters')}
            for k,v in r['counters'].items():counters[k]=counters.get(k,0)+v
            for t,rows in r['events'].items():all_events[t]+=rows
            if i%50==0:print('scan',i,len(uni),flush=True)
    ladder={}
    for t,rows in all_events.items():
        rows.sort(key=lambda r:(r['t_ref_ms'],r['symbol'])); br=[r for r in rows if r['brake_pass'] is True]; ladder[t]={'unbraked_events':len(rows),'braked_events':len(br),'symbols_braked':len({r['symbol'] for r in br})}
    out={'period':{'warmup':WARMUP_MONTHS,'scan':SCAN_MONTHS,'start_ms':SCAN_START_MS,'end_ms_exclusive':SCAN_END_MS},'definition':{'ignition':'+3% within first 15m from t_ref open','magnitude':'peak from t_ref price during next 12h','main_case':'>=20%','mega':'>=30%'},'thresholds':ladder,'filter_counters':counters,'per_symbol':per,'events':all_events}
    (ART/'ladder_counts.json').write_text(json.dumps(out,ensure_ascii=False,indent=2)); print(ladder)

def main():
    if len(sys.argv)!=2 or sys.argv[1] not in {'universe','download','scan'}:raise SystemExit('usage universe|download|scan')
    {'universe':list_universe,'download':download,'scan':scan}[sys.argv[1]]()
if __name__=='__main__':main()
