#!/usr/bin/env python3
from __future__ import annotations
import concurrent.futures as cf, hashlib, io, json, os, re, time, urllib.error, urllib.request, zipfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
import numpy as np
import pandas as pd

R=Path(__file__).resolve().parent; O=R/'artifacts'; O.mkdir(parents=True,exist_ok=True)
S3='https://s3-ap-northeast-1.amazonaws.com/data.binance.vision'
A='https://data.binance.vision/data/spot'
MONTHS=['2025-12','2026-01','2026-02','2026-03','2026-04','2026-05','2026-06']
START=1767225600000; END=1782864000000; M=60000
TH=(.05,.10,.15,.20,.30); IGN=.03; BRAKE=3.; LIQ=100000.
LEV=re.compile(r'(?:UP|DOWN|BULL|BEAR|3L|3S)USDT$')
STABLE={'USDC','TUSD','BUSD','DAI','FDUSD','USDP','SUSD','UST','USTC','VAI','USDE','USDS','USDSB','USDSOLD','RLUSD','PYUSD','USD1','XUSD','BFUSD','AEUR','EURI','EUR','GBP','AUD','BRL','TRY','RUB','UAH','NGN','ZAR','BIDR','IDRT','BKRW'}

def now(): return datetime.now(timezone.utc).isoformat().replace('+00:00','Z')
def get(u,t=90):
 req=urllib.request.Request(u,headers={'User-Agent':'pump500-strict/1.0'})
 with urllib.request.urlopen(req,timeout=t) as r:return r.read()

def universe():
 sy=[]; marker=''; pages=0
 while 1:
  from urllib.parse import quote
  u=f'{S3}?delimiter=/&prefix=data/spot/monthly/klines/'+(('&marker='+quote(marker,safe='')) if marker else '')
  x=get(u).decode(); pages+=1; p=re.findall(r'<Prefix>data/spot/monthly/klines/([^<]+)/</Prefix>',x); sy+=p
  if '<IsTruncated>true</IsTruncated>' not in x: break
  if not p: raise RuntimeError('bad S3 pagination')
  marker=f'data/spot/monthly/klines/{p[-1]}/'
 us=sorted(set(s for s in sy if s.endswith('USDT'))); keep=[]; exc={}
 for s in us:
  b=s[:-4]
  if LEV.search(s): exc[s]='leveraged'
  elif b in STABLE: exc[s]='stable_or_fiat'
  else: keep.append(s)
 z={'generated_utc':now(),'source':S3,'pages':pages,'archive_symbols':len(set(sy)),'usdt':len(us),'kept':keep,'excluded':exc,'source_type':'REAL_OBSERVED'}
 (O/'P02_UNIVERSE.json').write_text(json.dumps(z,indent=2)); return keep,z

def urls(s,label):
 if label=='2026-07-01': n=f'{s}-1m-{label}.zip'; b=f'{A}/daily/klines/{s}/1m/{n}'
 else: n=f'{s}-1m-{label}.zip'; b=f'{A}/monthly/klines/{s}/1m/{n}'
 return b,b+'.CHECKSUM'

def dl(s,label):
 u,c=urls(s,label); r={'symbol':s,'label':label,'url':u}
 try:
  e=get(c).decode().split()[0].lower(); assert re.fullmatch(r'[0-9a-f]{64}',e)
  p=get(u); a=hashlib.sha256(p).hexdigest(); assert a==e
  r.update(status='OK',bytes=len(p),sha256=a); return p,r
 except urllib.error.HTTPError as x:r.update(status='SOURCE_UNAVAILABLE' if x.code==404 else 'HTTP_ERROR',http=x.code)
 except Exception as x:r.update(status='ERROR',error=type(x).__name__+':'+str(x))
 return None,r

def frame(p):
 with zipfile.ZipFile(io.BytesIO(p)) as z:
  ns=[n for n in z.namelist() if not n.endswith('/')]; assert len(ns)==1; raw=z.read(ns[0])
 d=pd.read_csv(io.BytesIO(raw),header=None,usecols=[0,1,2,3,4,7],names=['t','o','h','l','c','q'])
 d['t']=pd.to_numeric(d.t,errors='coerce'); d=d[d.t.notna()].copy()
 for c in ['t','o','h','l','c','q']: d[c]=pd.to_numeric(d[c],errors='coerce')
 d=d.dropna(); d['t']=d.t.astype('int64'); d.loc[d.t>10**14,'t']//=1000; return d

def segments(d):
 d=d.drop_duplicates('t').sort_values('t').reset_index(drop=True); g=d.t.diff().fillna(M).ne(M).cumsum(); return [x.reset_index(drop=True) for _,x in d.groupby(g)]

def scan_seg(s,d,rec):
 n=len(d)
 if n<43920:return {'bars':n,'cand':0,'eligible':0}
 t=d.t.to_numpy('int64'); o=d.o.to_numpy(float); h=d.h.to_numpy(float); l=d.l.to_numpy(float); q=d.q.to_numpy(float)
 hs=pd.Series(h); ls=pd.Series(l)
 ign=hs[::-1].rolling(15,min_periods=15).max()[::-1].to_numpy()/o-1
 peak=hs[::-1].rolling(720,min_periods=720).max()[::-1].to_numpy()/o-1
 p6h=hs.rolling(360,min_periods=360).max().shift(1).to_numpy(); p6l=ls.rolling(360,min_periods=360).min().shift(1).to_numpy()
 day=t//86400000; block=t//43200000
 dv={}
 for x in np.unique(day):
  ix=np.flatnonzero(day==x)
  if len(ix)==1440 and t[ix[-1]]-t[ix[0]]==1439*M:dv[int(x)]=float(q[ix].sum())
 br={}
 rr={}
 for x in np.unique(block):
  ix=np.flatnonzero(block==x)
  if len(ix)==720 and t[ix[-1]]-t[ix[0]]==719*M and o[ix[0]]>0:rr[int(x)]=float((h[ix].max()-l[ix].min())/o[ix[0]])
 for x in np.unique(block):
  vals=[rr[y] for y in range(int(x)-60,int(x)) if y in rr]
  if len(vals)==60:br[int(x)]=float(np.median(vals))
 ix=np.flatnonzero((t>=START)&(t<END)&np.isfinite(ign)&np.isfinite(peak)&(ign>=IGN)&(o>0)); el=0
 for i in ix:
  v=dv.get(int(day[i])); b=br.get(int(block[i]))
  if v is None or b is None or not np.isfinite(p6h[i]) or not np.isfinite(p6l[i]) or p6l[i]<=0:continue
  el+=1; pr=float(p6h[i]/p6l[i]-1); pg=float(peak[i])
  for z in TH:
   if pg>=z and v>=LIQ and pr<z:rec[z].append({'symbol':s,'t_ref_ms':int(t[i]),'utc_day':int(day[i]),'ignition_gain':float(ign[i]),'peak_gain_12h':pg,'day_quote_volume':v,'prior6h_range':pr,'median_12h_range_prior30d':b,'brake_pass':pg>=BRAKE*b})
 return {'bars':n,'cand':len(ix),'eligible':el}

def one(s):
 fs=[]; logs=[]
 with cf.ThreadPoolExecutor(max_workers=4) as ex:
  fut=[ex.submit(dl,s,m) for m in MONTHS+['2026-07-01']]
  for f in cf.as_completed(fut):
   p,r=f.result(); logs.append(r)
   if p:
    try:fs.append(frame(p))
    except Exception as x:r.update(status='PARSE_ERROR',error=str(x))
 rec={z:[] for z in TH}; q={'download':logs,'bars':0,'segments':0,'gaps':0,'cand':0,'eligible':0}
 if fs:
  sg=segments(pd.concat(fs,ignore_index=True)); q['segments']=len(sg); q['gaps']=max(0,len(sg)-1)
  for d in sg:
   x=scan_seg(s,d,rec)
   for k in ['bars','cand','eligible']:q[k]+=x[k]
 return s,q,rec

def dedupe(a):
 out=[]; seen=set()
 for r in sorted(a,key=lambda x:(x['symbol'],x['t_ref_ms'])):
  k=(r['symbol'],r['utc_day'])
  if k not in seen:seen.add(k);out.append(r)
 return out

def main():
 st=now(); sy,u=universe(); qual={}; allr={z:[] for z in TH}; t0=time.time()
 with cf.ThreadPoolExecutor(max_workers=int(os.getenv('PUMP500_WORKERS','6'))) as ex:
  fs={ex.submit(one,s):s for s in sy}
  for n,f in enumerate(cf.as_completed(fs),1):
   s=fs[f]
   try:
    s,q,r=f.result();qual[s]=q
    for z in TH:allr[z]+=r[z]
   except Exception as x:qual[s]={'fatal':type(x).__name__+':'+str(x)}
   if n%25==0:print(n,len(sy),round(time.time()-t0),flush=True)
 ladder={}
 for z in TH:
  a=dedupe(allr[z]); b=[x for x in a if x['brake_pass']]; k=str(int(z*100))
  ladder[k]={'unbraked_events':len(a),'braked_events':len(b),'symbols_braked':len({x['symbol'] for x in b})}
  with (O/f'P01B_EVENTS_{k}PCT.jsonl').open('w') as f:
   for x in b:f.write(json.dumps(x,separators=(',',':'))+'\n')
 sc=defaultdict(int); bad=[]
 for q in qual.values():
  for r in q.get('download',[]):sc[r.get('status','UNKNOWN')]+=1; bad.extend([r] if r.get('status')!='OK' else [])
 dq={'generated_utc':now(),'file_status_counts':dict(sc),'source_unavailable':bad,'per_symbol':qual,'gap_policy':'never cross or fill a non-1m gap','checksum_policy':'official .CHECKSUM SHA-256 required'}
 (O/'P01B_DATA_QUALITY.json').write_text(json.dumps(dq,indent=2))
 res={'study':'pump500_strict_fresh_20260724','started_utc':st,'completed_utc':now(),'fresh_data_only':True,'prior_results_used':False,'period':{'scan':'2026-01-01..2026-06-30','warmup':'2025-12','forward':'2026-07-01 daily'},'definition':{'ignition':'1m sliding 15m high/open >=3%','peak':'next 720 complete 1m bars','brake':'peak >=3x median previous 60 complete UTC 12h blocks','liquidity':'complete UTC event-day quote volume >=100000','cleanliness':'complete prior 360m range below threshold','dedupe':'first per symbol per UTC day per threshold'},'ladder':ladder,'p01_status':'PENDING_OPERATOR_FREEZE_AFTER_FRESH_LADDER','p02_status':'DONE_VERIFIED','p03_status':'DONE_VERIFIED'}
 (O/'P01B_STRICT_RESULT.json').write_text(json.dumps(res,indent=2))
 L=['# P01b — Yeni Veri / 1 Dakika / Harfiyen','', '| Eşik | Frensiz | Frenli ≥3× | Sembol |','|---:|---:|---:|---:|']
 for z in TH:
  x=ladder[str(int(z*100))];L.append(f"| ≥+%{int(z*100)} | {x['unbraked_events']} | **{x['braked_events']}** | {x['symbols_braked']} |")
 L+=['','P01 bu tablo operatöre sunulmadan dondurulmaz.',f'Checksum/dosya durumları: {dict(sc)}'];(O/'P01B_STRICT_TABLE.md').write_text('\n'.join(L));print('\n'.join(L))
if __name__=='__main__':main()
