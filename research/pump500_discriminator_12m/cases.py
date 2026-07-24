#!/usr/bin/env python3
"""Donmuş P01 ile P05-P07 vaka kütüğü; TEST yalnız etiketlenir."""
from __future__ import annotations
import json,sys,time
from pathlib import Path
import numpy as np
import pandas as pd
sys.path.insert(0,str(Path(__file__).resolve().parent))
from ladder import ART,BRAKE_MULT,FWD_BARS,IGN_GAIN,IGN_WIN,LIQ_MIN_USDT,PRIOR_6H,PRIOR_24H,SCAN_END_MS,SCAN_START_MS,SUPPRESS_MS,VOL_BLOCK,VOL_BLOCKS_TARGET,VOL_MIN_BLOCKS,_load_segments
CASE_THR=.20; MEGA_THR=.30; SMALL_LO=.05; SMALL_HI=.20; TRAIN_TEST_SPLIT_MS=1777593600000

def _iter_candidates(sym):
    segments,_,fill=_load_segments(sym)
    for df in segments:
        ot=df.ot.to_numpy(); o=df.o.to_numpy(); h=df.h.to_numpy(); l=df.l.to_numpy(); qv=df.qv.to_numpy(); hs=pd.Series(h); qs=pd.Series(qv)
        win=hs[::-1].rolling(IGN_WIN,min_periods=IGN_WIN).max()[::-1].to_numpy(); fwd=hs[::-1].rolling(FWD_BARS,min_periods=FWD_BARS).max()[::-1].to_numpy(); liq24=qs.rolling(PRIOR_24H,min_periods=PRIOR_24H).sum().shift(1).to_numpy()
        valid=(ot>=SCAN_START_MS)&(ot<SCAN_END_MS)&(np.nan_to_num(win/o-1.,nan=-1.)>=IGN_GAIN)&(o>0)&~np.isnan(fwd)
        for t in np.where(valid)[0]:
            peak=fwd[t]/o[t]-1.
            if peak<SMALL_LO or not np.isfinite(liq24[t]) or t<PRIOR_6H:continue
            rise6=float(np.max(h[t-PRIOR_6H:t]/np.minimum.accumulate(l[t-PRIOR_6H:t]))-1.)
            ranges=[]
            for k in range(1,VOL_BLOCKS_TARGET+1):
                b=t-k*VOL_BLOCK
                if b<0:break
                if o[b]>0:ranges.append((h[b:b+VOL_BLOCK].max()-l[b:b+VOL_BLOCK].min())/o[b])
            if len(ranges)<VOL_MIN_BLOCKS:continue
            med=float(np.median(ranges)); pk=t+int(np.argmax(h[t:t+FWD_BARS]))
            yield {'t_ref_ms':int(ot[t]),'price_ref':float(o[t]),'peak_gain':float(peak),'peak_ms':int(ot[pk]),'mins_to_peak':int((ot[pk]-ot[t])/60000),'liq24_usdt':float(liq24[t]),'rise6h':rise6,'med12h_range':med,'brake_ratio':float(peak/med) if med>0 else None,'fill_ratio':fill}

def _pipeline(cands,thr):
    out=[]; last=-1
    for e in sorted(cands,key=lambda x:x['t_ref_ms']):
        if e['peak_gain']<thr or e['t_ref_ms']<last or e['liq24_usdt']<LIQ_MIN_USDT or e['rise6h']>=thr or e['brake_ratio'] is None or e['brake_ratio']<BRAKE_MULT:continue
        last=e['t_ref_ms']+SUPPRESS_MS; out.append(e)
    return out

def main():
    uni=json.loads((ART/'universe.json').read_text())['kept']; cases=[]; small=[]; t0=time.time()
    for i,sym in enumerate(uni,1):
        cands=list(_iter_candidates(sym)); cs=_pipeline(cands,CASE_THR); windows=[(c['t_ref_ms'],c['t_ref_ms']+SUPPRESS_MS) for c in cs]
        for c in cs:c['sym']=sym; cases.append(c)
        for s in _pipeline(cands,SMALL_LO):
            if SMALL_LO<=s['peak_gain']<SMALL_HI and not any(a<=s['t_ref_ms']<b for a,b in windows):s['sym']=sym; small.append(s)
        if i%100==0:print('cases',i,len(uni),round(time.time()-t0),flush=True)
    def enrich(e,kind):
        ts=time.gmtime(e['t_ref_ms']/1000)
        return {'case_id':f"{e['sym']}-{e['t_ref_ms']}",'kind':kind,'sym':e['sym'],'t_ref_ms':e['t_ref_ms'],'t_ref_utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',ts),'month':time.strftime('%Y-%m',ts),'split':'TRAIN' if e['t_ref_ms']<TRAIN_TEST_SPLIT_MS else 'TEST','tier':('MEGA' if e['peak_gain']>=MEGA_THR else 'CASE') if kind=='CASE' else 'SMALL','price_ref':round(e['price_ref'],10),'peak_gain_pct':round(e['peak_gain']*100,2),'mins_to_peak':e['mins_to_peak'],'liq24_usdt':round(e['liq24_usdt'],0),'rise6h_pct':round(e['rise6h']*100,2),'med12h_range_pct':round(e['med12h_range']*100,3),'brake_ratio':round(e['brake_ratio'],2)}
    rows=[enrich(e,'CASE') for e in cases]; small_rows=[enrich(e,'SMALL') for e in small]; per={}
    for r in rows:per[r['sym']]=per.get(r['sym'],0)+1
    train=sum(r['split']=='TRAIN' for r in rows); mega=sum(r['tier']=='MEGA' for r in rows)
    out={'definition_frozen':'P01 +3%/15m ignition; next-12h peak >=20%; brake 3x; prior24h liquidity; directional 6h cleanliness','split_rule':'TRAIN < 2026-05-01 <= TEST; TEST LOCKED','counts':{'cases':len(rows),'mega_subset':mega,'train':train,'test':len(rows)-train,'small_movers':len(small_rows),'unique_case_symbols':len(per)},'repeat_concentration_top10':sorted(per.items(),key=lambda x:-x[1])[:10],'cases':rows,'small_movers':small_rows,'data_class':'REAL_OBSERVED source; DERIVED_FROM_REAL fields'}
    (ART/'cases.json').write_text(json.dumps(out,ensure_ascii=False,indent=2)); (ART/'CASES_OZET.md').write_text(f"# Vaka kütüğü\n\nVaka: {len(rows)} · Mega: {mega} · Train/Test etiketi: {train}/{len(rows)-train} · TEST açılmadı.\n"); print(out['counts'])
if __name__=='__main__':main()
