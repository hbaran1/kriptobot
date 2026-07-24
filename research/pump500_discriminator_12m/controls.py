#!/usr/bin/env python3
"""P08-P10: four matched controls and strict SMD<0.10 gate."""
from __future__ import annotations
import json,math,sys,time
from pathlib import Path
import numpy as np
import pandas as pd
sys.path.insert(0,str(Path(__file__).resolve().parent))
from ladder import ART,BAR_MS,FWD_BARS,LIQ_MIN_USDT,PRIOR_24H,_load_segments
QUIET_THR=.05; K_CONTROLS=4; LOG_VOL_BAND=(math.log10(.5),math.log10(2.)); LOG_VOLAT_BAND=(math.log10(1/1.35),math.log10(1.35)); VOLAT_DIST_W=2.; REUSE_CAP=20; MIN_PANEL_DAYS=21; VOLAT_HALVES=14; VOLAT_MIN_HALVES=10; FWD_MS=12*3600*1000; DAY_MS=86_400_000; HALF_MS=DAY_MS//2; SMD_TARGET=.10; CLASS_EDGES=(6.,7.)
_cache={}
def _segments(sym):
    if sym not in _cache:
        segs,_,_=_load_segments(sym); _cache[sym]=[(d.ot.to_numpy(),d.o.to_numpy('float32'),d.h.to_numpy('float32'),d.l.to_numpy('float32'),d.qv.to_numpy('float32')) for d in segs]
    return _cache[sym]
def _panel(sym):
    out={}
    for ot,o,h,l,qv in _segments(sym):
        days=(ot//DAY_MS).astype('int64'); halves=(ot//HALF_MS).astype('int64'); d=pd.DataFrame({'day':days,'half':halves,'o':o.astype(float),'h':h.astype(float),'l':l.astype(float),'qv':qv.astype(float)})
        dv=d.groupby('day').qv.sum(); op=d.groupby('day').o.first(); gh=d.groupby('half'); hr=(gh.h.max()-gh.l.min())/gh.o.first().replace(0,np.nan); v30=dv.rolling(30,min_periods=MIN_PANEL_DAYS).median().shift(1); r7=hr.rolling(VOLAT_HALVES,min_periods=VOLAT_MIN_HALVES).median().shift(1)
        for day in dv.index:
            v=v30.get(day,np.nan); r=r7.get(2*day,np.nan); p=op.get(day,np.nan)
            if all(np.isfinite(x) and x>0 for x in (v,r,p)):out[int(day)]=(math.log10(float(v)),math.log10(float(r)),math.log10(float(p)))
    return out
def _cls(v):return 'S' if v<CLASS_EDGES[0] else ('M' if v<CLASS_EDGES[1] else 'L')
def _lookup(sym,t):
    for seg in _segments(sym):
        ot=seg[0]
        if ot[0]<=t<=ot[-1]:
            i=int((t-ot[0])//BAR_MS)
            if 0<=i<len(ot) and ot[i]==t:return seg,i
    return None
def _verify(sym,t):
    hit=_lookup(sym,t)
    if hit is None:return None
    (ot,o,h,l,qv),i=hit
    if i+FWD_BARS>len(ot) or i<PRIOR_24H:return None
    px=float(o[i]); liq=float(qv[i-PRIOR_24H:i].sum())
    if px<=0 or liq<LIQ_MIN_USDT:return None
    peak=float(h[i:i+FWD_BARS].max())/px-1.
    if peak>=QUIET_THR:return None
    return {'liq24_usdt':round(liq,0),'fwd_peak_pct':round(peak*100,2)}
def main():
    cases=json.loads((ART/'cases.json').read_text())['cases']; uni=json.loads((ART/'universe.json').read_text())['kept']; panels={}; t0=time.time()
    for i,s in enumerate(uni,1):panels[s]=_panel(s); print('panel',i,len(uni),flush=True) if i%150==0 else None
    bysym={}; dayidx={}
    for c in cases:bysym.setdefault(c['sym'],[]).append(c['t_ref_ms'])
    for s,p in panels.items():
        for d in p:dayidx.setdefault(d,[]).append(s)
    cv=[panels[c['sym']][c['t_ref_ms']//DAY_MS] for c in cases if c['t_ref_ms']//DAY_MS in panels.get(c['sym'],{})]; sdv=float(np.std([x[0] for x in cv])) or 1.; sdr=float(np.std([x[1] for x in cv])) or 1.
    matched=[]; dropped=[]; reuse={}
    for ci,c in enumerate(cases,1):
        T=c['t_ref_ms']; day=T//DAY_MS; me=panels.get(c['sym'],{}).get(day)
        if me is None:dropped.append({'case_id':c['case_id'],'reason':'no_panel'}); continue
        clv,clr,clp=me; cls=_cls(clv); candidates=[]
        for s in dayidx.get(day,[]):
            if s==c['sym']:continue
            lv,lr,lp=panels[s][day]; dv,dr=lv-clv,lr-clr
            if LOG_VOL_BAND[0]<=dv<=LOG_VOL_BAND[1] and LOG_VOLAT_BAND[0]<=dr<=LOG_VOLAT_BAND[1] and _cls(lv)==cls:candidates.append((abs(dv)/sdv+VOLAT_DIST_W*abs(dr)/sdr,s,lv,lr,lp))
        candidates.sort(); picked=[]
        for _,s,lv,lr,lp in candidates:
            if reuse.get(s,0)>=REUSE_CAP or any(abs(t-T)<FWD_MS for t in bysym.get(s,[])):continue
            v=_verify(s,T)
            if v is None:continue
            picked.append({'sym':s,'log_vol30':round(lv,4),'log_volat7':round(lr,4),'log_price':round(lp,4),**v}); reuse[s]=reuse.get(s,0)+1
            if len(picked)==K_CONTROLS:break
        if len(picked)<K_CONTROLS:
            for p in picked:reuse[p['sym']]-=1
            dropped.append({'case_id':c['case_id'],'reason':'insufficient_controls','found':len(picked)}); continue
        matched.append({'case_id':c['case_id'],'sym':c['sym'],'t_ref_ms':T,'split':c['split'],'tier':c['tier'],'vol_class':cls,'case_log_vol30':round(clv,4),'case_log_volat7':round(clr,4),'case_log_price':round(clp,4),'controls':picked})
        if ci%300==0:print('matching',ci,len(cases),flush=True)
    def smd(a,b):
        pooled=math.sqrt((a.var(ddof=1)+b.var(ddof=1))/2); return abs(a.mean()-b.mean())/pooled if pooled>0 else 0.
    vals={}
    for name,kc,kk in [('log_vol30','case_log_vol30','log_vol30'),('log_volat7','case_log_volat7','log_volat7'),('log_price','case_log_price','log_price')]:
        a=np.array([m[kc] for m in matched]); b=np.array([x[kk] for m in matched for x in m['controls']]); vals[name]=round(smd(a,b),4) if len(matched) else None
    passed=all(v is not None and v<SMD_TARGET for v in vals.values()); counts={'cases_input':len(cases),'matched':len(matched),'dropped':len(dropped),'controls_total':sum(len(m['controls']) for m in matched),'unique_control_symbols':len(reuse)}
    out={'counts':counts,'balance':{**{f'smd_{k}':v for k,v in vals.items()},'target':SMD_TARGET,'PASS':passed},'dropped_cases':dropped,'matched':matched,'test_access':'labels retained only; no metric/model/test performance read in this run','data_class':'REAL_OBSERVED source; covariates DERIVED_FROM_REAL'}
    (ART/'controls.json').write_text(json.dumps(out,ensure_ascii=False,indent=2)); (ART/'BALANCE_RAPORU.md').write_text(f"# Denge\n\nMatched: {len(matched)} · SMD: {vals} · PASS={passed}\n"); print(counts,vals,passed)
    if not passed:raise RuntimeError('SMD gate failed')
if __name__=='__main__':main()
