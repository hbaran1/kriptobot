#!/usr/bin/env python3
import json
from pathlib import Path
R=Path(__file__).resolve().parent; A=R/'artifacts'
def main():
    source=json.loads((A/'download_report.json').read_text()); universe=json.loads((A/'universe.json').read_text()); ladder=json.loads((A/'ladder_counts.json').read_text()); cases=json.loads((A/'cases.json').read_text()); controls=json.loads((A/'controls.json').read_text()); t20=ladder['thresholds']['20']; cc=cases['counts']; mc=controls['counts']; counters=ladder.get('filter_counters',{})
    rows=[
      {'stage':'source_jobs','remaining':source['jobs'],'removed':0,'note':'symbol-month jobs; June 2025 warmup included'},
      {'stage':'verified_source_files','remaining':source['verified_files'],'removed':source['jobs']-source['verified_files'],'note':f"SOURCE_UNAVAILABLE={source['source_unavailable']}; hard_failures={source['hard_failure_count']}"},
      {'stage':'universe_symbols','remaining':len(universe['kept']),'removed':len(universe.get('excluded_leveraged',[]))+len(universe.get('excluded_stable_fiat',[])),'note':'spot USDT archive universe'},
      {'stage':'ignition_candidates','remaining':counters.get('ignition_candidates'),'removed':None,'note':'+3% within 15m'},
      {'stage':'peak20_unbraked','remaining':t20['unbraked_events'],'removed':None,'note':'next-12h peak from t_ref >=20%'},
      {'stage':'peak20_braked','remaining':t20['braked_events'],'removed':t20['unbraked_events']-t20['braked_events'],'note':'3x own-volatility brake'},
      {'stage':'case_registry','remaining':cc['cases'],'removed':t20['braked_events']-cc['cases'],'note':f"mega={cc['mega_subset']}; symbols={cc['unique_case_symbols']}"},
      {'stage':'matched_cases','remaining':mc['matched'],'removed':mc['dropped'],'note':f"four controls; total={mc['controls_total']}"},
      {'stage':'smd_gate','remaining':mc['matched'] if controls['balance']['PASS'] else 0,'removed':0 if controls['balance']['PASS'] else mc['matched'],'note':json.dumps(controls['balance'],sort_keys=True)}]
    out={'period':{'warmup':'2025-06','scan':'2025-07-01..2026-06-30','test_boundary':'2026-05-01','test_status':'LOCKED_NOT_EVALUATED'},'definition':ladder['definition'],'rows':rows,'filter_counters':counters,'source_policy':source['checksum_policy'],'p17_plus_status':'CLOSED'}
    (A/'FILTER_FUNNEL.json').write_text(json.dumps(out,ensure_ascii=False,indent=2)); lines=['# 12 Aylık Filtre Hunisi','','| Aşama | Kalan | Elenen | Not |','|---|---:|---:|---|']
    for r in rows:lines.append(f"| {r['stage']} | {'—' if r['remaining'] is None else r['remaining']} | {'—' if r['removed'] is None else r['removed']} | {str(r['note']).replace('|','/')} |")
    lines+=['','TEST performansı açılmadı; P17-P20 kapalı.']; (A/'FILTER_FUNNEL.md').write_text('\n'.join(lines)); print('\n'.join(lines))
if __name__=='__main__':main()
