export type RidgeReadiness={recordedAt:string;eligible:number;required:number;horizonWeeks:number;provinces:string[]};
const object=(v:unknown):Record<string,unknown>=>{if(!v||typeof v!=='object'||Array.isArray(v))throw Error('Invalid Ridge evidence');return v as Record<string,unknown>;};
const integer=(v:unknown,min=0)=>{if(typeof v!=='number'||!Number.isSafeInteger(v)||v<min)throw Error('Invalid Ridge count');return v;};
// Read aggregate readiness only. Never use synthetic test forecasts as market results.
export function parseRidgeReadiness(input:unknown):RidgeReadiness{
 const report=object(input),audit=object(report.local_audit),benchmark=object(report.source_benchmark),config=object(audit.config);
 if(typeof report.recorded_at!=='string'||!Number.isFinite(Date.parse(report.recorded_at))||audit.status!=='BLOCKED'||benchmark.status!=='NOT_RUN'||benchmark.final_model!==null||benchmark.final_inference!==null)throw Error('Unsupported Ridge readiness report');
 const eligible=integer(benchmark.eligible_provinces),required=integer(benchmark.required_provinces,1);
 if(required!==integer(config.min_provinces,1)||eligible>=required||!Array.isArray(audit.eligible_provinces)||audit.eligible_provinces.length!==eligible)throw Error('Inconsistent Ridge eligibility');
 const ids=new Set<string>(),provinces=audit.eligible_provinces.map(v=>{const p=object(v);if(typeof p.province_id!=='string'||ids.has(p.province_id)||typeof p.province_name!=='string'||!p.province_name)throw Error('Invalid Ridge province');ids.add(p.province_id);return p.province_name;});
 return {recordedAt:report.recorded_at,eligible,required,horizonWeeks:integer(config.horizon,1),provinces};
}
