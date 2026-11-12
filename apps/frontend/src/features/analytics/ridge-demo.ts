export type RidgeDemo = {
 status:'TRAINED'; provenance:'mixed'; evaluationStatus:'provisional-solver-warning'; datasetId:string; runId:string; generatedAt:string;
 originalPosts:number; simulatedPosts:number; trainingCutoff:string; forecastWeek:string;
 metrics:{mae:number;rmse:number;baselineMae:number;baselineRmse:number};
 provinces:{id:string;name:string;prediction:number;test:{week:string;actual:number;prediction:number}[]}[];
};
export function parseRidgeDemo(value:unknown):RidgeDemo|null {
 if(!value||typeof value!=='object')return null;
 const r=value as RidgeDemo;
 const finite=(n:unknown)=>typeof n==='number'&&Number.isFinite(n)&&n>=0;
 const date=(s:unknown)=>typeof s==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(s)&&Number.isFinite(Date.parse(s));
 if(r.status!=='TRAINED'||r.provenance!=='mixed'||r.evaluationStatus!=='provisional-solver-warning'||typeof r.datasetId!=='string'||typeof r.runId!=='string'||typeof r.generatedAt!=='string'||!date(r.trainingCutoff)||!date(r.forecastWeek)||!Number.isInteger(r.originalPosts)||r.originalPosts<0||!Number.isInteger(r.simulatedPosts)||r.simulatedPosts<1||!r.metrics||![r.metrics.mae,r.metrics.rmse,r.metrics.baselineMae,r.metrics.baselineRmse].every(finite)||!Array.isArray(r.provinces)||r.provinces.length<3||r.provinces.length>34)return null;
 if(new Set(r.provinces.map(p=>p.id)).size!==r.provinces.length)return null;
 for(const p of r.provinces){if(typeof p.id!=='string'||typeof p.name!=='string'||!finite(p.prediction)||!Array.isArray(p.test)||!p.test.length||!p.test.every(x=>date(x.week)&&finite(x.actual)&&finite(x.prediction)))return null;}
 return r;
}
