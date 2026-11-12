import {readFile} from 'node:fs/promises';
import path from 'node:path';
import {DemandSupplyAnalysis} from '@/features/analytics/DemandSupplyAnalysis';
import {parseModelReport} from '@/features/analytics/model-report';
import {parseRidgeDemo} from '@/features/analytics/ridge-demo';
import {parseRidgeReadiness} from '@/features/analytics/ridge-report';
import {repoRoot} from '@/features/settings/server';
export const dynamic='force-dynamic';
export default async function AnalyticsPage(){
 const [model,ridge,forecast]=await Promise.all([
  readFile(/* turbopackIgnore: true */ path.join(repoRoot(),'docs/ml/evidence.json'),'utf8').then(text=>parseModelReport(JSON.parse(text))).catch(()=>null),
  readFile(/* turbopackIgnore: true */ path.join(repoRoot(),'docs/ml/provincial_readiness_evidence.json'),'utf8').then(text=>parseRidgeReadiness(JSON.parse(text))).catch(()=>null),
  readFile(/* turbopackIgnore: true */ path.join(repoRoot(),'docs/ml/ridge_v3_demo.json'),'utf8').then(text=>parseRidgeDemo(JSON.parse(text))).catch(()=>null),
 ]);
 return <DemandSupplyAnalysis model={model} ridge={ridge} forecast={forecast}/>;
}
