import {test,expect} from '@playwright/test';
const source=(configured=true)=>({enabled:configured,configured,interval_seconds:3600,status:'IDLE',queued:false});
const snapshot=()=>({pipeline:{status:'IDLE'},collection:{continuous:false,status:'IDLE',sources:{threads:source(),youtube:source(false),pbplus:source()},runs:[]},lakehouse:{tables:[]},warehouse:{}});
test('live flow controls send start, stop, run, and config requests without changing S3 information',async({page})=>{
 const data=snapshot();
 await page.route('**/api/v1/data-ops/status',r=>r.fulfill({json:data}));
 const requests:{action:string;method:string;body:unknown}[]=[];
 await page.route('**/api/v1/collection/*',r=>{
 const action=r.request().url().split('/').at(-1)!;requests.push({action,method:r.request().method(),body:r.request().postDataJSON()});
 const collection={...data.collection,continuous:action==='start'?true:action==='stop'?false:data.collection.continuous,active_run_id:action==='run'?'mock-run':undefined};
 data.collection.continuous=collection.continuous;
 return r.fulfill({status:202,json:collection});
 });
 await page.goto('/data-operations');const controls=page.getByRole('region',{name:'Flow controls',exact:true});
 await expect(controls.getByRole('button',{name:'Start continuous collection',exact:true})).toBeEnabled();
 await expect(controls.getByRole('button',{name:'Run YouTube now',exact:true})).toBeDisabled();
 await controls.getByRole('button',{name:'Save source settings',exact:true}).click();await expect(controls.getByRole('status')).toContainText('Source settings saved');
 expect(requests.at(-1)).toMatchObject({action:'config',method:'PATCH',body:{sources:{threads:{enabled:true,interval_seconds:3600},youtube:{enabled:false,interval_seconds:3600}}}});
 await controls.getByRole('button',{name:'Run Threads now',exact:true}).click();await expect(controls.getByRole('status')).toContainText('Run queued');expect(requests.at(-1)).toMatchObject({action:'run',method:'POST',body:{sources:['threads']}});
 await controls.getByRole('button',{name:'Start continuous collection',exact:true}).click();await expect(controls.getByRole('status')).toContainText('Continuous collection enabled');
 await controls.getByRole('button',{name:'Stop after current run',exact:true}).click();await expect(controls.getByRole('status')).toContainText('Future runs stopped');expect(requests.at(-1)).toMatchObject({action:'stop',method:'POST'});
 await controls.getByRole('button',{name:'Run all now',exact:true}).click();await expect(controls.getByRole('status')).toContainText('Run queued');expect(requests.at(-1)).toMatchObject({body:{sources:['threads','pbplus']}});
 await page.setViewportSize({width:390,height:844});expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
});
test('offline controller leaves flow actions visible and disabled',async({page})=>{
 await page.route('**/api/v1/data-ops/status',r=>r.fulfill({status:503,json:{error:'Offline'}}));await page.goto('/data-operations');
 const controls=page.getByRole('region',{name:'Flow controls',exact:true});await expect(controls.getByRole('alert')).toContainText('503');
 await expect(controls.getByRole('button',{name:'Run all now',exact:true})).toBeVisible();await expect(controls.getByRole('button',{name:'Run all now',exact:true})).toBeDisabled();
});
test('control errors remain visible and actions can be retried',async({page})=>{
 await page.route('**/api/v1/data-ops/status',r=>r.fulfill({json:snapshot()}));await page.route('**/api/v1/collection/run',r=>r.fulfill({status:503,json:{error:'Worker unavailable'}}));await page.goto('/data-operations');
 const controls=page.getByRole('region',{name:'Flow controls',exact:true});await controls.getByRole('button',{name:'Run all now',exact:true}).click();await expect(controls.getByRole('status')).toContainText('Worker unavailable');await expect(controls.getByRole('button',{name:'Run all now',exact:true})).toBeEnabled();
});
