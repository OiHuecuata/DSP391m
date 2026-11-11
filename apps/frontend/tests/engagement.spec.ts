import {test,expect} from '@playwright/test';
test('illustrated collector cards and engagement timelines use S3 attributes',async({page,request})=>{
 test.skip(process.env.RUN_S3_WEBSITE!=='1','Requires S3 cache');
 for(const source of ['threads','youtube']) {
 const response=await request.get(`/api/collected/${source}/overview`);expect(response.ok()).toBeTruthy();const data=await response.json();
 expect(data.engagement.length).toBeGreaterThan(1);expect(data.engagement.every((p:{likesReported:number;items:number})=>p.likesReported<=p.items)).toBe(true);
 if(source==='threads'){expect(data.reposts.total).toBeGreaterThanOrEqual(0);expect(data.quotes.reported).toBeGreaterThan(0);}else{expect(data.duration.reported).toBeGreaterThan(0);expect(data.duration.total).toBeGreaterThan(0);expect(data.tags.length).toBeGreaterThan(0);}
 await page.goto(`/collectors/${source}`);
 const overview=page.getByRole('region',{name:source==='threads'?'Threads overview':'YouTube overview',exact:true});
 await expect(overview.locator('article')).toHaveCount(6);
 const chart=page.getByRole('region',{name:'Engagement over time',exact:true});
 await expect(chart.getByRole('img')).toBeVisible();await expect(chart.getByRole('heading',{name:'Engagement over time'})).toHaveAttribute('title',/not newly gained/);
 if(source==='threads'){await expect(overview).toContainText('Reported reposts');await expect(overview).toContainText('Reported quotes');}else{await expect(overview).toContainText('Median video length');await expect(overview).toContainText('Common video tags');}
 await chart.getByRole('button',{name:'Latest 7 days',exact:true}).click();await expect(chart.getByRole('button',{name:'Latest 7 days',exact:true})).toHaveAttribute('aria-pressed','true');
 await chart.getByRole('button',{name:'All dates',exact:true}).click();await chart.getByText('View engagement counts',{exact:true}).click();await expect(chart.getByRole('table')).toContainText(data.engagement[0].date);
 await chart.getByText('View engagement counts',{exact:true}).click();await page.screenshot({path:`/home/coara/Documents/Codex/2026-10-26/i-j/outputs/${source}-illustrated-overview.png`,fullPage:true});
 await page.setViewportSize({width:390,height:844});expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
 await page.setViewportSize({width:1280,height:720});
 }
});
