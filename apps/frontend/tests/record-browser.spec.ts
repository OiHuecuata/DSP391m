import {test,expect} from "@playwright/test";
test.skip(process.env.RUN_S3_WEBSITE!=="1","Requires the saved S3 website snapshot");
test("collector search, sort and CSV match the selected records",async({page,request})=>{
 const initial=await (await request.get('/api/search?source=threads')).json();
 expect(initial.total).toBeGreaterThan(0);
 const ranked=await (await request.get('/api/search?source=threads&sort=likes')).json();
 expect(ranked.records[0].likes).toBeGreaterThanOrEqual(ranked.records[1].likes);
 await page.goto('/collectors/threads');
 const browser=page.getByRole('region',{name:'Threads records',exact:true});
 await expect(browser.getByText(`${initial.total} results`,{exact:true})).toBeVisible();
 await browser.getByLabel('Search content',{exact:true}).fill('zzzzzznomatchzzzz');
 await browser.getByRole('button',{name:'Search',exact:true}).click();
 await expect(browser.getByText('No records match these filters.')).toBeVisible();
 const csv=await request.get('/api/search?source=threads&download=csv');
 expect(csv.headers()['content-type']).toContain('text/csv');
 expect(await csv.text()).toContain('threads');
 const invalid=await request.get('/api/search?from=2026-11-10&to=2026-10-01');
 expect(invalid.status()).toBe(400);
});
test("cross-source results remain available and mobile fits",async({page})=>{
 await page.goto('/search?q=pickleball');
 await expect(page).toHaveURL(/\/search\?q=pickleball/);
 await expect(page.getByRole('heading',{name:'Search collected data'})).toBeVisible();
 await page.setViewportSize({width:390,height:844});
 expect(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth)).toBe(false);
});
