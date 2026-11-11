import {test,expect} from '@playwright/test';
import {locationMentions} from '../src/features/collectors/location-mentions';
const refs=[{location_id:'hanoi',canonical_name:'Hà Nội',aliases:['ha noi','hanoi']},{location_id:'danang',canonical_name:'Đà Nẵng',aliases:['da nang']}];
test('location matching counts items once, handles accents and separates words from links',()=>{
 const summary=locationMentions([{text:'HÀ NỘI, Hanoi and ha noi; Đà Nẵng'},{text:'#hanoi'},{text:'hanoian https://example.com/da-nang'},{text:'No place mentioned'}],refs);
 expect(summary).toEqual({total:4,matched:2,unmatched:2,locations:[{id:'hanoi',name:'Hà Nội',count:2},{id:'danang',name:'Đà Nẵng',count:1}]});
 expect(locationMentions([],refs)).toEqual({total:0,matched:0,unmatched:0,locations:[]});
});
test('both collector overviews expose location aggregates without record text',async({page,request})=>{
 test.skip(process.env.RUN_S3_WEBSITE!=='1','Requires configured S3 cache');
 for(const source of ['threads','youtube']) {
 const response=await request.get(`/api/collected/${source}/overview`);expect(response.ok()).toBeTruthy();
 const data=await response.json();expect(data.locations).not.toBeNull();expect(data.locations.total).toBe(data.posts);expect(data.locations.matched+data.locations.unmatched).toBe(data.posts);
 for(const location of data.locations.locations) {expect(location.count).toBeGreaterThan(0);expect(location.count).toBeLessThanOrEqual(data.locations.matched);}
 await page.goto(`/collectors/${source}`);
 const locations=page.getByRole('region',{name:'Locations mentioned',exact:true});
 await expect(locations).toBeVisible();await expect(locations.getByRole('heading',{name:'Locations mentioned'})).toHaveAttribute('title',/not verified court locations/);
 if(data.locations.locations.length) await expect(locations).toContainText(data.locations.locations[0].name);
 await page.setViewportSize({width:390,height:844});expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
 }
});
