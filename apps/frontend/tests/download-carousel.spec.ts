import { test, expect } from '@playwright/test';
import { courtDownload } from '../src/features/courts/download';
import { normalizeSnapshot } from '../src/features/courts/normalize';
const snapshot = normalizeSnapshot({ collected_at:'2026-10-10T12:00:00Z',posts:[
 {id:'one',court_name:'=HYPERLINK("bad")',address:'Address, with comma',number_of_courts:4,services:['Để xe'],opening_hours:['Thứ Hai 06:00 - 22:00'],latitude:21,longitude:105},
 {id:'two',court_name:'Other court',number_of_courts:8,services:[],opening_hours:[]}
]},[],'2026-10-10','test.json');
test('CSV protects spreadsheet formulas, quotes content and preserves unknown values',()=>{
 const csv=courtDownload(snapshot,snapshot.venues,'csv','all');
 expect(csv.content.startsWith('\uFEFF')).toBe(true);
 expect(csv.content).toContain('"\'=HYPERLINK(""bad"")"');
 expect(csv.content).toContain('"Address, with comma"');
 expect(csv.content).toContain('Thứ Hai 06:00 - 22:00');
 const json=JSON.parse(courtDownload(snapshot,snapshot.venues,'json','all').content);
 expect(json.count).toBe(2);expect(json.venues[1].latitude).toBeNull();expect(json.collected_at).toBe('2026-10-10T12:00:00Z');
});
test('download includes all court listings',async({page})=>{
 await page.route('**/api/pbplus/venues',r=>r.fulfill({json:snapshot}));
 await page.goto('/collectors/pickleballplus');
 const pending=page.waitForEvent('download');await page.getByRole('button',{name:'Download file'}).click();const csv=await pending;
 expect(csv.suggestedFilename()).toBe('pickleball-courts-all-2026-10-10.csv');
 const stream=await csv.createReadStream();let content='';for await (const chunk of stream!) content+=chunk.toString();
 expect(content).toContain('HYPERLINK');expect(content).toContain('Other court');
 await page.getByRole('combobox',{name:'File format',exact:true}).selectOption('json');
 const next=page.waitForEvent('download');await page.getByRole('button',{name:'Download file'}).click();const jsonFile=await next;
 const jsonStream=await jsonFile.createReadStream();let raw='';for await(const chunk of jsonStream!) raw+=chunk.toString();
 expect(JSON.parse(raw).count).toBe(2);

});
test('carousel advances continuously, has dot controls and a white separator',async({page})=>{
 await page.clock.install();await page.goto('/');
 const cover=page.getByRole('region',{name:'Pickleball homepage cover'});
 await expect(cover.getByRole('button',{name:'Show photo 1',exact:true})).toHaveAttribute('aria-pressed','true');
 const gap=await cover.evaluate(el=>el.getBoundingClientRect().top-document.querySelector('header')!.getBoundingClientRect().bottom);
 expect(gap).toBe(6);
 await page.clock.fastForward(6100);
 await expect(cover.getByRole('button',{name:'Show photo 2',exact:true})).toHaveAttribute('aria-pressed','true');
 await expect(cover.getByRole('button',{name:'Pause slideshow'})).toHaveCount(0);
 await expect(cover.getByRole('button',{name:'Play slideshow'})).toHaveCount(0);
 await cover.hover();
 await page.clock.fastForward(6100);
 await expect(cover.getByRole('button',{name:'Show photo 3',exact:true})).toHaveAttribute('aria-pressed','true');
 await cover.getByRole('button',{name:'Show photo 2',exact:true}).click();
 await cover.getByRole('button',{name:'Next photo',exact:true}).click();
 await expect(cover.getByRole('button',{name:'Show photo 3',exact:true})).toHaveAttribute('aria-pressed','true');
 await cover.getByRole('button',{name:'Show photo 5',exact:true}).click();
 await cover.getByRole('button',{name:'Next photo',exact:true}).click();
 await expect(cover.getByRole('button',{name:'Show photo 1',exact:true})).toHaveAttribute('aria-pressed','true');
 await page.clock.fastForward(6100);
 await expect(cover.getByRole('button',{name:'Show photo 2',exact:true})).toHaveAttribute('aria-pressed','true');
 await cover.getByRole('button',{name:'Show photo 1',exact:true}).click();
 await page.clock.runFor(800);
 await page.screenshot({path:'/home/coara/Documents/Codex/2026-10-26/i-j/outputs/home-carousel.png'});
 await page.setViewportSize({width:390,height:844});
 expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
 await page.screenshot({path:'/home/coara/Documents/Codex/2026-10-26/i-j/outputs/home-carousel-mobile.png'});
});
test('reduced-motion preference disables automatic rotation',async({page})=>{
 await page.emulateMedia({reducedMotion:'reduce'});await page.clock.install();await page.goto('/');await page.clock.fastForward(12000);
 await expect(page.getByRole('button',{name:'Show photo 1',exact:true})).toHaveAttribute('aria-pressed','true');
 await expect(page.getByRole('button',{name:'Pause slideshow'})).toHaveCount(0);
 await page.getByRole('button',{name:'Next photo',exact:true}).click();
 await expect(page.getByRole('button',{name:'Show photo 2',exact:true})).toHaveAttribute('aria-pressed','true');
});
