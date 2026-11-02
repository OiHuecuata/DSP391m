import { test, expect } from '@playwright/test';
import { matchesHours, filterVenues, type VenueFilters } from '../src/features/courts/filters';
import { normalizeSnapshot } from '../src/features/courts/normalize';

const data = normalizeSnapshot({posts:[
 {id:'one',court_name:'Morning court',address:'Hà Nội',number_of_courts:4,services:['Có mái che','Để xe'],opening_hours:['Thứ Hai 06:00 - 22:00'],latitude:21,longitude:105},
 {id:'two',court_name:'Evening court',address:'Hà Nội',number_of_courts:8,services:['Để xe'],opening_hours:['Thứ Hai 18:00 - 24:00'],latitude:21.02,longitude:105.02},
 {id:'three',court_name:'Unknown court',number_of_courts:null,services:[],opening_hours:[],latitude:null,longitude:null},
 {id:'four',court_name:'Overnight court',number_of_courts:12,services:['Có mái che'],opening_hours:['Chủ Nhật 22:00 - 02:00'],latitude:20.9,longitude:105.1}
]},[{canonical_name:'Hà Nội',aliases:['ha noi']}],'2026-11-02','test.json');
const defaults: VenueFilters = {search:'',area:'',coordinates:'all',courtCount:'all',services:[],day:1,time:''};

test('reported schedules handle boundaries, overnight hours and ambiguous data',()=>{
 expect(matchesHours(['Thứ Hai 06:00 - 22:00'],1,'06:00')).toBe(true);
 expect(matchesHours(['Thứ Hai 06:00 - 22:00'],1,'22:00')).toBe(false);
 expect(matchesHours(['Thứ Hai 00:00 - 24:00'],1,'23:59')).toBe(true);
 expect(matchesHours(['Thứ Hai 00:00 - 00:00'],1,'12:00')).toBe(false);
 expect(matchesHours(['Chủ Nhật 22:00 - 02:00'],1,'01:00')).toBe(true);
 expect(matchesHours(['Chủ Nhật 22:00 - 02:00'],1,'02:00')).toBe(false);
 expect(matchesHours([],1,'12:00')).toBe(false);
 expect(matchesHours(['Thứ Hai 29:00 - 30:00'],1,'12:00')).toBe(false);
 expect(matchesHours(['Thứ Hai 06:00 - 22:00'],1,'24:00')).toBe(false);
});
test('court filters combine and missing values stay separate',()=>{
 expect(filterVenues(data.venues,{...defaults,services:['Có mái che','Để xe'],time:'07:00',courtCount:'1-4'}).map(v=>v.id)).toEqual(['one']);
 expect(filterVenues(data.venues,{...defaults,courtCount:'unknown'}).map(v=>v.id)).toEqual(['three']);
 expect(filterVenues(data.venues,{...defaults,courtCount:'5-8'}).map(v=>v.id)).toEqual(['two']);
 expect(filterVenues(data.venues,{...defaults,courtCount:'9+',time:'01:00'}).map(v=>v.id)).toEqual(['four']);
 expect(filterVenues(data.venues,{...defaults,coordinates:'missing',time:'07:00'})).toEqual([]);
 expect(data.venues[0].openingHours).toEqual(['Thứ Hai 06:00 - 22:00']);
});
