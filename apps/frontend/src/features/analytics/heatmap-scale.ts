export type HeatmapBand={label:string;color:string};
const quiet='#edf0e9';
const number=(value:number)=>value.toLocaleString('en-GB',{maximumFractionDigits:3});
export function heatmapScale(values:(number|null)[],signed:boolean,palette:'spectrum'|'warm'|'blue',integer:boolean){
 if(signed){
  const colors=['#08519c','#3182bd','#6baed6','#c6dbef',quiet,'#fee391','#fec44f','#f46d43','#b30000'];
  const labels=['−8 or less','−8 to −3','−3 to −1','−1 to −0.25','Near zero','+0.25 to +1','+1 to +3','+3 to +8','+8 or more'];
  return {bands:labels.map((label,i)=>({label,color:colors[i]})),fill:(value:number)=>{const size=Math.abs(value),level=size>=8?4:size>=3?3:size>=1?2:size>=.25?1:0;return colors[4+(value<0?-level:level)];}};
 }
 const positive=values.filter((value):value is number=>value!==null&&Number.isFinite(value)&&value>0).sort((a,b)=>a-b);
 const starts=[...new Set([0,.2,.4,.6,.8].map(q=>positive[Math.floor(q*positive.length)]).filter((value):value is number=>value!==undefined))];
 const colors=palette==='warm'?['#ffffb2','#fed976','#feb24c','#f03b20','#bd0026']:palette==='blue'?['#c6dbef','#6baed6','#4292c6','#2171b5','#08306b']:['#2c7bb6','#1a9850','#fee08b','#f46d43','#b30000'];
 const bins=starts.map((min,index)=>{const next=starts[index+1],max=positive.at(-1)!;const upper=next===undefined?max:integer?next-1:next;const label=min===upper?number(min):`${number(min)}–${next!==undefined&&!integer?'<':''}${number(upper)}`;return {min,label,color:colors[starts.length===1?4:Math.round(index*4/(starts.length-1))]};});
 return {bands:[{label:'0',color:quiet},...bins.map(({label,color})=>({label,color}))],fill:(value:number)=>{if(value<=0)return quiet;return [...bins].reverse().find(bin=>value>=bin.min)?.color??quiet;}};
}
