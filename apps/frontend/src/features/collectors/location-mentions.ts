export type MentionReference = { location_id: string; canonical_name: string; aliases: string[] };
export type LocationMentions = { total: number; matched: number; unmatched: number; locations: { id: string; name: string; count: number }[] };
function normalize(value: string) {
 return value.normalize("NFD").replace(/[\u0300-\u036f]/g, "").replace(/[đĐ]/g,"d").toLowerCase().replace(/[^\p{L}\p{N}]+/gu," ").trim();
}
export function locationMentions(rows: {text: string}[], references: MentionReference[]): LocationMentions {
 const catalog = references.map(r=>({...r, terms:[...new Set([r.canonical_name,...r.aliases].map(normalize).filter(Boolean))]}));
 const counts = new Map<string,number>(); let matched=0;
 for(const row of rows) {
  // Links may contain unrelated place names, so match only visible text.
  const text=" "+normalize(row.text.replace(/https?:\/\/\S+/gi," "))+" "; let found=false;
  for(const ref of catalog) if(ref.terms.some(term=>text.includes(" "+term+" "))) {
   counts.set(ref.location_id,(counts.get(ref.location_id)??0)+1);found=true;
  }
  if(found)matched++;
 }
 return {total:rows.length,matched,unmatched:rows.length-matched,locations:catalog.filter(r=>counts.has(r.location_id)).map(r=>({id:r.location_id,name:r.canonical_name,count:counts.get(r.location_id)!})).sort((a,b)=>b.count-a.count||a.name.localeCompare(b.name,"vi"))};
}
