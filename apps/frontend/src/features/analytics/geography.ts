export type GeoReference = { geo_id: string; canonical_name: string; aliases: string[]; admin_version: string; level: string; current_name?: string };
export function normalize(value: string) {
  return value.normalize("NFD").replace(/[\u0300-\u036f]/g, "").replace(/[đĐ]/g, "d").toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
}
export function provinceMatcher(references: GeoReference[]) {
  const provinces = references.filter(r => r.admin_version === "2025_07" && r.level === "province");
  const local: Record<string,string> = {"Nha Trang":"Khánh Hòa","Vũng Tàu":"Hồ Chí Minh","Cầu Giấy":"Hà Nội","Bắc Từ Liêm":"Hà Nội","Nam Từ Liêm":"Hà Nội","Thủ Đức":"Hồ Chí Minh"};
  const terms = references.filter(r => r.level === "province" || r.level === "locality").flatMap(r => {
    const target = r.level === "locality" ? local[r.canonical_name] : r.current_name ?? r.canonical_name;
    const province = provinces.find(p => normalize(p.canonical_name) === normalize(target ?? ""));
    return province ? [...new Set([r.canonical_name,...r.aliases].map(normalize))].filter(Boolean).map(term => ({term,id:province.geo_id})) : [];
  }).sort((a,b)=>b.term.length-a.term.length);
  return (value: string) => {
    let text = ` ${normalize(value.replace(/https?:\/\/\S+/gi," "))} `;
    const found = new Set<string>();
    for(const {term,id} of terms) if(text.includes(` ${term} `)) {
      found.add(id);
      text = text.split(` ${term} `).join(" ");
    }
    return [...found];
  };
}
