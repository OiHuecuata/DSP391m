"use client";
import {useAuth} from "@/features/auth/AuthProvider";
import {createContext,useContext,useEffect,useState,type ReactNode} from "react";
import {defaults,preferenceKey,validatePreferences,type Preferences} from "./preferences";
const Context=createContext<{preferences:Preferences;ready:boolean;save:(v:Preferences)=>boolean}>({preferences:defaults,ready:false,save:()=>false});
export function PreferencesProvider({children}:{children:ReactNode}){
 const {user,loading}=useAuth();const key=preferenceKey+":"+(user?.id??"guest");
 const [preferences,setPreferences]=useState(defaults),[loadedKey,setLoadedKey]=useState("");const ready=!loading&&loadedKey===key;
 useEffect(()=>{function read(){try{setPreferences(validatePreferences(JSON.parse(localStorage.getItem(key)??localStorage.getItem(preferenceKey)??"null")))}catch{setPreferences(defaults);}setLoadedKey(key);}const timer=setTimeout(read,0);window.addEventListener("storage",read);return()=>{clearTimeout(timer);window.removeEventListener("storage",read)};},[key]);
 useEffect(()=>{document.documentElement.dataset.preferenceKey=key;document.documentElement.dataset.theme=preferences.theme;document.documentElement.lang=preferences.language;},[preferences.theme,preferences.language,key]);
 function save(value:Preferences){try{const next=validatePreferences(value);localStorage.setItem(key,JSON.stringify(next));setPreferences(next);return true}catch{return false;}}
 return <Context.Provider value={{preferences,ready,save}}>{children}</Context.Provider>;
}
export const usePreferences=()=>useContext(Context);
export const vietnamese:Record<string,string>={"Chart sources":"Nguồn biểu đồ",Courts:"Sân", "Explore data":"Khám phá dữ liệu", "All records":"Tất cả bản ghi",Collectors:"Thu thập",Threads:"Threads",YouTube:"YouTube",PickleballPlus:"PickleballPlus",TikTok:"TikTok","Data Operations":"Vận hành dữ liệu",Dashboard:"Tổng quan",Analytics:"Phân tích",Settings:"Cài đặt",Homepage:"Trang chủ",Search:"Tìm kiếm","S3 connection":"Kết nối S3","Automatic updates":"Tự động cập nhật","Dashboard defaults":"Mặc định tổng quan","Display preferences":"Hiển thị","Downloads":"Tải xuống",About:"Giới thiệu","Test connection":"Kiểm tra kết nối","Sync now":"Đồng bộ ngay","Save preferences":"Lưu tùy chọn","Reset preferences":"Khôi phục mặc định","Enable automatic sync":"Bật đồng bộ tự động","Check interval":"Chu kỳ kiểm tra","Default sources":"Nguồn mặc định","Chart colors":"Màu biểu đồ","Default province":"Tỉnh mặc định",Theme:"Giao diện",Language:"Ngôn ngữ","Date format":"Định dạng ngày",Timezone:"Múi giờ","Default format":"Định dạng mặc định","Default export scope":"Phạm vi tải mặc định",Light:"Sáng",Dark:"Tối","All matching results":"Tất cả kết quả phù hợp","Current page":"Trang hiện tại","No default":"Không chọn","Saved on this browser":"Đã lưu trên trình duyệt này"};
