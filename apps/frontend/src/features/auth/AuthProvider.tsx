'use client';
import {createContext,useCallback,useContext,useEffect,useRef,useState,type ReactNode} from 'react';
import type {PublicAccount} from './server';
type Auth={user:PublicAccount|null;loading:boolean;reload:()=>Promise<void>;acceptAccount:(account:PublicAccount|null)=>void};
const Context=createContext<Auth>({user:null,loading:true,reload:async()=>{},acceptAccount:()=>{}});
export function AuthProvider({children}:{children:ReactNode}){
 const [user,setUser]=useState<PublicAccount|null>(null),[loading,setLoading]=useState(true);
 const pending=useRef<AbortController|null>(null),generation=useRef(0),lastCheck=useRef(0);
 const acceptAccount=useCallback((account:PublicAccount|null)=>{generation.current++;pending.current?.abort();lastCheck.current=Date.now();setUser(previous=>JSON.stringify(previous)===JSON.stringify(account)?previous:account);setLoading(false);},[]);
 const reload=useCallback(async()=>{
  const current=++generation.current;pending.current?.abort();const controller=new AbortController();pending.current=controller;
  try{const response=await fetch('/api/auth/me',{cache:'no-store',signal:controller.signal});const result=await response.json();if(current===generation.current&&!controller.signal.aborted){const account=result.user??null;setUser(previous=>JSON.stringify(previous)===JSON.stringify(account)?previous:account);lastCheck.current=Date.now();}}
  catch{if(current===generation.current&&!controller.signal.aborted)setUser(null);}
  finally{if(current===generation.current&&!controller.signal.aborted)setLoading(false);}
 },[]);
 useEffect(()=>{const initial=setTimeout(()=>void reload(),0);const check=()=>{if(document.visibilityState==='visible'&&Date.now()-lastCheck.current>30000)void reload();};const timer=setInterval(check,60000);window.addEventListener('focus',check);document.addEventListener('visibilitychange',check);return()=>{clearTimeout(initial);pending.current?.abort();clearInterval(timer);window.removeEventListener('focus',check);document.removeEventListener('visibilitychange',check);};},[reload]);
 return <Context.Provider value={{user,loading,reload,acceptAccount}}>{children}</Context.Provider>;
}
export const useAuth=()=>useContext(Context);
