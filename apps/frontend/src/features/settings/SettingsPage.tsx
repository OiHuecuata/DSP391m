"use client";
import Link from 'next/link';
import {useEffect,useState} from 'react';
import {useAuth} from '@/features/auth/AuthProvider';
import {usePreferences,vietnamese} from './PreferencesProvider';
import {defaults,formatDate,type Preferences} from './preferences';
import {useS3Status} from '@/features/s3/status';
import provinces from '@/features/analytics/provinces.json';
import styles from './settings.module.css';
export function SettingsPage(){const {ready}=usePreferences();const {user,loading}=useAuth();return ready&&user&&!loading?<SettingsForm/>:<main className={styles.page}><h1>Settings</h1><p role='status'>Loading preferences…</p></main>;}
function SettingsForm(){
 const {preferences,save}=usePreferences(),{data:snapshot}=useS3Status(),{user}=useAuth();
 const [draft,setDraft]=useState(preferences),[notice,setNotice]=useState(''),[error,setError]=useState('');
 const dirty=JSON.stringify(draft)!==JSON.stringify(preferences);
 const t=(s:string)=>preferences.language==='vi'?vietnamese[s]??s:s;
 const update=<K extends keyof Preferences>(key:K,value:Preferences[K])=>{setDraft(old=>({...old,[key]:value}));setNotice('');setError('');};
 useEffect(()=>{if(!dirty)return;const warn=(event:BeforeUnloadEvent)=>{event.preventDefault();};window.addEventListener('beforeunload',warn);return()=>window.removeEventListener('beforeunload',warn);},[dirty]);
 function saveAll(){setError('');setNotice('');if(!save(draft)){setError('Browser storage is unavailable. Preferences were not saved.');return;}setNotice('Preferences saved.');}
 const row=(title:string,control:React.ReactNode,note?:string)=><div className={styles.row}><div><span className={styles.rowTitle}>{t(title)}</span>{note&&<small>{note}</small>}</div>{control}</div>;
 return <main className={styles.page}><header className={styles.heading}><div><h1>{t('Settings')}</h1></div><Link className={styles.accountLink} href='/account'>My account ↗</Link></header>
 <nav className={styles.sectionNav} aria-label='Settings sections'>{[['appearance','Display preferences'],['dashboard','Dashboard defaults'],['downloads','Downloads'],['about','About']].map(([id,label])=><a key={id} href={'#settings-'+id}>{t(label)}</a>)}</nav>
 <div className={styles.grid}>
 <section id='settings-appearance' className={styles.card}><h2>{t('Display preferences')}</h2>
 {row('Theme',<select aria-label={t('Theme')} value={draft.theme} onChange={e=>update('theme',e.target.value as Preferences['theme'])}><option value='light'>{t('Light')}</option><option value='dark'>{t('Dark')}</option></select>)}
 {row('Language',<select aria-label={t('Language')} value={draft.language} onChange={e=>update('language',e.target.value as Preferences['language'])}><option value='en'>English</option><option value='vi'>Tiếng Việt</option></select>)}
 {row('Date format',<select aria-label={t('Date format')} value={draft.dateFormat} onChange={e=>update('dateFormat',e.target.value as Preferences['dateFormat'])}><option value='day-first'>DD/MM/YYYY</option><option value='iso'>YYYY-MM-DD</option></select>)}
 {row('Timezone',<select aria-label={t('Timezone')} value={draft.timezone} onChange={e=>update('timezone',e.target.value as Preferences['timezone'])}><option value='Asia/Ho_Chi_Minh'>Vietnam · ICT (UTC+7)</option><option value='UTC'>UTC</option></select>)}
 <div className={styles.preview}><span>Date preview</span><strong>{formatDate(snapshot?.syncedAt,draft)}</strong></div></section>
 <section id='settings-dashboard' className={styles.card}><h2>{t('Dashboard defaults')}</h2>
 {row('Chart sources',<fieldset className={styles.sourceOptions}><legend className={styles.srOnly}>{t('Chart sources')}</legend>{(['threads','youtube','pickleballplus'] as const).map(source=><label key={source}><input type='checkbox' checked={draft.sources.includes(source)} onChange={e=>update('sources',e.target.checked?[...draft.sources,source]:draft.sources.filter(s=>s!==source))}/>{{threads:'Threads',youtube:'YouTube',pickleballplus:'PickleballPlus'}[source]}</label>)}</fieldset>)}
 {row('Chart colors',<select aria-label={t('Chart colors')} value={draft.palette} onChange={e=>update('palette',e.target.value as Preferences['palette'])}><option value='spectrum'>Spectrum</option><option value='warm'>Yellow → orange → red</option><option value='blue'>Pale → deep blue</option></select>)}
 {row('Default province',<select aria-label={t('Default province')} value={draft.province} onChange={e=>update('province',e.target.value)}><option value=''>{t('No default')}</option>{[...provinces].sort((a,b)=>a.name.localeCompare(b.name,'vi')).map(p=><option key={p.id} value={p.id}>{p.name}</option>)}</select>)}
 </section>
 <section id='settings-downloads' className={styles.card}><h2>{t('Downloads')}</h2>
 {row('Default format',<select aria-label={t('Default format')} value={draft.downloadFormat} onChange={e=>update('downloadFormat',e.target.value as Preferences['downloadFormat'])}><option value='csv'>CSV · spreadsheet</option><option value='json'>JSON · structured data</option></select>)}
 {row('Default export scope',<select aria-label={t('Default export scope')} value={draft.downloadScope} onChange={e=>update('downloadScope',e.target.value as Preferences['downloadScope'])}><option value='all'>{t('All matching results')}</option><option value='page'>{t('Current page')}</option></select>)}
 </section>
 <section id='settings-about' className={styles.card}><h2>{t('About')}</h2><div className={styles.row}><div><span className={styles.rowTitle}>PickleballDB</span><small>Local demo · version 0.1.0</small></div><span className={styles.tag}>34 provinces · Vietnam</span></div><dl className={styles.aboutList}><div><dt>Data sources</dt><dd>Threads, YouTube, PickleballPlus</dd></div><div><dt>Project goal</dt><dd>Vietnam court supply and demand-signal analysis</dd></div><div><dt>Predictive model</dt><dd>Awaiting validated results</dd></div><div><dt>Preference storage</dt><dd>This browser · your account</dd></div></dl>
 <details><summary>Dataset definitions</summary><p>Courts are venue listings; playing-court counts are source reported. Social totals are saved posts, videos and comments. Location matches identify places mentioned in text. Collection history includes repeated observations.</p></details>
 {user?.role==='admin'&&<Link href='/data-operations?source=storage' className={styles.textLink}>Manage S3 connection & automatic updates ↗</Link>}
 <div className={styles.resetRow}><span>{t('Reset preferences')}</span><button type='button' onClick={()=>{setDraft({...defaults,sources:[...defaults.sources]});setError('');setNotice('Defaults loaded. Save to apply.');}}>Restore defaults</button></div></section>
 </div>
 <footer className={styles.footer}><div>{error?<p role='alert' className={styles.error}>{error}</p>:notice?<p role='status' className={styles.notice}>{notice}</p>:<span role='status'>{dirty?'You have unsaved changes':t('Saved on this browser')}</span>}</div><div className={styles.saveActions}><button type='button' disabled={!dirty} onClick={()=>{setDraft(preferences);setError('');setNotice('Changes discarded.');}}>Discard</button><button type='button' disabled={!dirty} className={styles.save} onClick={saveAll}>{t('Save preferences')}</button></div></footer>
 </main>;
}
