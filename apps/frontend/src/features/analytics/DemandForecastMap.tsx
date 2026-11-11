'use client';

import {useState} from 'react';
import shapes from './provinces.json';
import styles from './demand-forecast.module.css';

// This map intentionally has no predicted values until a trained model supplies them.
// Observed social activity and court listings are not substituted for forecasts.
export function DemandForecastMap() {
  const [selected,setSelected]=useState('');
  const province=shapes.find(shape=>shape.id===selected);
  function choose(id:string){setSelected(id);}

  return <section className={styles.panel} aria-labelledby='forecast-title'>
    <header className={styles.heading}>
      <div><h2 id='forecast-title'>Demand forecast</h2><p>Predicted pickleball demand across Vietnam.</p></div>
      <span className={styles.status}>Awaiting model results</span>
    </header>
    <div className={styles.body}>
      <div className={styles.mapArea}>
        <svg viewBox='0 0 800 930' className={styles.map} role='group' aria-label='Vietnam demand forecast map, 34 provinces. Predictions are not available yet.'>
          {shapes.map(shape=><path key={shape.id} d={shape.path} fillRule='evenodd'
            className={styles.province} data-selected={selected===shape.id}
            vectorEffect='non-scaling-stroke' role='button' tabIndex={0}
            aria-label={`${shape.name}: forecast unavailable`} aria-pressed={selected===shape.id}
            onClick={()=>choose(shape.id)} onKeyDown={event=>{if(event.key==='Enter'||event.key===' '){event.preventDefault();choose(shape.id);}}}>
            <title>{shape.name}: forecast unavailable</title>
          </path>)}
        </svg>
        <a className={styles.credit} href='https://github.com/TaliaCao/Vietnam-Shape-Map' target='_blank' rel='noreferrer'>Map: Talia Cao · MIT</a>
      </div>
      <aside className={styles.sidebar} aria-label='Forecast details'>
        <label className={styles.field}>Forecast period<select disabled aria-describedby='forecast-waiting'><option>Awaiting forecast</option></select></label>
        <label className={styles.field} htmlFor='forecast-province'>Province<select id='forecast-province' value={selected} onChange={event=>choose(event.target.value)}><option value=''>Choose a province</option>{shapes.map(shape=><option key={shape.id} value={shape.id}>{shape.name}</option>)}</select></label>
        <div className={styles.details} aria-live='polite'>
          <h3>{province?.name??'Vietnam'}</h3>
          <dl><div><dt>Predicted demand</dt><dd>Not available</dd></div><div><dt>Confidence</dt><dd>Not available</dd></div></dl>
          {province&&<button type='button' onClick={()=>choose('')}>Clear selection</button>}
        </div>
        <div className={styles.waiting} id='forecast-waiting'><h3>Forecast coming soon</h3><p>Predictions will appear when model results are connected. Grey provinces have no forecast yet.</p></div>
        <div className={styles.legend} aria-label='Future demand color scale: low, moderate, high'>
          <strong>Predicted demand</strong><div className={styles.ramp}/><div className={styles.ticks}><span>Low</span><span>Moderate</span><span>High</span></div>
          <span className={styles.noForecast}><i/>No forecast</span>
        </div>
      </aside>
    </div>
  </section>;
}
