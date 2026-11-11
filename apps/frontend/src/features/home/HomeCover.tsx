"use client";

import Image from "next/image";
import Link from "next/link";
import { useEffect, useRef, useState, useSyncExternalStore } from "react";
import styles from "./cover.module.css";

const slides = [
  {src:"/images/pickleball-game-4k.jpg",alt:"Players enjoying a pickleball game on outdoor courts",position:"50% 55%",credit:"Keith Cassill / Pexels",url:"https://www.pexels.com/photo/young-adults-enjoying-a-game-of-pickleball-outdoors-37661179/"},
  {src:"/images/pickleball-courts-4k.jpg",alt:"Sunlit pickleball courts with a net and long shadows",position:"center",credit:"Frank Schrader / Pexels",url:"https://www.pexels.com/photo/empty-pickleball-court-with-net-in-summer-sun-29820786/"},
  {src:"/images/pickleball-action-4k.jpg",alt:"Pickleball player returning a shot outdoors",position:"50% 40%",credit:"Sanket Mishra / Pexels",url:"https://www.pexels.com/photo/young-man-playing-pickleball-outdoors-34618470/"},
  {src:"/images/pickleball-player-4k.jpg",alt:"Close-up of a pickleball player holding a paddle on an outdoor court",position:"50% 45%",credit:"Rodrigo Ortega / Pexels",url:"https://www.pexels.com/photo/pickleball-player-on-outdoor-court-30864598/"},
  {src:"/images/pickleball-equipment-4k.jpg",alt:"Pickleball paddles and yellow balls on a colorful court",position:"50% 75%",credit:"Kadir Avşar / Pexels",url:"https://www.pexels.com/photo/vibrant-pickleball-equipment-on-court-36513707/"},
];
function subscribeMotion(callback: () => void) {
  const query = window.matchMedia("(prefers-reduced-motion: reduce)");
  query.addEventListener("change", callback); return () => query.removeEventListener("change", callback);
}
export function HomeCover() {
  const [index,setIndex] = useState(0);
  const [visible,setVisible] = useState(true);
  const touchX = useRef<number | null>(null);
  const reducedMotion = useSyncExternalStore(subscribeMotion, () => window.matchMedia("(prefers-reduced-motion: reduce)").matches, () => false);
  useEffect(() => {
    const change = () => setVisible(!document.hidden);
    document.addEventListener("visibilitychange",change); return () => document.removeEventListener("visibilitychange",change);
  },[]);
  useEffect(() => {
    if (!visible || reducedMotion) return;
    const timer = window.setInterval(() => setIndex(current => (current+1)%slides.length),6000);
    return () => window.clearInterval(timer);
  },[visible,reducedMotion,index]);
  function show(next: number) { setIndex((next+slides.length)%slides.length); }
  return <div className={styles.coverWrapper}>
    <div className={styles.separator} aria-hidden="true" />
    <section className={styles.cover} aria-label="Pickleball homepage cover" aria-roledescription="carousel"
      onTouchStart={event => { touchX.current = event.touches[0]?.clientX ?? null; }}
      onTouchEnd={event => { if (touchX.current != null) { const distance = touchX.current-(event.changedTouches[0]?.clientX ?? touchX.current); if (Math.abs(distance)>50) show(index+(distance>0?1:-1)); } touchX.current=null; }}>
      <div className={styles.slideTrack} style={{transform:`translateX(-${index*100}%)`}}>
        {slides.map((slide,i) => <div key={slide.src} className={styles.slide} role="group" aria-roledescription="slide" aria-label={`${i+1} of ${slides.length}`} aria-hidden={i!==index}>
          <Image src={slide.src} alt={slide.alt} unoptimized fill sizes="100vw" loading="eager" fetchPriority={i===0?"high":"low"} className={styles.photo} style={{objectPosition:slide.position}} />
        </div>)}
      </div>
      <div className={styles.intro}><h2>Vietnam’s pickleball<br/>at a glance.</h2><Link href="/dashboard">Open dashboard <span aria-hidden="true">↗</span></Link></div>
      <button className={`${styles.arrow} ${styles.previous}`} aria-label="Previous photo" onClick={() => show(index-1)}>‹</button>
      <button className={`${styles.arrow} ${styles.next}`} aria-label="Next photo" onClick={() => show(index+1)}>›</button>
      <a className={styles.scrollLink} href="#system-status">View data overview <span aria-hidden="true">↓</span></a>
      <div className={styles.carouselControls}>
        {slides.map((slide,i) => <button key={slide.src} aria-label={`Show photo ${i+1}`} aria-pressed={index===i} className={styles.dot} onClick={() => show(i)} />)}
      </div>
      {slides[index].credit && <a className={styles.photoCredit} href={slides[index].url} target="_blank" rel="noopener noreferrer">Photo: {slides[index].credit}</a>}
    </section>
  </div>;
}
