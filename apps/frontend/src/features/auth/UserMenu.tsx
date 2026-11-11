"use client";
import {useEffect, useRef, useState} from 'react';
import Link from 'next/link';
import {useRouter} from 'next/navigation';
import {useAuth} from './AuthProvider';
import styles from './auth.module.css';

export function UserMenu() {
  const {user,acceptAccount} = useAuth();
  const router = useRouter();
  const [open, setOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const container = useRef<HTMLDivElement>(null);
  const button = useRef<HTMLButtonElement>(null);
  useEffect(() => {
    if (!open) return;
    function dismiss(event: PointerEvent) {
      if (!container.current?.contains(event.target as Node)) setOpen(false);
    }
    document.addEventListener('pointerdown', dismiss);
    return () => document.removeEventListener('pointerdown', dismiss);
  }, [open]);
  async function signOut() {
    setBusy(true); setError('');
    try {
      const response = await fetch('/api/auth/logout', {method: 'POST'});
      if (!response.ok) throw Error();
      setOpen(false); acceptAccount(null); router.replace('/login'); router.refresh();
    } catch { setError('Could not sign out. Try again.'); }
    finally { setBusy(false); }
  }
  return <div ref={container} className={styles.userMenu}
    onBlur={event => {if (!event.currentTarget.contains(event.relatedTarget as Node | null)) setOpen(false);}}
    onKeyDown={event => {if (event.key === 'Escape') {setOpen(false); button.current?.focus();}}}>
    <button ref={button} type='button' className={styles.avatar} aria-label='User account'
      aria-expanded={open} aria-controls='account-dropdown' onClick={() => setOpen(value => !value)}>
      <svg width='19' height='19' viewBox='0 0 24 24' fill='none' stroke='currentColor' strokeWidth='1.7' aria-hidden='true'><circle cx='12' cy='8' r='4'/><path d='M4 21v-2a8 8 0 0 1 16 0v2'/></svg>
    </button>
    {open && <div id='account-dropdown' className={styles.dropdown}>
      {user && <><strong>{user.name}</strong><small>{user.email}</small></>}
      <Link href='/account' onClick={() => setOpen(false)}>My account</Link>
      {!user?.mustChangePassword && <>
        {user?.role === 'admin' && <Link href='/admin' onClick={() => setOpen(false)}>User management</Link>}
        <Link href='/settings' onClick={() => setOpen(false)}>Settings</Link>
      </>}
      <button type='button' disabled={busy} onClick={() => void signOut()}>{busy ? 'Signing out…' : 'Sign out'}</button>
      {error && <small role='alert'>{error}</small>}
    </div>}
  </div>;
}
