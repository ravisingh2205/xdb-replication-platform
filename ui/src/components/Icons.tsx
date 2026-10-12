import type { DbType } from '../api/types';

/** Small inline SVG icons for the tree and dialogs (no icon library needed). */
export function DbIcon({ type, size = 18 }: { type?: DbType; size?: number }) {
  const color = type === 'ORACLE' ? '#c74634' : type === 'SQLSERVER' ? '#a91d22' : type === 'EPAS' ? '#2b6cb0' : '#336791';
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" aria-hidden className="icon">
      <ellipse cx="12" cy="5" rx="8" ry="3" fill="none" stroke={color} strokeWidth="1.8" />
      <path d="M4 5v14c0 1.7 3.6 3 8 3s8-1.3 8-3V5" fill="none" stroke={color} strokeWidth="1.8" />
      <path d="M4 12c0 1.7 3.6 3 8 3s8-1.3 8-3" fill="none" stroke={color} strokeWidth="1.8" />
    </svg>
  );
}

export function ServerIcon({ size = 18 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" aria-hidden className="icon">
      <rect x="3" y="3" width="18" height="7" rx="1.5" fill="#d9e2ec" stroke="#486581" />
      <rect x="3" y="14" width="18" height="7" rx="1.5" fill="#d9e2ec" stroke="#486581" />
      <circle cx="7" cy="6.5" r="1" fill="#2f855a" />
      <circle cx="7" cy="17.5" r="1" fill="#2f855a" />
    </svg>
  );
}

export function FolderIcon({ size = 18 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" aria-hidden className="icon">
      <path d="M3 6h7l2 2h9v11H3z" fill="#f6d78b" stroke="#b7791f" />
    </svg>
  );
}

export function PubIcon({ size = 18, color = '#2f855a' }: { size?: number; color?: string }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" aria-hidden className="icon">
      <rect x="4" y="3" width="16" height="18" rx="2" fill="none" stroke={color} strokeWidth="1.8" />
      <path d="M8 8h8M8 12h8M8 16h5" stroke={color} strokeWidth="1.8" />
    </svg>
  );
}

export function StatusDot({ status }: { status: string }) {
  const cls = status === 'RUNNING' || status === 'ACTIVE' ? 'green'
    : status === 'FAILED' ? 'red'
    : status === 'PAUSED' ? 'amber' : 'grey';
  return <span className={`dot ${cls}`} aria-label={status} />;
}
