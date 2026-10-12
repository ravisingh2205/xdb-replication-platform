import { useEffect, type ReactNode } from 'react';

/** Modal window styled after the legacy desktop console: title bar with traffic lights, grey body. */
export function Window(props: {
  title: string;
  width?: number;
  onClose: () => void;
  children: ReactNode;
  footer?: ReactNode;
}) {
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === 'Escape' && props.onClose();
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [props]);

  return (
    <div className="modal-backdrop" onMouseDown={(e) => e.target === e.currentTarget && props.onClose()}>
      <div className="window" style={{ width: props.width ?? 760 }} role="dialog" aria-label={props.title}>
        <div className="window-titlebar">
          <span className="lights">
            <button className="light red" aria-label="Close" onClick={props.onClose} />
            <span className="light" />
            <span className="light" />
          </span>
          <span className="window-title">{props.title}</span>
        </div>
        <div className="window-body">{props.children}</div>
        {props.footer && <div className="window-footer">{props.footer}</div>}
      </div>
    </div>
  );
}

/** Folder-style tabs as in the legacy dialogs. */
export function Tabs(props: {
  tabs: { id: string; label: string; disabled?: boolean; title?: string }[];
  active: string;
  onChange: (id: string) => void;
}) {
  return (
    <div className="tabs" role="tablist">
      {props.tabs.map((t) => (
        <button
          key={t.id}
          role="tab"
          aria-selected={props.active === t.id}
          className={`tab ${props.active === t.id ? 'active' : ''}`}
          disabled={t.disabled}
          title={t.title}
          onClick={() => props.onChange(t.id)}
        >
          {t.label}
        </button>
      ))}
    </div>
  );
}

export function GroupBox(props: { legend: string; children: ReactNode; className?: string }) {
  return (
    <fieldset className={`groupbox ${props.className ?? ''}`}>
      {props.legend && <legend>{props.legend}</legend>}
      {props.children}
    </fieldset>
  );
}
