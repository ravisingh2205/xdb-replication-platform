import { useCallback, useEffect, useMemo, useState, type ReactNode } from 'react';
import { api } from './api/client';
import { DB_TYPES, dbLabel, type ApplyStatus, type ConnectorStatus, type Database, type Publication, type Subscription } from './api/types';
import { AddDatabaseDialog } from './components/AddDatabaseDialog';
import { CreatePublicationDialog } from './components/CreatePublicationDialog';
import { CreateSubscriptionDialog } from './components/CreateSubscriptionDialog';
import { DbIcon, FolderIcon, PubIcon, ServerIcon, StatusDot } from './components/Icons';

type Selection =
  | { kind: 'pubServer' }
  | { kind: 'subServer' }
  | { kind: 'source'; name: string }
  | { kind: 'publication'; name: string }
  | { kind: 'target'; name: string }
  | { kind: 'subscription'; name: string };

type Dialog =
  | { kind: 'addSource' }
  | { kind: 'addTarget' }
  | { kind: 'createPublication'; source: Database }
  | { kind: 'createSubscription'; target: Database }
  | null;

const sameSel = (a: Selection, b: Selection) =>
  a.kind === b.kind && ('name' in a ? a.name : '') === ('name' in b ? b.name : '');

export default function App() {
  const [sources, setSources] = useState<Database[]>([]);
  const [targets, setTargets] = useState<Database[]>([]);
  const [publications, setPublications] = useState<Publication[]>([]);
  const [subscriptions, setSubscriptions] = useState<Subscription[]>([]);
  const [apply, setApply] = useState<ApplyStatus[]>([]);
  const [connector, setConnector] = useState<ConnectorStatus | null>(null);
  const [online, setOnline] = useState<{ pub: boolean; sub: boolean }>({ pub: false, sub: false });
  const [selected, setSelected] = useState<Selection>({ kind: 'pubServer' });
  const [dialog, setDialog] = useState<Dialog>(null);
  const [message, setMessage] = useState<{ text: string; error?: boolean } | null>(null);
  const [expanded, setExpanded] = useState<Set<string>>(new Set(['pubServer', 'subServer']));

  const refresh = useCallback(async () => {
    const [s, p, t, sub, a] = await Promise.allSettled([
      api.databases('source'), api.publications(), api.databases('target'), api.subscriptions(), api.applyStatus(),
    ]);
    if (s.status === 'fulfilled') setSources(s.value);
    if (p.status === 'fulfilled') setPublications(p.value);
    if (t.status === 'fulfilled') setTargets(t.value);
    if (sub.status === 'fulfilled') setSubscriptions(sub.value);
    if (a.status === 'fulfilled') setApply(a.value);
    setOnline({ pub: s.status === 'fulfilled', sub: t.status === 'fulfilled' });
  }, []);

  useEffect(() => {
    refresh();
    const id = window.setInterval(refresh, 5000);
    return () => window.clearInterval(id);
  }, [refresh]);

  useEffect(() => {
    setConnector(null);
    if (selected.kind === 'publication') {
      api.connector(selected.name).then(setConnector).catch(() => setConnector(null));
    }
  }, [selected, publications]);

  const act = async (label: string, fn: () => Promise<unknown>, confirmText?: string) => {
    if (confirmText && !window.confirm(confirmText)) return;
    try {
      await fn();
      setMessage({ text: `${label}: done` });
      await refresh();
    } catch (e) {
      setMessage({ text: `${label}: ${(e as Error).message}`, error: true });
    }
  };

  const toggle = (key: string) => {
    const next = new Set(expanded);
    if (next.has(key)) {
      next.delete(key);
    } else {
      next.add(key);
    }
    setExpanded(next);
  };

  const closeDialog = (saved?: string) => {
    setDialog(null);
    if (saved) {
      setMessage({ text: saved });
      refresh();
    }
  };

  // ---------------------------------------------------------------- selected entity + properties + actions

  const view = useMemo(() => describe(selected, {
    sources, targets, publications, subscriptions, apply, connector, online,
  }), [selected, sources, targets, publications, subscriptions, apply, connector, online]);

  const actions: { label: string; onClick: () => void; danger?: boolean }[] = [];
  switch (selected.kind) {
    case 'pubServer':
      actions.push({ label: 'Add Database…', onClick: () => setDialog({ kind: 'addSource' }) });
      break;
    case 'subServer':
      actions.push({ label: 'Add Database…', onClick: () => setDialog({ kind: 'addTarget' }) });
      break;
    case 'source': {
      const db = sources.find((d) => d.name === selected.name);
      if (db) actions.push({ label: 'Create Publication…', onClick: () => setDialog({ kind: 'createPublication', source: db }) });
      actions.push({ label: 'Remove Database', danger: true, onClick: () => act('Remove database',
        () => api.removeDatabase('source', selected.name), `Remove source database ${selected.name}?`) });
      break;
    }
    case 'target': {
      const db = targets.find((d) => d.name === selected.name);
      if (db) actions.push({ label: 'Create Subscription…', onClick: () => setDialog({ kind: 'createSubscription', target: db }) });
      actions.push({ label: 'Remove Database', danger: true, onClick: () => act('Remove database',
        () => api.removeDatabase('target', selected.name), `Remove target database ${selected.name}?`) });
      break;
    }
    case 'publication':
      actions.push(
        { label: 'Pause', onClick: () => act('Pause', () => api.publicationAction(selected.name, 'pause')) },
        { label: 'Resume', onClick: () => act('Resume', () => api.publicationAction(selected.name, 'resume')) },
        { label: 'Restart Connector', onClick: () => act('Restart', () => api.publicationAction(selected.name, 'restart')) },
        { label: 'Remove Publication', danger: true, onClick: () => act('Remove publication', () => api.removePublication(selected.name),
          `Remove publication ${selected.name}? Its Debezium connector and replication slot are dropped.`) },
      );
      break;
    case 'subscription':
      actions.push(
        { label: 'Pause', onClick: () => act('Pause', () => api.subscriptionAction(selected.name, 'pause')) },
        { label: 'Resume', onClick: () => act('Resume', () => api.subscriptionAction(selected.name, 'resume')) },
        { label: 'Remove Subscription', danger: true, onClick: () => act('Remove subscription',
          () => api.removeSubscription(selected.name), `Remove subscription ${selected.name}?`) },
      );
      break;
  }

  // ---------------------------------------------------------------- tree

  const node = (sel: Selection, icon: ReactNode, label: ReactNode, childKey?: string, hasChildren?: boolean) => (
    <div
      className={`tree-node ${sameSel(sel, selected) ? 'selected' : ''}`}
      onClick={() => setSelected(sel)}
      onDoubleClick={() => childKey && toggle(childKey)}
    >
      <span className="twisty" onClick={(e) => { e.stopPropagation(); if (childKey) toggle(childKey); }}>
        {hasChildren ? (expanded.has(childKey!) ? '▾' : '▸') : ''}
      </span>
      {icon}
      <span className="tree-label">{label}</span>
    </div>
  );

  return (
    <div className="app">
      <header className="menubar">
        <span className="app-title">xDB Replication Console</span>
        <span className="menubar-sub">modernized · Debezium + Kafka</span>
        <span className="spacer" />
        <button className="btn small" onClick={() => setDialog({ kind: 'addSource' })}>Add Publication DB</button>
        <button className="btn small" onClick={() => setDialog({ kind: 'addTarget' })}>Add Subscription DB</button>
        <button className="btn small" onClick={refresh}>Refresh</button>
      </header>

      <div className="workspace">
        <nav className="tree" aria-label="Replication tree">
          {node({ kind: 'pubServer' }, <ServerIcon />, <>Publication Server <span className="muted">(api-gateway)</span></>, 'pubServer', true)}
          {expanded.has('pubServer') && (
            <div className="tree-children">
              {sources.length === 0 && <div className="tree-empty">No publication databases</div>}
              {sources.map((db) => {
                const pubs = publications.filter((p) => p.sourceDatabase === db.name);
                const key = `source:${db.name}`;
                return (
                  <div key={db.name}>
                    {node({ kind: 'source', name: db.name }, <DbIcon type={db.dbType} />,
                      <>{db.name} <span className="muted">({db.host}:{db.port})</span></>, key, pubs.length > 0)}
                    {expanded.has(key) && (
                      <div className="tree-children">
                        {pubs.map((p) => (
                          <div key={p.name}>
                            {node({ kind: 'publication', name: p.name }, <PubIcon />,
                              <><StatusDot status={p.status} />{p.name}</>)}
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          )}

          {node({ kind: 'subServer' }, <ServerIcon />, <>Subscription Server <span className="muted">(api-gateway)</span></>, 'subServer', true)}
          {expanded.has('subServer') && (
            <div className="tree-children">
              {targets.length === 0 && <div className="tree-empty">No subscription databases</div>}
              {targets.map((db) => {
                const subs = subscriptions.filter((s) => s.targetDatabase === db.name);
                const key = `target:${db.name}`;
                return (
                  <div key={db.name}>
                    {node({ kind: 'target', name: db.name }, <DbIcon type={db.dbType} />,
                      <>{db.name} <span className="muted">({db.host}:{db.port})</span></>, key, subs.length > 0)}
                    {expanded.has(key) && (
                      <div className="tree-children">
                        {subs.map((s) => (
                          <div key={s.name}>
                            {node({ kind: 'subscription', name: s.name }, <PubIcon color="#2b6cb0" />,
                              <><StatusDot status={s.active ? 'ACTIVE' : 'PAUSED'} />{s.name}</>)}
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          )}
        </nav>

        <main className="detail">
          <div className="detail-header">
            <h1>{view.title}</h1>
            <div className="actions">
              {actions.map((a) => (
                <button key={a.label} className={`btn ${a.danger ? 'danger' : ''}`} onClick={a.onClick}>{a.label}</button>
              ))}
            </div>
          </div>
          <div className="prop-box">
            <table className="props">
              <thead><tr><th>Property</th><th>Value</th></tr></thead>
              <tbody>
                {view.rows.map(([k, v]) => (
                  <tr key={k}><td>{k}</td><td>{v}</td></tr>
                ))}
              </tbody>
            </table>
          </div>

          <MonitorPanel apply={apply} onSelect={(name) => setSelected({ kind: 'subscription', name })} />
        </main>
      </div>

      <footer className="statusbar">
        <span><StatusDot status={online.pub ? 'RUNNING' : 'FAILED'} />Publication service</span>
        <span><StatusDot status={online.sub ? 'RUNNING' : 'FAILED'} />Subscription service</span>
        <span>{publications.length} publication(s) · {subscriptions.length} subscription(s)</span>
        <span className="spacer" />
        {message && <span className={message.error ? 'err' : 'ok'}>{message.text}</span>}
      </footer>

      {dialog?.kind === 'addSource' && (
        <AddDatabaseDialog side="source" onClose={() => closeDialog()} onSaved={() => closeDialog('Publication database added')} />
      )}
      {dialog?.kind === 'addTarget' && (
        <AddDatabaseDialog side="target" onClose={() => closeDialog()} onSaved={() => closeDialog('Subscription database added')} />
      )}
      {dialog?.kind === 'createPublication' && (
        <CreatePublicationDialog source={dialog.source} onClose={() => closeDialog()}
                                 onSaved={() => closeDialog('Publication created; Debezium connector is being provisioned')} />
      )}
      {dialog?.kind === 'createSubscription' && (
        <CreateSubscriptionDialog target={dialog.target} publications={publications} onClose={() => closeDialog()}
                                  onSaved={() => closeDialog('Subscription created; apply consumer is starting')} />
      )}
    </div>
  );
}

// ---------------------------------------------------------------- property grid content

function describe(sel: Selection, d: {
  sources: Database[]; targets: Database[]; publications: Publication[]; subscriptions: Subscription[];
  apply: ApplyStatus[]; connector: ConnectorStatus | null; online: { pub: boolean; sub: boolean };
}): { title: string; rows: [string, ReactNode][] } {
  const status = (s: string) => <><StatusDot status={s} />{s}</>;
  switch (sel.kind) {
    case 'pubServer':
      return {
        title: 'Publication Server',
        rows: [
          ['Service status', status(d.online.pub ? 'RUNNING' : 'UNREACHABLE')],
          ['Endpoint', '/api/source-databases, /api/publications'],
          ['Change capture', 'Debezium source connectors on Kafka Connect (log-based)'],
          ['Publication databases', d.sources.length],
          ['Publications', d.publications.length],
          ['Running', d.publications.filter((p) => p.status === 'RUNNING').length],
          ['Failed', d.publications.filter((p) => p.status === 'FAILED').length],
        ],
      };
    case 'subServer':
      return {
        title: 'Subscription Server',
        rows: [
          ['Service status', status(d.online.sub ? 'RUNNING' : 'UNREACHABLE')],
          ['Endpoint', '/api/target-databases, /api/subscriptions'],
          ['Apply engine', 'apply-service: one Kafka consumer group per subscription'],
          ['Subscription databases', d.targets.length],
          ['Subscriptions', d.subscriptions.length],
          ['Total consumer lag', d.apply.reduce((n, a) => n + a.totalLag, 0).toLocaleString()],
        ],
      };
    case 'source':
    case 'target': {
      const list = sel.kind === 'source' ? d.sources : d.targets;
      const db = list.find((x) => x.name === sel.name);
      if (!db) return { title: sel.name, rows: [] };
      return {
        title: `${sel.kind === 'source' ? 'Publication' : 'Subscription'} Database: ${db.name}`,
        rows: [
          ['Database type', dbLabel(db.dbType)],
          ['Host', db.host],
          ['Port', db.port],
          ['Database', db.databaseName],
          ...(db.pdbName ? [['PDB', db.pdbName] as [string, ReactNode]] : []),
          ['User', db.username],
          ['URL options', db.urlOptions ?? '-'],
          ...(sel.kind === 'source'
            ? [['Changeset logging', DB_TYPES.find((t) => t.value === db.dbType)?.cdc] as [string, ReactNode]]
            : [['Write method', db.dbType === 'POSTGRES' || db.dbType === 'EPAS'
                ? 'INSERT … ON CONFLICT DO UPDATE' : 'MERGE (upsert)'] as [string, ReactNode]]),
          ['Password', 'stored in secret store'],
          ['Added', new Date(db.createdAt).toLocaleString()],
        ],
      };
    }
    case 'publication': {
      const p = d.publications.find((x) => x.name === sel.name);
      if (!p) return { title: sel.name, rows: [] };
      const c = d.connector;
      return {
        title: `Publication: ${p.name}`,
        rows: [
          ['Status', status(p.status)],
          ['Status detail', p.statusMessage ?? '-'],
          ['Publication database', `${p.sourceDatabase} (${dbLabel(p.sourceDbType)})`],
          ['Tables', `${p.tables.length}: ${p.tables.slice(0, 8).join(', ')}${p.tables.length > 8 ? ' …' : ''}`],
          ['Row filters', p.rowFilters.length === 0 ? 'none'
            : p.rowFilters.map((f) => `${f.table}.${f.column} ${f.operator} (${f.values.join(', ')})`).join('; ')],
          ['Snapshot', p.snapshotMode],
          ['Topic routing', p.topicRouting],
          ['Topic prefix', p.topicPrefix],
          ['Partitions per topic', p.partitions],
          ['Debezium connector', p.connectorName],
          ['Connector state', c ? status(c.connector.state) : '-'],
          ['Tasks', c ? c.tasks.map((t) => `#${t.id} ${t.state} @ ${t.worker_id}`).join(', ') : '-'],
          ['Created', new Date(p.createdAt).toLocaleString()],
        ],
      };
    }
    case 'subscription': {
      const s = d.subscriptions.find((x) => x.name === sel.name);
      if (!s) return { title: sel.name, rows: [] };
      const a = d.apply.find((x) => x.subscription === s.name);
      return {
        title: `Subscription: ${s.name}`,
        rows: [
          ['Status', status(a?.state ?? (s.active ? 'STARTING' : 'PAUSED'))],
          ['Publication', s.publication],
          ['Subscription database', `${s.targetDatabase} (${dbLabel(s.targetDbType)})`],
          ['Consumer group', s.consumerGroup],
          ['Table mappings', s.tableMappings.map((m) =>
            `${m.sourceSchema}.${m.sourceTable} → ${m.targetSchema}.${m.targetTable}`).join('; ')],
          ['Consumer lag (records)', a ? a.totalLag.toLocaleString() : '-'],
          ['Replication delay', a?.replicationDelayMs != null ? `${(a.replicationDelayMs / 1000).toFixed(1)} s` : '-'],
          ['Rows applied', a ? a.applied.toLocaleString() : '-'],
          ['Filtered / unmapped', a ? a.skipped.toLocaleString() : '-'],
          ['Duplicates suppressed', a ? a.duplicatesSuppressed.toLocaleString() : '-'],
          ['Dead-lettered', a ? a.deadLettered.toLocaleString() : '-'],
          ['Last error', a?.lastError ? <span className="err">{a.lastError}</span> : 'none'],
        ],
      };
    }
  }
}

function MonitorPanel(props: { apply: ApplyStatus[]; onSelect: (name: string) => void }) {
  return (
    <section className="monitor">
      <div className="monitor-title">Replication Monitor <span className="muted">(apply-service, refreshes every 5 s)</span></div>
      <div className="table-box short">
        <table className="grid">
          <thead>
            <tr>
              <th>Subscription</th><th>Publication</th><th>Target</th><th>State</th>
              <th className="num">Lag</th><th className="num">Applied</th><th className="num">DLQ</th><th>Last error</th>
            </tr>
          </thead>
          <tbody>
            {props.apply.length === 0 && <tr><td colSpan={8} className="muted">No active subscriptions</td></tr>}
            {props.apply.map((a) => (
              <tr key={a.subscription} onClick={() => props.onSelect(a.subscription)} className="clickable">
                <td>{a.subscription}</td>
                <td>{a.publication}</td>
                <td>{a.target} <span className="muted">({dbLabel(a.targetType)})</span></td>
                <td><StatusDot status={a.state} />{a.state}</td>
                <td className="num">{a.totalLag.toLocaleString()}</td>
                <td className="num">{a.applied.toLocaleString()}</td>
                <td className={`num ${a.deadLettered > 0 ? 'err' : ''}`}>{a.deadLettered}</td>
                <td className="err ellipsis">{a.lastError ?? ''}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
