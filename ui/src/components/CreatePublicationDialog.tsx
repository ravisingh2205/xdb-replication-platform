import { useEffect, useMemo, useState } from 'react';
import { api } from '../api/client';
import type { Database, FilterOperator, RowFilter, TopicRouting } from '../api/types';
import { GroupBox, Tabs, Window } from './Window';

/**
 * "Create Publication", laid out like the legacy dialog: a table checklist loaded live from the
 * source DB, Select All, wildcard selection, and Table Filters / Conflict Resolution tabs.
 * An extra "Kafka Topics" tab exposes partitions, topic routing and snapshot mode.
 */
export function CreatePublicationDialog(props: { source: Database; onClose: () => void; onSaved: () => void }) {
  const [tab, setTab] = useState('tables');
  const [name, setName] = useState('');
  const [available, setAvailable] = useState<string[]>([]);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [filters, setFilters] = useState<RowFilter[]>([]);
  const [partitions, setPartitions] = useState(3);
  const [routing, setRouting] = useState<TopicRouting>('PER_TABLE');
  const [snapshotMode, setSnapshotMode] = useState('initial');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    api.tables('source', props.source.name)
      .then((t) => setAvailable(t))
      .catch((e: Error) => setError(e.message))
      .finally(() => setLoading(false));
  }, [props.source.name]);

  // Wide publications: suggest collapsing topics (see README "Scaling to hundreds of tables").
  useEffect(() => {
    if (selected.size > 200 && routing === 'PER_TABLE') {
      setRouting('PER_SCHEMA');
    }
  }, [selected.size, routing]);

  const allSelected = available.length > 0 && selected.size === available.length;
  const toggle = (t: string) => {
    const next = new Set(selected);
    if (next.has(t)) {
      next.delete(t);
    } else {
      next.add(t);
    }
    setSelected(next);
  };

  const wildcard = () => {
    const pattern = window.prompt('Select tables matching (use * as wildcard), e.g. public.order*', 'public.*');
    if (!pattern) return;
    const re = new RegExp('^' + pattern.split('*').map((p) => p.replace(/[.+?^${}()|[\]\\]/g, '\\$&')).join('.*') + '$', 'i');
    setSelected(new Set([...selected, ...available.filter((t) => re.test(t))]));
  };

  const selectedList = useMemo(() => available.filter((t) => selected.has(t)), [available, selected]);
  const topicCount = routing === 'PER_TABLE' ? selected.size
    : routing === 'PER_SCHEMA' ? new Set(selectedList.map((t) => t.split('.')[0])).size : 1;

  const onCreate = async () => {
    setBusy(true);
    setError(null);
    try {
      await api.createPublication({
        name: name.trim(),
        sourceDatabase: props.source.name,
        tables: selectedList,
        rowFilters: filters.filter((f) => f.column && f.values.length > 0),
        partitions,
        snapshotMode,
        topicRouting: routing,
      });
      props.onSaved();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <Window
      title="Create Publication"
      width={860}
      onClose={props.onClose}
      footer={
        <>
          <span className="footer-status">{error && <span className="err">✖ {error}</span>}</span>
          <button className="btn primary" disabled={busy || !name || selected.size === 0} onClick={onCreate}>Create</button>
          <button className="btn" onClick={props.onClose}>Cancel</button>
        </>
      }
    >
      <Tabs
        active={tab}
        onChange={setTab}
        tabs={[
          { id: 'tables', label: 'Create Publication' },
          { id: 'filters', label: 'Table Filters' },
          { id: 'conflict', label: 'Conflict Resolution Options', disabled: true,
            title: 'Single-master replication: conflict resolution applies to multi-master (planned)' },
          { id: 'kafka', label: 'Kafka Topics' },
        ]}
      />
      <div className="tab-panel tall">
        {tab === 'tables' && (
          <>
            <div className="form-row">
              <label htmlFor="pubname">Publication Name</label>
              <input id="pubname" value={name} onChange={(e) => setName(e.target.value)} placeholder="demo_pub" />
              <span className="muted small">Source: {props.source.name} ({props.source.host})</span>
            </div>
            <div className="table-box">
              <table className="grid">
                <thead>
                  <tr><th className="col-check">Publish</th><th>Available Tables</th></tr>
                </thead>
                <tbody>
                  {loading && <tr><td colSpan={2} className="muted">Reading tables from {props.source.name}…</td></tr>}
                  {!loading && available.length === 0 && <tr><td colSpan={2} className="muted">No user tables found</td></tr>}
                  {available.map((t) => (
                    <tr key={t} className={selected.has(t) ? 'selected' : ''} onClick={() => toggle(t)}>
                      <td className="col-check">
                        <input type="checkbox" checked={selected.has(t)} onChange={() => toggle(t)} onClick={(e) => e.stopPropagation()} />
                      </td>
                      <td>{t}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <div className="below-table">
              <label>
                <input type="checkbox" checked={allSelected}
                       onChange={() => setSelected(allSelected ? new Set() : new Set(available))} /> Select All
              </label>
              <button className="btn" onClick={wildcard}>Use Wildcard Selection...</button>
              <span className="muted">{selected.size} of {available.length} selected</span>
            </div>
          </>
        )}

        {tab === 'filters' && (
          <FiltersEditor tables={selectedList} filters={filters} onChange={setFilters} />
        )}

        {tab === 'kafka' && (
          <GroupBox legend="Topic layout and snapshot">
            <div className="grid-2">
              <label>Topic Routing</label>
              <select className="span-3" value={routing} onChange={(e) => setRouting(e.target.value as TopicRouting)}>
                <option value="PER_TABLE">One topic per table (cdc.{name || '<pub>'}.schema.table)</option>
                <option value="PER_SCHEMA">One topic per schema (cdc.{name || '<pub>'}.schema)</option>
                <option value="SINGLE">One topic for the publication (cdc.{name || '<pub>'}.all)</option>
              </select>
              <label>Partitions / topic</label>
              <input type="number" min={1} max={64} value={partitions} className="narrow"
                     onChange={(e) => setPartitions(Number(e.target.value))} />
              <label className="right">Initial Snapshot</label>
              <select value={snapshotMode} onChange={(e) => setSnapshotMode(e.target.value)}>
                <option value="initial">Perform initial snapshot</option>
                <option value="no_data">Stream changes only</option>
                <option value="when_needed">When needed</option>
              </select>
            </div>
            <p className="muted small">
              Result: {topicCount} topic(s) × {partitions} partition(s) = <b>{topicCount * partitions}</b> partitions.
              Rows keep per-key ordering in every mode (key = primary key).
            </p>
          </GroupBox>
        )}
      </div>
    </Window>
  );
}

function FiltersEditor(props: { tables: string[]; filters: RowFilter[]; onChange: (f: RowFilter[]) => void }) {
  const update = (i: number, patch: Partial<RowFilter>) =>
    props.onChange(props.filters.map((f, j) => (j === i ? { ...f, ...patch } : f)));
  return (
    <>
      <p className="muted small">
        Only rows matching every filter of a table are replicated. A row updated out of the filter is deleted on the target.
      </p>
      <div className="table-box">
        <table className="grid">
          <thead>
            <tr><th>Table</th><th>Column</th><th>Operator</th><th>Values (comma separated)</th><th /></tr>
          </thead>
          <tbody>
            {props.filters.length === 0 && <tr><td colSpan={5} className="muted">No filters: all rows are published</td></tr>}
            {props.filters.map((f, i) => (
              <tr key={i}>
                <td>
                  <select value={f.table} onChange={(e) => update(i, { table: e.target.value })}>
                    {props.tables.map((t) => <option key={t}>{t}</option>)}
                  </select>
                </td>
                <td><input value={f.column} onChange={(e) => update(i, { column: e.target.value })} placeholder="region" /></td>
                <td>
                  <select value={f.operator} onChange={(e) => update(i, { operator: e.target.value as FilterOperator })}>
                    <option value="EQ">=</option>
                    <option value="NE">&lt;&gt;</option>
                    <option value="IN">IN</option>
                    <option value="NOT_IN">NOT IN</option>
                  </select>
                </td>
                <td>
                  <input value={f.values.join(',')} placeholder="APAC,EMEA"
                         onChange={(e) => update(i, { values: e.target.value.split(',').map((v) => v.trim()).filter(Boolean) })} />
                </td>
                <td><button className="btn small" onClick={() => props.onChange(props.filters.filter((_, j) => j !== i))}>Remove</button></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="below-table">
        <button className="btn" disabled={props.tables.length === 0}
                onClick={() => props.onChange([...props.filters, { table: props.tables[0], column: '', operator: 'IN', values: [] }])}>
          Add Filter
        </button>
        {props.tables.length === 0 && <span className="muted">Select tables on the first tab</span>}
      </div>
    </>
  );
}
