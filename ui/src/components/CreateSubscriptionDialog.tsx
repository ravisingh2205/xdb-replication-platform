import { useMemo, useState } from 'react';
import { api } from '../api/client';
import { dbLabel, type Database, type Publication, type TableMapping } from '../api/types';
import { GroupBox, Tabs, Window } from './Window';

const identity = (t: string): TableMapping => {
  const [schema, table] = t.split('.');
  return { sourceSchema: schema, sourceTable: table, targetSchema: schema, targetTable: table };
};

/** "Create Subscription": which publication, which target database, which table mappings. */
export function CreateSubscriptionDialog(props: {
  target: Database;
  publications: Publication[];
  onClose: () => void;
  onSaved: () => void;
}) {
  const [tab, setTab] = useState('sub');
  const [name, setName] = useState('');
  const [publication, setPublication] = useState(props.publications[0]?.name ?? '');
  const [overrides, setOverrides] = useState<Record<string, TableMapping>>({});
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const pub = props.publications.find((p) => p.name === publication);
  const oracleTarget = props.target.dbType === 'ORACLE';
  const mappings = useMemo(
    () => (pub?.tables ?? []).map((t) => {
      const base = identity(t);
      // Oracle stores unquoted identifiers in upper case; propose that by default.
      const proposed = oracleTarget
        ? { ...base, targetSchema: base.targetSchema.toUpperCase(), targetTable: base.targetTable.toUpperCase() }
        : base;
      return overrides[t] ?? proposed;
    }),
    [pub, overrides, oracleTarget],
  );

  const edit = (t: string, patch: Partial<TableMapping>) =>
    setOverrides({ ...overrides, [t]: { ...mappings.find((m) => `${m.sourceSchema}.${m.sourceTable}` === t)!, ...patch } });

  const onCreate = async () => {
    setBusy(true);
    setError(null);
    try {
      await api.createSubscription({ name: name.trim(), publication, targetDatabase: props.target.name, tableMappings: mappings });
      props.onSaved();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <Window
      title="Subscription Service - Create Subscription"
      width={820}
      onClose={props.onClose}
      footer={
        <>
          <span className="footer-status">{error && <span className="err">✖ {error}</span>}</span>
          <button className="btn primary" disabled={busy || !name || !publication} onClick={onCreate}>Create</button>
          <button className="btn" onClick={props.onClose}>Cancel</button>
        </>
      }
    >
      <Tabs active={tab} onChange={setTab}
            tabs={[{ id: 'sub', label: 'Create Subscription' }, { id: 'map', label: 'Table Mappings' }]} />
      <div className="tab-panel tall">
        {tab === 'sub' && (
          <GroupBox legend="Subscription">
            <div className="grid-2">
              <label>Subscription Name</label>
              <input className="span-3" value={name} onChange={(e) => setName(e.target.value)} placeholder="demo_sub" />
              <label>Publication</label>
              <select className="span-3" value={publication} onChange={(e) => { setPublication(e.target.value); setOverrides({}); }}>
                {props.publications.length === 0 && <option value="">No publications yet</option>}
                {props.publications.map((p) => (
                  <option key={p.name} value={p.name}>{p.name}  ({p.sourceDatabase}, {p.tables.length} tables, {p.status})</option>
                ))}
              </select>
              <label>Target Database</label>
              <input className="span-3" readOnly
                     value={`${props.target.name} - ${dbLabel(props.target.dbType)} ${props.target.host}:${props.target.port}/${props.target.databaseName}`} />
            </div>
            <p className="muted small">
              The apply service starts a dedicated consumer group <code>xdb-apply-{name || '<subscription>'}</code> for
              this subscription. Target tables must already exist (create them with your schema migration tool).
            </p>
          </GroupBox>
        )}
        {tab === 'map' && (
          <div className="table-box">
            <table className="grid">
              <thead>
                <tr><th>Source table</th><th>→ Target schema</th><th>Target table</th></tr>
              </thead>
              <tbody>
                {mappings.map((m) => {
                  const key = `${m.sourceSchema}.${m.sourceTable}`;
                  return (
                    <tr key={key}>
                      <td>{key}</td>
                      <td><input value={m.targetSchema} onChange={(e) => edit(key, { targetSchema: e.target.value })} /></td>
                      <td><input value={m.targetTable} onChange={(e) => edit(key, { targetTable: e.target.value })} /></td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </Window>
  );
}
