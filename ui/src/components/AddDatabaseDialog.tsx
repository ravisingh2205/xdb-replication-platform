import { useState, type ChangeEvent } from 'react';
import { api } from '../api/client';
import { DB_TYPES, type DatabaseRequest, type DbType, type TestResult } from '../api/types';
import { DbIcon } from './Icons';
import { GroupBox, Tabs, Window } from './Window';

/**
 * "Publication Service - Add Database" / "Subscription Service - Add Database".
 * Same layout as the legacy dialog; "Test" performs a real JDBC check through the backend.
 */
export function AddDatabaseDialog(props: { side: 'source' | 'target'; onClose: () => void; onSaved: () => void }) {
  const [dbType, setDbType] = useState<DbType>('POSTGRES');
  const [form, setForm] = useState({
    name: '', host: '', port: '5432', username: '', password: '', databaseName: '', pdbName: '', urlOptions: '',
  });
  const [test, setTest] = useState<TestResult | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const typeInfo = DB_TYPES.find((d) => d.value === dbType)!;
  const service = props.side === 'source' ? 'Publication Service' : 'Subscription Service';
  const set = (k: keyof typeof form) => (e: ChangeEvent<HTMLInputElement>) => {
    setForm({ ...form, [k]: e.target.value });
    setTest(null);
  };

  const request = (): DatabaseRequest => ({
    name: form.name.trim(),
    dbType,
    host: form.host.trim(),
    port: form.port ? Number(form.port) : undefined,
    databaseName: form.databaseName.trim(),
    pdbName: dbType === 'ORACLE' && form.pdbName ? form.pdbName.trim() : undefined,
    username: form.username.trim(),
    password: form.password,
    urlOptions: form.urlOptions.trim() || undefined,
  });

  const run = async (fn: () => Promise<void>) => {
    setBusy(true);
    setError(null);
    try {
      await fn();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const onTest = () => run(async () => {
    const { name: _ignored, ...rest } = request();
    setTest(await api.testDatabase(props.side, rest));
  });

  const onSave = () => run(async () => {
    await api.addDatabase(props.side, request());
    props.onSaved();
  });

  return (
    <Window
      title={`${service} - Add Database`}
      width={720}
      onClose={props.onClose}
      footer={
        <>
          <span className="footer-status">
            {test && <span className={test.ok ? 'ok' : 'err'}>{test.ok ? `✔ ${test.product} ${test.version}` : `✖ ${test.message}`}</span>}
            {error && <span className="err">✖ {error}</span>}
          </span>
          <button className="btn" onClick={onTest} disabled={busy}>Test</button>
          <button className="btn primary" onClick={onSave} disabled={busy || !form.name}>Save</button>
          <button className="btn" onClick={props.onClose}>Cancel</button>
        </>
      }
    >
      <p className="hint-green">
        {props.side === 'source'
          ? 'This database will be added as a Publication (source) node.'
          : 'This database will be added as a Subscription (target) node.'}
      </p>
      <div className="form-row">
        <label htmlFor="dbtype">Database Type</label>
        <select
          id="dbtype"
          value={dbType}
          onChange={(e) => {
            const t = e.target.value as DbType;
            setDbType(t);
            setForm({ ...form, port: String(DB_TYPES.find((d) => d.value === t)!.port) });
            setTest(null);
          }}
        >
          {DB_TYPES.map((d) => <option key={d.value} value={d.value}>{d.label}</option>)}
        </select>
      </div>

      <Tabs tabs={[{ id: 'db', label: 'Database' }]} active="db" onChange={() => undefined} />
      <div className="tab-panel">
        <GroupBox legend={`${typeInfo.label} Database Information`}>
          <p className="intro"><DbIcon type={dbType} /> Please specify {typeInfo.label} database information.</p>
          <div className="grid-2">
            <label>Name</label>
            <input value={form.name} onChange={set('name')} placeholder="e.g. orders_src" />
            <span className="grid-note">Unique node name: lowercase, digits, underscore</span>

            <label>Host</label>
            <input value={form.host} onChange={set('host')} placeholder="172.24.36.210" />
            <label className="right">Port</label>
            <input value={form.port} onChange={set('port')} className="narrow" />

            <label>User</label>
            <input value={form.username} onChange={set('username')} />
            <label className="right">Password</label>
            <input type="password" value={form.password} onChange={set('password')} className="narrow" />

            <label>{dbType === 'ORACLE' ? 'Service / CDB' : 'Database'}</label>
            <input value={form.databaseName} onChange={set('databaseName')} />
            {dbType === 'ORACLE' ? (
              <>
                <label className="right">PDB</label>
                <input value={form.pdbName} onChange={set('pdbName')} className="narrow" placeholder="ORCLPDB1" />
              </>
            ) : (<><span /><span /></>)}

            <label>URL Options</label>
            <input className="span-3" value={form.urlOptions} onChange={set('urlOptions')}
                   placeholder={dbType === 'SQLSERVER' ? 'applicationIntent=ReadOnly' : 'sslmode=require'} />

            <label>Changeset Logging</label>
            <div className="span-3 radios">
              <label title="Not available: the modernized platform captures changes from the database log only">
                <input type="radio" disabled /> Table Triggers
              </label>
              <label>
                <input type="radio" checked readOnly /> {typeInfo.cdc}
              </label>
            </div>
          </div>
        </GroupBox>
      </div>

      <GroupBox legend="" className="bottom-box">
        <div className="bottom-row">
          <span>CDC via Debezium on Kafka Connect</span>
          <label className="muted"><input type="checkbox" disabled /> Replicate Publication Schema</label>
          <span className="muted">Initial snapshot is chosen per publication</span>
        </div>
      </GroupBox>
    </Window>
  );
}
