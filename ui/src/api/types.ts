export type DbType = 'POSTGRES' | 'EPAS' | 'ORACLE' | 'SQLSERVER';
export type TopicRouting = 'PER_TABLE' | 'PER_SCHEMA' | 'SINGLE';
export type FilterOperator = 'EQ' | 'NE' | 'IN' | 'NOT_IN';

export const DB_TYPES: { value: DbType; label: string; port: number; cdc: string }[] = [
  { value: 'POSTGRES', label: 'PostgreSQL', port: 5432, cdc: 'WAL Stream (logical decoding, pgoutput)' },
  { value: 'EPAS', label: 'EDB Postgres Advanced Server', port: 5444, cdc: 'WAL Stream (logical decoding, pgoutput)' },
  { value: 'ORACLE', label: 'Oracle', port: 1521, cdc: 'Redo / Archive Log (LogMiner)' },
  { value: 'SQLSERVER', label: 'SQL Server', port: 1433, cdc: 'SQL Server CDC (transaction log)' },
];

export const dbLabel = (t: DbType) => DB_TYPES.find((d) => d.value === t)?.label ?? t;

export interface DatabaseRequest {
  name: string;
  dbType: DbType;
  host: string;
  port?: number;
  databaseName: string;
  pdbName?: string;
  username: string;
  password: string;
  urlOptions?: string;
}

export interface Database {
  id: number;
  name: string;
  dbType: DbType;
  host: string;
  port: number;
  databaseName: string;
  pdbName?: string;
  username: string;
  urlOptions?: string;
  createdAt: string;
}

export interface TestResult {
  ok: boolean;
  message: string;
  product?: string;
  version?: string;
}

export interface RowFilter {
  table: string;
  column: string;
  operator: FilterOperator;
  values: string[];
}

export interface PublicationRequest {
  name: string;
  sourceDatabase: string;
  tables: string[];
  rowFilters: RowFilter[];
  partitions?: number;
  snapshotMode?: string;
  topicRouting?: TopicRouting;
}

export interface Publication {
  id: number;
  name: string;
  sourceDatabase: string;
  sourceDbType: DbType;
  tables: string[];
  rowFilters: RowFilter[];
  partitions: number;
  snapshotMode: string;
  topicRouting: TopicRouting;
  topicPrefix: string;
  connectorName: string;
  status: 'PROVISIONING' | 'RUNNING' | 'PAUSED' | 'FAILED';
  statusMessage?: string;
  createdAt: string;
  updatedAt: string;
}

export interface ConnectorStatus {
  name: string;
  connector: { state: string; worker_id: string; trace?: string };
  tasks: { id: number; state: string; worker_id: string; trace?: string }[];
}

export interface TableMapping {
  sourceSchema: string;
  sourceTable: string;
  targetSchema: string;
  targetTable: string;
}

export interface SubscriptionRequest {
  name: string;
  publication: string;
  targetDatabase: string;
  tableMappings?: TableMapping[];
}

export interface Subscription {
  id: number;
  name: string;
  publication: string;
  targetDatabase: string;
  targetDbType: DbType;
  tableMappings: TableMapping[];
  active: boolean;
  consumerGroup: string;
  createdAt: string;
  updatedAt: string;
}

export interface PartitionLag {
  topic: string;
  partition: number;
  committed: number;
  end: number;
  lag: number;
}

export interface ApplyStatus {
  subscription: string;
  publication: string;
  target: string;
  targetType: DbType;
  state: string;
  consumerGroup: string;
  assignedToThisPod: string[];
  totalLag: number;
  partitions: PartitionLag[];
  applied: number;
  skipped: number;
  duplicatesSuppressed: number;
  deadLettered: number;
  lastAppliedAt?: string;
  replicationDelayMs?: number;
  lastError?: string;
  lastErrorAt?: string;
}
