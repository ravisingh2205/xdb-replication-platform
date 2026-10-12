import type {
  ApplyStatus, ConnectorStatus, Database, DatabaseRequest, Publication, PublicationRequest,
  Subscription, SubscriptionRequest, TableMapping, TestResult,
} from './types';

/** All calls go through the API Gateway at /api (proxied by Vite in dev, nginx in the container). */
async function http<T>(method: string, path: string, body?: unknown): Promise<T> {
  const res = await fetch(`/api${path}`, {
    method,
    headers: body !== undefined ? { 'Content-Type': 'application/json' } : undefined,
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) {
    let detail = `${res.status} ${res.statusText}`;
    try {
      const problem = await res.json();
      detail = problem.detail ?? problem.message ?? detail;
    } catch {
      /* not JSON */
    }
    throw new Error(detail);
  }
  return res.status === 204 ? (undefined as T) : ((await res.json()) as T);
}

type Side = 'source' | 'target';
const dbPath = (side: Side) => (side === 'source' ? '/source-databases' : '/target-databases');

export const api = {
  databases: (side: Side) => http<Database[]>('GET', dbPath(side)),
  addDatabase: (side: Side, req: DatabaseRequest) => http<Database>('POST', dbPath(side), req),
  testDatabase: (side: Side, req: Omit<DatabaseRequest, 'name'>) => http<TestResult>('POST', `${dbPath(side)}/test`, req),
  tables: (side: Side, name: string) => http<string[]>('GET', `${dbPath(side)}/${name}/tables`),
  removeDatabase: (side: Side, name: string) => http<void>('DELETE', `${dbPath(side)}/${name}`),

  publications: () => http<Publication[]>('GET', '/publications'),
  createPublication: (req: PublicationRequest) => http<Publication>('POST', '/publications', req),
  connector: (name: string) => http<ConnectorStatus>('GET', `/publications/${name}/connector`),
  connectorConfig: (name: string) => http<Record<string, string>>('GET', `/publications/${name}/connector-config`),
  publicationAction: (name: string, action: 'pause' | 'resume' | 'restart') =>
    http<Publication>('POST', `/publications/${name}/${action}`),
  removePublication: (name: string) => http<void>('DELETE', `/publications/${name}`),

  subscriptions: () => http<Subscription[]>('GET', '/subscriptions'),
  createSubscription: (req: SubscriptionRequest) => http<Subscription>('POST', '/subscriptions', req),
  updateMappings: (name: string, tableMappings: TableMapping[]) =>
    http<Subscription>('PUT', `/subscriptions/${name}`, { tableMappings }),
  subscriptionAction: (name: string, action: 'pause' | 'resume') =>
    http<Subscription>('POST', `/subscriptions/${name}/${action}`),
  removeSubscription: (name: string) => http<void>('DELETE', `/subscriptions/${name}`),

  applyStatus: () => http<ApplyStatus[]>('GET', '/apply/subscriptions'),
};
