import { and, desc, eq, sql } from 'drizzle-orm';
import { db, hasProxyLogDownstreamApiKeyIdColumn, schema } from '../db/index.js';
import {
  getManagedDownstreamApiKeyByToken,
  type DownstreamApiKeyPolicyView,
} from './downstreamApiKeyService.js';
import {
  resolveDownstreamTrendRangeSinceUtc,
  type DownstreamKeyTrendRange,
} from './downstreamApiKeyTrendService.js';

export type PublicDownstreamApiKeyView = Omit<DownstreamApiKeyPolicyView, 'key'>;

export type DownstreamUsageAggregate = {
  totalRequests: number;
  successRequests: number;
  failedRequests: number;
  successRate: number | null;
  promptTokens: number;
  completionTokens: number;
  totalTokens: number;
  totalCost: number;
  avgLatencyMs: number | null;
  avgFirstByteLatencyMs: number | null;
  firstSeenAt: string | null;
  lastSeenAt: string | null;
};

export type DownstreamModelUsageAggregate = {
  model: string;
  totalRequests: number;
  successRequests: number;
  failedRequests: number;
  successRate: number | null;
  promptTokens: number;
  completionTokens: number;
  totalTokens: number;
  totalCost: number;
  avgLatencyMs: number | null;
  avgFirstByteLatencyMs: number | null;
  firstSeenAt: string | null;
  lastSeenAt: string | null;
};

export type DownstreamRecentUsageRequest = {
  id: number;
  createdAt: string | null;
  status: string | null;
  httpStatus: number | null;
  isStream: boolean | null;
  retryCount: number;
  modelRequested: string | null;
  modelActual: string | null;
  promptTokens: number | null;
  completionTokens: number | null;
  totalTokens: number | null;
  estimatedCost: number;
  latencyMs: number | null;
  firstByteLatencyMs: number | null;
  errorMessage: string | null;
};

export type PublicDownstreamApiKeyUsageLookup = {
  item: PublicDownstreamApiKeyView;
  usage: {
    last24h: DownstreamUsageAggregate | null;
    last7d: DownstreamUsageAggregate | null;
    all: DownstreamUsageAggregate | null;
  };
  modelUsage: DownstreamModelUsageAggregate[];
  recentRequests: DownstreamRecentUsageRequest[];
};

function roundMicro(value: unknown): number {
  const numeric = Number(value || 0);
  if (!Number.isFinite(numeric)) return 0;
  return Math.round(numeric * 1_000_000) / 1_000_000;
}

function toNullableNumber(value: unknown): number | null {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return null;
  return numeric;
}

function toAggregate(row: Record<string, unknown> | undefined): DownstreamUsageAggregate {
  const totalRequests = Number(row?.totalRequests || 0);
  const successRequests = Number(row?.successRequests || 0);
  const avgLatencyMs = toNullableNumber(row?.avgLatencyMs);
  const avgFirstByteLatencyMs = toNullableNumber(row?.avgFirstByteLatencyMs);

  return {
    totalRequests,
    successRequests,
    failedRequests: Number(row?.failedRequests || 0),
    successRate: totalRequests > 0 ? Math.round((successRequests / totalRequests) * 1000) / 10 : null,
    promptTokens: Number(row?.promptTokens || 0),
    completionTokens: Number(row?.completionTokens || 0),
    totalTokens: Number(row?.totalTokens || 0),
    totalCost: roundMicro(row?.totalCost),
    avgLatencyMs: avgLatencyMs === null ? null : Math.round(avgLatencyMs * 100) / 100,
    avgFirstByteLatencyMs: avgFirstByteLatencyMs === null ? null : Math.round(avgFirstByteLatencyMs * 100) / 100,
    firstSeenAt: typeof row?.firstSeenAt === 'string' ? row.firstSeenAt : null,
    lastSeenAt: typeof row?.lastSeenAt === 'string' ? row.lastSeenAt : null,
  };
}

function toPublicKeyView(item: DownstreamApiKeyPolicyView): PublicDownstreamApiKeyView {
  const { key: _key, ...rest } = item;
  return rest;
}

async function readUsageAggregate(
  keyId: number,
  range: DownstreamKeyTrendRange,
): Promise<DownstreamUsageAggregate> {
  const sinceUtc = resolveDownstreamTrendRangeSinceUtc(range);
  const row = await db.select({
    totalRequests: sql<number>`count(*)`,
    successRequests: sql<number>`coalesce(sum(case when ${schema.proxyLogs.status} = 'success' then 1 else 0 end), 0)`,
    failedRequests: sql<number>`coalesce(sum(case when ${schema.proxyLogs.status} = 'success' then 0 else 1 end), 0)`,
    promptTokens: sql<number>`coalesce(sum(coalesce(${schema.proxyLogs.promptTokens}, 0)), 0)`,
    completionTokens: sql<number>`coalesce(sum(coalesce(${schema.proxyLogs.completionTokens}, 0)), 0)`,
    totalTokens: sql<number>`coalesce(sum(coalesce(${schema.proxyLogs.totalTokens}, 0)), 0)`,
    totalCost: sql<number>`coalesce(sum(coalesce(${schema.proxyLogs.estimatedCost}, 0)), 0)`,
    avgLatencyMs: sql<number>`avg(${schema.proxyLogs.latencyMs})`,
    avgFirstByteLatencyMs: sql<number>`avg(${schema.proxyLogs.firstByteLatencyMs})`,
    firstSeenAt: sql<string | null>`min(${schema.proxyLogs.createdAt})`,
    lastSeenAt: sql<string | null>`max(${schema.proxyLogs.createdAt})`,
  })
    .from(schema.proxyLogs)
    .where(and(
      eq(schema.proxyLogs.downstreamApiKeyId, keyId),
      ...(sinceUtc ? [sql`${schema.proxyLogs.createdAt} >= ${sinceUtc}`] : []),
    ))
    .get();

  return toAggregate((row || {}) as Record<string, unknown>);
}

async function readModelUsage(keyId: number): Promise<DownstreamModelUsageAggregate[]> {
  const modelExpression = sql<string>`coalesce(nullif(trim(${schema.proxyLogs.modelActual}), ''), nullif(trim(${schema.proxyLogs.modelRequested}), ''), 'unknown')`;

  const rows = await db.select({
    model: modelExpression,
    totalRequests: sql<number>`count(*)`,
    successRequests: sql<number>`coalesce(sum(case when ${schema.proxyLogs.status} = 'success' then 1 else 0 end), 0)`,
    failedRequests: sql<number>`coalesce(sum(case when ${schema.proxyLogs.status} = 'success' then 0 else 1 end), 0)`,
    promptTokens: sql<number>`coalesce(sum(coalesce(${schema.proxyLogs.promptTokens}, 0)), 0)`,
    completionTokens: sql<number>`coalesce(sum(coalesce(${schema.proxyLogs.completionTokens}, 0)), 0)`,
    totalTokens: sql<number>`coalesce(sum(coalesce(${schema.proxyLogs.totalTokens}, 0)), 0)`,
    totalCost: sql<number>`coalesce(sum(coalesce(${schema.proxyLogs.estimatedCost}, 0)), 0)`,
    avgLatencyMs: sql<number>`avg(${schema.proxyLogs.latencyMs})`,
    avgFirstByteLatencyMs: sql<number>`avg(${schema.proxyLogs.firstByteLatencyMs})`,
    firstSeenAt: sql<string | null>`min(${schema.proxyLogs.createdAt})`,
    lastSeenAt: sql<string | null>`max(${schema.proxyLogs.createdAt})`,
  })
    .from(schema.proxyLogs)
    .where(eq(schema.proxyLogs.downstreamApiKeyId, keyId))
    .groupBy(modelExpression)
    .all();

  return rows
    .map((row) => ({
      model: String((row as any).model || 'unknown'),
      ...toAggregate(row as Record<string, unknown>),
    }))
    .sort((left, right) => {
      if (right.totalRequests !== left.totalRequests) {
        return right.totalRequests - left.totalRequests;
      }
      if (right.totalTokens !== left.totalTokens) {
        return right.totalTokens - left.totalTokens;
      }
      return left.model.localeCompare(right.model);
    });
}

async function readRecentRequests(keyId: number, limit = 20): Promise<DownstreamRecentUsageRequest[]> {
  const rows = await db.select({
    id: schema.proxyLogs.id,
    createdAt: schema.proxyLogs.createdAt,
    status: schema.proxyLogs.status,
    httpStatus: schema.proxyLogs.httpStatus,
    isStream: schema.proxyLogs.isStream,
    retryCount: schema.proxyLogs.retryCount,
    modelRequested: schema.proxyLogs.modelRequested,
    modelActual: schema.proxyLogs.modelActual,
    promptTokens: schema.proxyLogs.promptTokens,
    completionTokens: schema.proxyLogs.completionTokens,
    totalTokens: schema.proxyLogs.totalTokens,
    estimatedCost: schema.proxyLogs.estimatedCost,
    latencyMs: schema.proxyLogs.latencyMs,
    firstByteLatencyMs: schema.proxyLogs.firstByteLatencyMs,
    errorMessage: schema.proxyLogs.errorMessage,
  })
    .from(schema.proxyLogs)
    .where(eq(schema.proxyLogs.downstreamApiKeyId, keyId))
    .orderBy(desc(schema.proxyLogs.createdAt), desc(schema.proxyLogs.id))
    .limit(limit)
    .all();

  return rows.map((row) => ({
    id: Number(row.id || 0),
    createdAt: row.createdAt || null,
    status: row.status || null,
    httpStatus: row.httpStatus ?? null,
    isStream: typeof row.isStream === 'boolean' ? row.isStream : row.isStream == null ? null : Boolean(row.isStream),
    retryCount: Number(row.retryCount || 0),
    modelRequested: row.modelRequested || null,
    modelActual: row.modelActual || null,
    promptTokens: row.promptTokens ?? null,
    completionTokens: row.completionTokens ?? null,
    totalTokens: row.totalTokens ?? null,
    estimatedCost: roundMicro(row.estimatedCost),
    latencyMs: row.latencyMs ?? null,
    firstByteLatencyMs: row.firstByteLatencyMs ?? null,
    errorMessage: row.errorMessage || null,
  }));
}

export async function lookupPublicDownstreamApiKeyUsageByKey(key: string): Promise<PublicDownstreamApiKeyUsageLookup | null> {
  const item = await getManagedDownstreamApiKeyByToken(key);
  if (!item) {
    return null;
  }

  const columnReady = await hasProxyLogDownstreamApiKeyIdColumn();
  if (!columnReady) {
    return {
      item: toPublicKeyView(item),
      usage: { last24h: null, last7d: null, all: null },
      modelUsage: [],
      recentRequests: [],
    };
  }

  const [last24h, last7d, all, modelUsage, recentRequests] = await Promise.all([
    readUsageAggregate(item.id, '24h'),
    readUsageAggregate(item.id, '7d'),
    readUsageAggregate(item.id, 'all'),
    readModelUsage(item.id),
    readRecentRequests(item.id),
  ]);

  return {
    item: toPublicKeyView(item),
    usage: {
      last24h,
      last7d,
      all,
    },
    modelUsage,
    recentRequests,
  };
}
