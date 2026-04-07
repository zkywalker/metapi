import type { FastifyInstance } from 'fastify';

const DESKTOP_HEALTH_ROUTE = '/api/desktop/health';
const PUBLIC_DOWNSTREAM_KEY_USAGE_ROUTE = '/api/public/downstream-key-usage';

export function isPublicApiRoute(url: string): boolean {
  const [pathname] = String(url || '').split('?', 1);
  return pathname === DESKTOP_HEALTH_ROUTE
    || pathname === PUBLIC_DOWNSTREAM_KEY_USAGE_ROUTE
    || pathname.startsWith('/api/oauth/callback/');
}

export async function registerDesktopRoutes(app: FastifyInstance) {
  app.get(DESKTOP_HEALTH_ROUTE, async () => ({ ok: true }));
}
