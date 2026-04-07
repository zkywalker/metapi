import Fastify from 'fastify';
import { describe, expect, it } from 'vitest';
import { isPublicApiRoute, registerDesktopRoutes } from './desktop.js';

describe('desktop server routes', () => {
  it('marks desktop health and public downstream key usage routes as public', () => {
    expect(isPublicApiRoute('/api/desktop/health')).toBe(true);
    expect(isPublicApiRoute('/api/public/downstream-key-usage')).toBe(true);
    expect(isPublicApiRoute('/api/public/downstream-key-usage?key=sk-test-001')).toBe(true);
    expect(isPublicApiRoute('/api/stats/dashboard')).toBe(false);
  });

  it('registers a public desktop health probe', async () => {
    const app = Fastify();
    await registerDesktopRoutes(app);

    const response = await app.inject({
      method: 'GET',
      url: '/api/desktop/health',
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({ ok: true });
    await app.close();
  });
});
