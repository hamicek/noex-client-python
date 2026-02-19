import { Store } from '@hamicek/noex-store';
import { NoexServer } from '@hamicek/noex-server';
import { RuleEngine } from '@hamicek/noex-rules';

const config = JSON.parse(process.argv[2] || '{}');

const store = await Store.start({ name: `py-test-${Date.now()}` });

if (config.buckets) {
  for (const b of config.buckets) {
    await store.defineBucket(b.name, {
      key: 'id',
      schema: {
        id: { type: 'string', generated: 'uuid' },
        ...b.schema,
      },
    });
  }
}

if (config.queries) {
  for (const q of config.queries) {
    switch (q.type) {
      case 'all':
        store.defineQuery(q.name, async (ctx) => ctx.bucket(q.bucket).all());
        break;
      case 'where':
        store.defineQuery(q.name, async (ctx, params) =>
          ctx.bucket(q.bucket).where({ [q.field]: params[q.field] }),
        );
        break;
      case 'count':
        store.defineQuery(q.name, async (ctx) => ctx.bucket(q.bucket).count());
        break;
    }
  }
}

let engine = undefined;
if (config.rules !== false) {
  engine = await RuleEngine.start({ name: `py-test-rules-${Date.now()}` });
}

// ── Auth config ──────────────────────────────────────────────────

let auth = undefined;

if (config.auth) {
  if (config.auth.builtIn) {
    // Built-in identity auth
    auth = {
      builtIn: true,
      adminSecret: config.auth.adminSecret || 'test-secret',
    };
  } else if (config.auth.sessions) {
    // Session-based auth with static token → session mapping
    const sessions = config.auth.sessions;
    const authConfig = {
      validate: async (token) => sessions[token] ?? null,
    };

    if (config.auth.required === false) {
      authConfig.required = false;
    }

    if (config.auth.permissions) {
      const allowedRoles = config.auth.permissions.checkRoles;
      authConfig.permissions = {
        check: (session, _operation, _resource) => {
          return session.roles.some((r) => allowedRoles.includes(r));
        },
      };
    }

    auth = authConfig;
  }
}

// ── Audit config ─────────────────────────────────────────────────

let audit = undefined;
if (config.audit !== undefined) {
  audit = {};
  if (config.audit.tiers) {
    audit.tiers = config.audit.tiers;
  }
}

// ── Start server ─────────────────────────────────────────────────

const server = await NoexServer.start({
  store,
  rules: engine,
  auth,
  audit,
  port: 0,
  host: '127.0.0.1',
});

// Print the URL to stdout — the Python test fixture reads this.
console.log(`ws://127.0.0.1:${server.port}`);

// Keep running until killed
process.on('SIGTERM', async () => {
  if (server.isRunning) await server.stop();
  if (engine) await engine.stop();
  await store.stop();
  process.exit(0);
});

process.on('SIGINT', async () => {
  if (server.isRunning) await server.stop();
  if (engine) await engine.stop();
  await store.stop();
  process.exit(0);
});
