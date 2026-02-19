import { Store } from '@hamicek/noex-store';
import { NoexServer } from '@hamicek/noex-server';

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

const server = await NoexServer.start({
  store,
  port: 0,
  host: '127.0.0.1',
});

// Print the URL to stdout — the Python test fixture reads this.
console.log(`ws://127.0.0.1:${server.port}`);

// Keep running until killed
process.on('SIGTERM', async () => {
  if (server.isRunning) await server.stop();
  await store.stop();
  process.exit(0);
});

process.on('SIGINT', async () => {
  if (server.isRunning) await server.stop();
  await store.stop();
  process.exit(0);
});
