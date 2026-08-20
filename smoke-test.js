const { Database } = require('jsql-neo');

async function main() {
  const db = new Database('/tmp/neo-test/db', {
    mode: 'disk',
    flushInterval: 0,
    wal: true,
    fileLock: true
  });
  await db.start();

  if (!db.hasTable('jobs')) {
    db.createTable('jobs', {
      id: { type: 'string', primaryKey: true },
      name: { type: 'string' },
      payload: { type: 'any' },
      status: { type: 'string', default: 'pending' },
      priority: { type: 'integer', default: 0 },
      runAt: { type: 'integer', default: 0 }
    });
  }

  db.insert('jobs', { id: 'a1', name: 'send-email', payload: { to: 'x@y.z' }, status: 'pending', priority: 10, runAt: 0 });

  // update
  db.updateById('jobs', 'a1', { status: 'running', priority: 20 });
  const row = db.findById('jobs', 'a1');
  console.log('after update:', row.status, row.priority, JSON.stringify(row.payload));

  // find filter
  const all = db.find('jobs', { status: 'pending' });
  console.log('find pending count:', all.length);

  // insert array object payload
  db.insert('jobs', { id: 'a2', name: 'backup', payload: { deep: { list: [1,2,3] } }, status: 'running', priority: 5, runAt: 100 });
  const a2 = db.findById('jobs', 'a2');
  console.log('a2 payload list:', JSON.stringify(a2.payload));

  await db.stop();

  // re-open to verify persistence
  const db2 = new Database('/tmp/neo-test/db', { mode: 'disk', flushInterval: 0, wal: true, fileLock: true });
  await db2.start();
  console.log('after reopen count:', db2.count('jobs'), 'a1:', db2.findById('jobs','a1').status);
  await db2.stop();
}

main().catch(e => { console.error('ERR', e); process.exit(1); });