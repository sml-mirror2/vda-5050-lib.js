const { test } = require('node:test');
const assert = require('node:assert/strict');
const net = require('node:net');
const { spawn } = require('node:child_process');
const { once } = require('node:events');
const { setTimeout: delay } = require('node:timers/promises');
const mqtt = require('../node_modules/mqtt');
const { AgvController, VirtualAgvAdapter } = require('../dist');

async function waitFor(fn, ms = 10000) {
  const end = Date.now() + ms;
  while (!fn()) { if (Date.now() > end) throw Error('condition timed out'); await delay(20); }
}
async function freePort() {
  const s = net.createServer(); s.listen(0, '127.0.0.1'); await once(s, 'listening');
  const p = s.address().port; await new Promise(r => s.close(r)); return p;
}
async function broker(port = 0) {
  port ||= await freePort();
  const proc = spawn('mosquitto', ['-p', String(port)], { stdio: 'ignore' });
  let ready = false;
  await waitFor(() => { if (!ready) { const s = net.connect(port, '127.0.0.1'); s.on('connect', () => { ready = true; s.destroy(); }); s.on('error', () => s.destroy()); } return ready; });
  return { port, url: `mqtt://127.0.0.1:${port}`, stop: async () => { if (proc.exitCode !== null) return; proc.kill(); await once(proc, 'exit'); } };
}
function controller(url) {
  return new AgvController({ manufacturer: 'recovery-test', serialNumber: '1' }, {
    interfaceName: 'test', vdaVersion: '2.1.0',
    transport: { brokerUrl: url, recoverForever: true, connectTimeout: 3000, reconnectPeriod: 1000, heartbeat: 2 },
  }, { agvAdapterType: VirtualAgvAdapter, publishStateInterval: 100, publishVisualizationInterval: 0 }, {});
}
function observer(url) {
  const c = mqtt.connect(url, { protocolVersion: 5, reconnectPeriod: 0 });
  const messages = [];
  c.on('message', (topic, body) => messages.push({ topic, body: JSON.parse(body) }));
  return { c, messages, ready: once(c, 'connect').then(() => new Promise((r, j) => c.subscribe('test/#', e => e ? j(e) : r()))), close: () => new Promise(r => c.end(true, r)) };
}

test('broker migration preserves a real active order, adapter, subscriptions and current state', async t => {
  const a = await broker(), b = await broker(); t.after(a.stop); t.after(b.stop);
  const ob = observer(b.url); await ob.ready; t.after(ob.close);
  const ca = mqtt.connect(a.url, { protocolVersion: 5 }); await once(ca, 'connect'); t.after(() => new Promise(r => ca.end(true, r)));
  const c = controller(a.url); t.after(() => c.stop()); await c.start();
  const adapter = c._agvAdapter, instance = c.execId;
  await waitFor(() => c._subscriptionManager.getAll().length >= 2);
  ca.publish('test/v2/recovery-test/1/order', JSON.stringify({
    headerId: 1, timestamp: new Date().toISOString(), version: '2.1.0', manufacturer: 'recovery-test', serialNumber: '1',
    orderId: 'retained-order', orderUpdateId: 0,
    nodes: [{ nodeId: 'start', sequenceId: 0, released: true, nodePosition: { x: 0, y: 0, mapId: 'local' },
      actions: [{ actionId: 'long-action', actionType: 'noop', blockingType: 'HARD', actionParameters: [{ key: 'duration', value: 60 }] }] }], edges: [],
  }));
  await waitFor(() => c.currentState.actionStates.some(a => a.actionStatus === 'RUNNING'));
  const before = Date.now(); c.updateBrokerUrl(b.url);
  await waitFor(() => ob.messages.some(m => m.topic.endsWith('/state') && m.body.orderId === 'retained-order'));
  assert.ok(Date.now() - before < 10000);
  assert.equal(c._agvAdapter, adapter); assert.equal(c.execId, instance);
  assert.equal(c.currentState.actionStates[0].actionStatus, 'RUNNING');
  assert.ok(ob.messages.some(m => m.topic.endsWith('/connection') && m.body.connectionState === 'ONLINE'));
  ob.c.publish('test/v2/recovery-test/1/instantActions', JSON.stringify({ headerId: 2, timestamp: new Date().toISOString(), version: '2.1.0', manufacturer: 'recovery-test', serialNumber: '1', actions: [{ actionId: 'state-check', actionType: 'stateRequest', blockingType: 'NONE' }] }));
  await waitFor(() => c.currentState.actionStates.some(a => a.actionId === 'state-check' && a.actionStatus === 'FINISHED'));
  const transport = c._mqtt; c.updateBrokerUrl(b.url); await delay(100); assert.equal(c._mqtt, transport);
});

test('initial refusal and subsequent same-address outage recover within default budget', async t => {
  const p = await freePort(), url = `mqtt://127.0.0.1:${p}`;
  const c = controller(url); t.after(() => c.stop()); const started = c.start();
  await delay(4200); let b = await broker(p); t.after(() => b.stop());
  let before = Date.now(); await started; assert.ok(Date.now() - before < 10000);
  const adapter = c._agvAdapter;
  await waitFor(() => c._subscriptionManager.getAll().length >= 2);
  await delay(100);
  await b.stop(); await delay(4200); before = Date.now(); b = await broker(p);
  await waitFor(() => c._mqtt.connected); assert.ok(Date.now() - before < 10000); assert.equal(c._agvAdapter, adapter);
});

test('blackholed sockets are detected and same destination reconnects', async t => {
  const b = await broker(); t.after(b.stop);
  let blackhole = false; const sockets = new Set(); let connections = 0;
  const proxy = net.createServer(client => {
    connections++; const upstream = net.connect(b.port, '127.0.0.1'); sockets.add(client); sockets.add(upstream);
    client.on('data', data => { if (!blackhole) upstream.write(data); });
    upstream.on('data', data => { if (!blackhole) client.write(data); });
    client.on('error', () => {}); upstream.on('error', () => {});
    client.on('close', () => { sockets.delete(client); upstream.destroy(); });
    upstream.on('close', () => { sockets.delete(upstream); client.destroy(); });
  });
  proxy.listen(0, '127.0.0.1'); await once(proxy, 'listening');
  t.after(async () => { for (const s of sockets) s.destroy(); await new Promise(r => proxy.close(r)); });
  const c = controller(`mqtt://127.0.0.1:${proxy.address().port}`); t.after(() => c.stop()); await c.start();
  blackhole = true; await delay(9000); blackhole = false; const before = Date.now();
  await waitFor(() => c._mqtt.connected); assert.ok(Date.now() - before < 10000); assert.ok(connections >= 2);
});

test('latest endpoint wins during initial blackhole; stop cancels initial and established recovery', async t => {
  const sockets = new Set(); const silent = net.createServer(s => { sockets.add(s); s.resume(); s.on('error', () => {}); s.on('close', () => sockets.delete(s)); });
  silent.listen(0, '127.0.0.1'); await once(silent, 'listening');
  t.after(async () => { for (const s of sockets) s.destroy(); await new Promise(r => silent.close(r)); });
  const a = `mqtt://127.0.0.1:${silent.address().port}`, b = await broker(); t.after(b.stop);
  const c = controller(a); t.after(() => c.stop()); const started = c.start(); await delay(100);
  c.updateBrokerUrl(b.url); c.updateBrokerUrl(a); c.updateBrokerUrl(b.url);
  await started; assert.equal(c.clientOptions.transport.brokerUrl, b.url); await waitFor(() => sockets.size === 0);
  await c.stop(); const generation = c._transportGeneration; await delay(1200); assert.equal(c._transportGeneration, generation); assert.equal(c._mqtt.connected, false);
  const d = controller(a); const pending = d.start(); const rejection = assert.rejects(pending, /stopped/); await delay(100); await d.stop(); await rejection;
  await delay(1200); assert.equal(d._mqtt.connected, false); await waitFor(() => sockets.size === 0);
});
