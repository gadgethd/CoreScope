'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

assert.equal(process.env.NODE_V8_COVERAGE, undefined, 'Run the hop-path deadline without V8 coverage');
const code = fs.readFileSync(path.resolve(__dirname, '../../public/hop-resolver.js'), 'utf8');

// A vm sandbox adds global lookup overhead that a browser does not have.
// Run the resolver in this process's ordinary JavaScript context.
global.window = {};
vm.runInThisContext(code, { filename: 'hop-resolver.js' });
const Fast = global.window.HopResolver;

// Deterministic pseudo-random layout in a ~300 km box.
let seed = 12345;
const rnd = () => (seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff;
const nodes = [];
for (let b = 0; b < 64; b++) {
  for (let k = 0; k < 16; k++) {
    const hex = b.toString(16).padStart(2, '0') + k.toString(16).padStart(2, '0');
    nodes.push({ public_key: hex + '0'.repeat(60), name: 'P' + hex, role: 'repeater',
      lat: 30 + rnd() * 2.7, lon: -98 + rnd() * 3 });
  }
}
Fast.init(nodes);
const hops = [];
for (let i = 0; i < 64; i++) hops.push(((i * 7) % 64).toString(16).padStart(2, '0'));
const opts = { observer: { lat: 31, lon: -97 } };
for (let w = 0; w < 3; w++) Fast.inferPath(hops, opts); // let the JIT settle
const times = [];
for (let r = 0; r < 5; r++) {
  const t0 = process.hrtime.bigint();
  Fast.inferPath(hops, opts);
  times.push(Number(process.hrtime.bigint() - t0) / 1e6);
}
times.sort((a, b) => a - b);
const median = times[2];
console.log('    median ' + median.toFixed(1) + ' ms over 5 runs (target 20 ms)');
// Headroom over the target for slower CI runners.
assert(median < 40, 'median under 40 ms — ' + median.toFixed(1) + ' ms');
