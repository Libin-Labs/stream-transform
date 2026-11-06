import test from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import {
  map,
  filter,
  flatMap,
  batch,
  take,
  drop,
  scan,
  reduce,
  collect,
  hashJoin,
  observe,
} from '../dist/esm/index.js';

test('mapping and filtering support async callbacks and stable zero-based indexes', async () => {
  const indexes = [];
  const values = map(
    filter([2, 3, 4], async (value, index) => {
      indexes.push(index);
      return value % 2 === 0;
    }),
    async (value, index) => `${index}:${value * 2}`,
  );
  assert.deepEqual(await collect(values), ['0:4', '1:8']);
  assert.deepEqual(indexes, [0, 1, 2]);
});
test('construction is lazy; take pulls no extra rows and returns its upstream iterator', async () => {
  let opened = 0,
    pulled = 0,
    closed = 0;
  const source = {
    *[Symbol.iterator]() {
      opened++;
      try {
        for (let i = 0; i < 10; i++) {
          pulled++;
          yield i;
        }
      } finally {
        closed++;
      }
    },
  };
  const values = take(
    map(source, (value) => value * 2),
    3,
  );
  assert.equal(opened, 0);
  assert.deepEqual(await collect(values), [0, 2, 4]);
  assert.deepEqual([opened, pulled, closed], [1, 3, 1]);
  assert.deepEqual(await collect(take(source, 0)), []);
  assert.equal(opened, 1);
});
test('flatMap closes inner and outer sources on early consumer return', async () => {
  let innerClosed = false,
    outerClosed = false;
  async function* outer() {
    try {
      yield 1;
      yield 2;
    } finally {
      outerClosed = true;
    }
  }
  async function* inner(value) {
    try {
      yield value;
      yield value + 1;
    } finally {
      innerClosed = true;
    }
  }
  assert.deepEqual(await collect(take(flatMap(outer(), inner), 1)), [1]);
  assert.equal(innerClosed, true);
  assert.equal(outerClosed, true);
});
test('batch emits an independent final partial batch and drop counts input rows', async () => {
  const groups = await collect(batch(drop([0, 1, 2, 3, 4, 5], 1), 2));
  assert.deepEqual(groups, [[1, 2], [3, 4], [5]]);
  groups[0].push(99);
  assert.deepEqual(groups[1], [3, 4]);
  assert.deepEqual(await collect(batch([], 2)), []);
});
test('scan and reduce have explicit initial state including empty input', async () => {
  assert.deepEqual(await collect(scan([1, 2, 3], async (sum, n, i) => sum + n + i, 0)), [1, 4, 9]);
  assert.equal(await reduce([], (sum, n) => sum + n, 17), 17);
  assert.equal(await reduce([1, 2, 3], (sum, n) => sum + n, 0), 6);
});
test('sequential callback backpressure never schedules ahead', async () => {
  let active = 0,
    peak = 0,
    calls = 0;
  const values = map([1, 2, 3], async (value) => {
    peak = Math.max(peak, ++active);
    calls++;
    await Promise.resolve();
    active--;
    return value;
  });
  const iterator = values[Symbol.asyncIterator]();
  await iterator.next();
  assert.equal(calls, 1);
  await iterator.return();
  assert.equal(peak, 1);
});
test('abort before consumption does not open source; callback abort prevents output', async () => {
  const controller = new AbortController();
  const reason = new Error('stop');
  controller.abort(reason);
  let opened = false;
  function* source() {
    opened = true;
    yield 1;
  }
  await assert.rejects(
    collect(map(source(), (v) => v, { signal: controller.signal })),
    (error) => error === reason,
  );
  assert.equal(opened, false);
  const during = new AbortController();
  let closed = false;
  function* values() {
    try {
      yield 1;
    } finally {
      closed = true;
    }
  }
  await assert.rejects(
    collect(
      map(
        values(),
        (v) => {
          during.abort(reason);
          return v;
        },
        { signal: during.signal },
      ),
    ),
    (error) => error === reason,
  );
  assert.equal(closed, true);
});
test('source and callback exceptions propagate and release upstream', async () => {
  let closed = false;
  async function* source() {
    try {
      yield 1;
      throw new Error('source');
    } finally {
      closed = true;
    }
  }
  await assert.rejects(collect(map(source(), (v) => v)), /source/);
  assert.equal(closed, true);
  closed = false;
  function* other() {
    try {
      yield 1;
      yield 2;
    } finally {
      closed = true;
    }
  }
  await assert.rejects(
    collect(
      map(other(), () => {
        throw new Error('callback');
      }),
    ),
    /callback/,
  );
  assert.equal(closed, true);
  await assert.rejects(collect(filter([1], () => 1)), /boolean/);
  await assert.rejects(collect(flatMap([1], () => null)), /iterable/);
});
test('collection quota rejects overflow and closes the source', async () => {
  let closed = false;
  function* source() {
    try {
      yield 1;
      yield 2;
    } finally {
      closed = true;
    }
  }
  await assert.rejects(collect(source(), { maxItems: 1 }), /maxItems/);
  assert.equal(closed, true);
  assert.deepEqual(await collect([], { maxItems: 0 }), []);
});
test('hash join preserves left and right ordering, duplicates and null keys', async () => {
  const right = [
    { k: 1, v: 'a' },
    { k: null, v: 'null' },
    { k: 1, v: 'b' },
  ];
  const result = await collect(
    hashJoin(
      [1, 2, null],
      right,
      (v) => v,
      async (v) => v.k,
      { mode: 'left' },
    ),
  );
  assert.deepEqual(
    result.map((v) => [v.left, v.right?.v, v.matched]),
    [
      [1, 'a', true],
      [1, 'b', true],
      [2, undefined, false],
      [null, 'null', true],
    ],
  );
  assert.deepEqual(
    await collect(
      hashJoin(
        [2],
        right,
        (v) => v,
        (v) => v.k,
      ),
    ),
    [],
  );
});
test('hash join distinguishes matching undefined values from missing matches', async () => {
  assert.deepEqual(
    await collect(
      hashJoin(
        [0, 1],
        [undefined],
        (v) => v,
        () => 0,
        { mode: 'left' },
      ),
    ),
    [
      { left: 0, right: undefined, matched: true },
      { left: 1, right: undefined, matched: false },
    ],
  );
});
test('right quota rejects before opening left; pair quota closes left', async () => {
  let opened = false,
    rightClosed = false,
    leftClosed = false;
  function* left() {
    opened = true;
    try {
      yield 1;
    } finally {
      leftClosed = true;
    }
  }
  function* right() {
    try {
      yield 1;
      yield 1;
    } finally {
      rightClosed = true;
    }
  }
  await assert.rejects(
    collect(
      hashJoin(
        left(),
        right(),
        (v) => v,
        (v) => v,
        { maxRightRows: 1 },
      ),
    ),
    /maxRightRows/,
  );
  assert.equal(opened, false);
  assert.equal(rightClosed, true);
  await assert.rejects(
    collect(
      hashJoin(
        left(),
        [1, 1],
        (v) => v,
        (v) => v,
        { maxPairs: 1 },
      ),
    ),
    /maxPairs/,
  );
  assert.equal(leftClosed, true);
});
test('observe counts emissions and reports completion with a backward-clamped clock', async () => {
  const events = [];
  const times = [10, 12, 11, 15];
  assert.deepEqual(
    await collect(observe([1, 2], (metric) => events.push(metric), { clock: () => times.shift() })),
    [1, 2],
  );
  assert.deepEqual(events, [
    { count: 1, elapsedMs: 2, completed: false },
    { count: 2, elapsedMs: 2, completed: false },
    { count: 2, elapsedMs: 5, completed: true },
  ]);
});
test('observe final events distinguish early returns and failures from exhaustion', async () => {
  const events = [];
  await collect(
    take(
      observe([1, 2], (v) => events.push(v)),
      1,
    ),
  );
  assert.equal(events.at(-1).completed, false);
  assert.equal(events.at(-1).count, 1);
  await assert.rejects(collect(observe([], () => {}, { clock: () => NaN })), /finite/);
  await assert.rejects(
    collect(
      observe([1], () => {
        throw new Error('metrics');
      }),
    ),
    /metrics/,
  );
});
test('configuration failures are explicit and string iterables are supported', async () => {
  for (const n of [-1, NaN, 1.5, Infinity]) {
    assert.throws(() => take([], n), RangeError);
    assert.throws(() => drop([], n), RangeError);
    assert.throws(() => batch([], n), RangeError);
  }
  assert.throws(() => batch([], 0), RangeError);
  assert.throws(() => map(null, (v) => v), TypeError);
  assert.throws(() => map([], null), TypeError);
  assert.throws(() => map([], (v) => v, { signal: {} }), TypeError);
  assert.throws(
    () =>
      hashJoin(
        [],
        [],
        (v) => v,
        (v) => v,
        { mode: 'outer' },
      ),
    TypeError,
  );
  assert.deepEqual(await collect(map('abc', (v) => v.toUpperCase())), ['A', 'B', 'C']);
});
test('large generated streams aggregate without collecting intermediate rows', async () => {
  function* values() {
    for (let i = 1; i <= 50_000; i++) yield i;
  }
  assert.equal(
    await reduce(
      map(values(), (v) => v * 2),
      (sum, v) => sum + v,
      0,
    ),
    2_500_050_000,
  );
});
test('CommonJS supports the same pull-based operators', async () => {
  const require = createRequire(import.meta.url);
  const api = require('../dist/cjs/index.js');
  assert.deepEqual(await api.collect(api.map([1, 2], (v) => v + 1)), [2, 3]);
});
