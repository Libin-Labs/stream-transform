# @lp-labs/stream-transform

Pull-based transformations for JavaScript iterables and async iterables. Compose data pipelines without collecting every intermediate result: map, filter, flatten, batch, limit, aggregate, join and observe.

- Zero runtime or peer dependencies; framework-independent.
- ESM, CommonJS and TypeScript declarations.
- Sequential async callbacks, cooperative cancellation and iterator cleanup.
- Explicit collection, join-build and output quotas.
- No shared package imports, background timers, global patches or browser globals at import.

## Installation

```sh
npm install @lp-labs/stream-transform
```

Requires Node.js 18+ or a modern browser with async iterators. Cancellation additionally uses the platform's `AbortSignal.throwIfAborted()`. Browser use requires an ESM-aware bundler or an ESM URL; this is not a script-tag global bundle.

## Quickstart

```js
import { map, filter, batch, collect, reduce } from '@lp-labs/stream-transform';

async function* orders() {
  yield { paid: true, cents: 1200 };
  yield { paid: false, cents: 999 };
  yield { paid: true, cents: 300 };
}

const amounts = map(filter(orders(), (row) => row.paid), (row) => row.cents);
const total = await reduce(amounts, (sum, cents) => sum + cents, 0);
console.log(total); // 1500
console.log(await collect(batch([1, 2, 3, 4, 5], 2))); // [[1, 2], [3, 4], [5]]
```

For CommonJS, use `const { map, collect } = require('@lp-labs/stream-transform')`.

## Execution model

`Source<T>` is `Iterable<T> | AsyncIterable<T>`. All operators return an **async iterable**, including when the source is synchronous. Terminal operations return promises. Each returned operator is a single-use async generator: create a new pipeline to run it again. A reusable input such as an array can be used to construct multiple pipelines.

Constructing an operator validates configuration but does not open or consume input. Each downstream pull advances upstream only as far as necessary for its next result. Filtering may read several rejected items for a single downstream pull. Callbacks run sequentially and are awaited; there is no speculative work or internal concurrency. Indexes start at zero and count that operator's input, not a global original-row index.

Sources own their resources. Early return, errors and cancellation call an unfinished source iterator's `return()` and await it. Nested flat maps release both active inner and outer iterators. An iterator already naturally exhausted is not returned again. Put file, cursor and subscription cleanup in your source's `finally` block. Cleanup exceptions propagate; JavaScript cleanup errors can replace the original error.

## API

Runtime exports: `map`, `filter`, `flatMap`, `batch`, `take`, `drop`, `scan`, `reduce`, `collect`, `hashJoin`, `observe`.

Type exports: `Source`, `StreamOptions`, `Mapper`, `CollectOptions`, `JoinOptions`, `Joined`, `StreamMetric`, `ObserveOptions`.

All operators accept a final optional configuration object with `signal?: AbortSignal`.

| Function | Behavior |
|---|---|
| `map(source, transform, options?)` | Await `(value, index) => result` and emit each result. |
| `filter(source, predicate, options?)` | Await `(value, index) => boolean`; non-boolean results are errors, not truthy filters. |
| `flatMap(source, transform, options?)` | Await `(value, index) => Source<R>` and fully drain that nested source before the next outer item. |
| `batch(source, size, options?)` | Emit independent arrays of up to `size` items, including a final partial batch. `size` must be a positive safe integer. |
| `take(source, count, options?)` | Emit at most `count` items, then close upstream without pulling an extra item. Zero does not open input. |
| `drop(source, count, options?)` | Consume and omit the first `count` items; emit the remainder. |
| `scan(source, reducer, initial, options?)` | Await `(state, value, index) => state`; emit updated state after each input. Does not emit `initial` separately. |
| `reduce(source, reducer, initial, options?)` | Return the final state, or `initial` for an empty source. |
| `collect(source, options?)` | Return an array; default `maxItems` is 100000. |
| `hashJoin(left, right, leftKey, rightKey, options?)` | Build a bounded right-side index, then stream inner or left-outer pairs. |
| `observe(source, report, options?)` | Await metrics reporting while passing through values without copying them. |

Counts and quotas must be non-negative safe integers unless otherwise specified. `collect` checks overflow by pulling the first excess item and then throws `RangeError`; it never returns a truncated success result. `maxItems: 0` accepts only an empty source.

Reducers, output items, key values and initial state are not cloned. A reducer that mutates and returns one accumulator will emit that same reference from `scan`; return new states if historical snapshots are required. Promised yielded values follow normal async-generator awaiting semantics.

## Bounded joins

```js
import { hashJoin, collect } from '@lp-labs/stream-transform';

const rows = await collect(hashJoin(
  [{ user: 1 }, { user: 2 }],
  [{ id: 1, name: 'Ada' }, { id: 1, name: 'A. Lovelace' }],
  (order) => order.user,
  (user) => user.id,
  { mode: 'left', maxRightRows: 1000, maxPairs: 5000 },
));
for (const row of rows) {
  console.log(row.matched ? row.right.name : 'unmatched');
}
```

Key callbacks accept `(value, index)` and may be async. Keys use JavaScript `Map` equality: primitive value equality, `NaN` matching itself, and object **reference** equality. Composite keys should use a consistent explicit encoding; allocating a new object for each key will not match equivalent objects.

| Join option | Default | Meaning |
|---|---|---|
| `mode` | `'inner'` | `'inner'` or `'left'`; left mode emits unmatched rows. |
| `maxRightRows` | `10000` | Maximum indexed right rows, including duplicate keys. |
| `maxPairs` | `100000` | Maximum total emitted rows, including unmatched left rows. |
| `signal` | none | Cooperative cancellation for both sources and callbacks. |

The right source is consumed before opening left. Results preserve left input order and right input order within each matching bucket. Duplicate keys produce all matching pairs. No right/full outer join, sort-merge join or disk spill is implied.

`Joined<L, R>` is discriminated by `matched`. For an unmatched row, `right` is `undefined`; matching an actual right-side `undefined` is distinguishable by `matched: true`. At quota overflow, previously yielded pairs remain yielded: streaming is not an atomic transaction.

## Cancellation and cleanup

```js
import { map, collect } from '@lp-labs/stream-transform';

const controller = new AbortController();
const output = map([1, 2, 3], async (value) => {
  controller.signal.throwIfAborted();
  return value * 10;
}, { signal: controller.signal });
console.log(await collect(output));
```

Signals are checked before opening/pulling sources, after awaited pulls/callbacks, and between join matches. Abort rejects with the signal's reason; it does not return a partial array. Cancellation is **cooperative**: it cannot interrupt a pending source `next()` or callback that never settles. Pass the same signal into fetch/storage/worker adapters and make those operations settle on abort. No cancellation race leaves unobserved callback promises running.

There are no built-in file, database or worker transport adapters. Supply async iterables whose lifecycle and backpressure obey the documented iterator protocol. This avoids hiding queues or environment-specific dependencies inside transformations.

## Metrics and customization

```js
import { observe, take, collect } from '@lp-labs/stream-transform';

const events = [];
const stage = observe([10, 20, 30], async (metric) => { events.push(metric); });
console.log(await collect(take(stage, 2)));
console.log(events.at(-1)); // { count: 2, elapsedMs: ..., completed: false }
```

`report(metric)` may return void or an awaitable void. It runs before each item is yielded, then once in `finally`. `count` measures items observed at this stage, **not committed downstream work**. `completed: true` appears only in the final event after natural exhaustion. An early downstream limit, failure, or abort finishes with `completed: false`. A final callback failure rejects the pipeline and can replace a preceding error; metrics must be reliable if they are part of the pipeline.

Use `{ clock: () => milliseconds }` to inject a deterministic clock; default is `Date.now`. Values must be finite. Backward movement is clamped so elapsed time cannot go backward. Elapsed time includes consumer pauses, not only CPU time. Wrap multiple stages with different observer callbacks for per-stage reports.

## Performance and limits

- Map/filter/take/drop hold O(1) item references; batch holds O(size).
- Flat map keeps only the active inner source; scan/reduce keep the chosen accumulator.
- Collection holds O(maxItems); right-side join holds O(maxRightRows) references plus buckets.
- Join CPU is expected O(right rows + left rows + emitted pairs), excluding callback costs.
- Limits count rows, not nested bytes. A single huge value, key or accumulator can still consume large memory.
- Async generators have per-item promise overhead. Batch workloads when callbacks do very little work.
- There is no parallel map, scheduler, retry, transaction, cross-worker execution or automatic storage spill in this release.

Configuration/protocol/callback-shape errors throw `TypeError`; count/quota errors throw `RangeError`. Source, callback, reporting, cleanup and abort errors propagate explicitly. No errors are logged by the library because the caller owns its reporting policy.

This independently authored initial release is tested with cleanup, cancellation, bounded joins and generated streams; it does not claim compatibility with any other stream library.

## Support

If this library is useful to you, consider supporting its maintenance with a [donation via PayPal](https://www.paypal.me/LibinPrasanth).

## Author

Created by [Libin Prasanth](https://www.linkedin.com/in/libinprasanth/). Connect on LinkedIn for updates and discussion.
