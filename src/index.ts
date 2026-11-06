export type Source<T> = Iterable<T> | AsyncIterable<T>;
export interface StreamOptions {
  signal?: AbortSignal;
}
export type Mapper<T, R> = (value: T, index: number) => R | PromiseLike<R>;
export interface CollectOptions extends StreamOptions {
  maxItems?: number;
}
export interface JoinOptions extends StreamOptions {
  mode?: 'inner' | 'left';
  maxRightRows?: number;
  maxPairs?: number;
}
export type Joined<L, R> =
  { left: L; right: R; matched: true } | { left: L; right: undefined; matched: false };
export interface StreamMetric {
  count: number;
  elapsedMs: number;
  completed: boolean;
}
export interface ObserveOptions extends StreamOptions {
  clock?: () => number;
}

function integer(value: number, name: string, minimum = 0): number {
  if (!Number.isSafeInteger(value) || value < minimum) {
    throw new RangeError(`${name} must be a safe integer >= ${minimum}.`);
  }
  return value;
}
function callback(value: unknown): void {
  if (typeof value !== 'function') throw new TypeError('Expected a callback function.');
}
function options(value: StreamOptions): void {
  if (value === null || typeof value !== 'object') throw new TypeError('Invalid stream options.');
  if (value.signal !== undefined && typeof value.signal.throwIfAborted !== 'function') {
    throw new TypeError('signal must be an AbortSignal.');
  }
}
function asynchronous<T>(value: Source<T>): value is AsyncIterable<T> {
  return typeof (value as AsyncIterable<T>)[Symbol.asyncIterator] === 'function';
}
function source<T>(value: Source<T>): void {
  if (
    value == null ||
    (!asynchronous(value) && typeof (value as Iterable<T>)[Symbol.iterator] !== 'function')
  ) {
    throw new TypeError('Source must be iterable or async iterable.');
  }
}
function check<T>(input: Source<T>, config: StreamOptions): void {
  source(input);
  options(config);
}
async function* read<T>(input: Source<T>, config: StreamOptions): AsyncGenerator<T> {
  config.signal?.throwIfAborted();
  const iterator = asynchronous(input) ? input[Symbol.asyncIterator]() : input[Symbol.iterator]();
  let done = false;
  try {
    while (true) {
      config.signal?.throwIfAborted();
      const next = await iterator.next();
      if (typeof next !== 'object' || next === null)
        throw new TypeError('Invalid iterator result.');
      done = Boolean(next.done);
      config.signal?.throwIfAborted();
      if (done) return;
      yield next.value;
    }
  } finally {
    if (!done && iterator.return !== undefined) await iterator.return();
  }
}

export function map<T, R>(
  input: Source<T>,
  transform: Mapper<T, R>,
  config: StreamOptions = {},
): AsyncIterable<R> {
  check(input, config);
  callback(transform);
  return (async function* () {
    let index = 0;
    for await (const value of read(input, config)) {
      const result = await transform(value, index++);
      config.signal?.throwIfAborted();
      yield result;
    }
  })();
}
export function filter<T>(
  input: Source<T>,
  predicate: Mapper<T, boolean>,
  config: StreamOptions = {},
): AsyncIterable<T> {
  check(input, config);
  callback(predicate);
  return (async function* () {
    let index = 0;
    for await (const value of read(input, config)) {
      const keep = await predicate(value, index++);
      config.signal?.throwIfAborted();
      if (typeof keep !== 'boolean') throw new TypeError('Predicate must return a boolean.');
      if (keep) yield value;
    }
  })();
}
export function flatMap<T, R>(
  input: Source<T>,
  transform: Mapper<T, Source<R>>,
  config: StreamOptions = {},
): AsyncIterable<R> {
  check(input, config);
  callback(transform);
  return (async function* () {
    let index = 0;
    for await (const value of read(input, config)) {
      const nested = await transform(value, index++);
      source(nested);
      yield* read(nested, config);
    }
  })();
}
export function batch<T>(
  input: Source<T>,
  size: number,
  config: StreamOptions = {},
): AsyncIterable<T[]> {
  check(input, config);
  integer(size, 'size', 1);
  return (async function* () {
    let values: T[] = [];
    for await (const value of read(input, config)) {
      values.push(value);
      if (values.length === size) {
        yield values;
        values = [];
      }
    }
    if (values.length) yield values;
  })();
}
export function take<T>(
  input: Source<T>,
  count: number,
  config: StreamOptions = {},
): AsyncIterable<T> {
  check(input, config);
  integer(count, 'count');
  return (async function* () {
    config.signal?.throwIfAborted();
    if (count === 0) return;
    let seen = 0;
    for await (const value of read(input, config)) {
      yield value;
      if (++seen === count) return;
    }
  })();
}
export function drop<T>(
  input: Source<T>,
  count: number,
  config: StreamOptions = {},
): AsyncIterable<T> {
  check(input, config);
  integer(count, 'count');
  return (async function* () {
    let seen = 0;
    for await (const value of read(input, config)) {
      if (seen++ >= count) yield value;
    }
  })();
}
export function scan<T, R>(
  input: Source<T>,
  reducer: (state: R, value: T, index: number) => R | PromiseLike<R>,
  initial: R,
  config: StreamOptions = {},
): AsyncIterable<R> {
  check(input, config);
  callback(reducer);
  return (async function* () {
    let state = initial;
    let index = 0;
    for await (const value of read(input, config)) {
      state = await reducer(state, value, index++);
      config.signal?.throwIfAborted();
      yield state;
    }
  })();
}
export async function reduce<T, R>(
  input: Source<T>,
  reducer: (state: R, value: T, index: number) => R | PromiseLike<R>,
  initial: R,
  config: StreamOptions = {},
): Promise<R> {
  let result = initial;
  for await (const state of scan(input, reducer, initial, config)) result = state;
  return result;
}
export async function collect<T>(input: Source<T>, config: CollectOptions = {}): Promise<T[]> {
  check(input, config);
  const maximum = integer(config.maxItems ?? 100_000, 'maxItems');
  const result: T[] = [];
  for await (const value of read(input, config)) {
    if (result.length === maximum) throw new RangeError('Collection exceeds maxItems.');
    result.push(value);
  }
  return result;
}
export function hashJoin<L, R, K>(
  left: Source<L>,
  right: Source<R>,
  leftKey: Mapper<L, K>,
  rightKey: Mapper<R, K>,
  config: JoinOptions = {},
): AsyncIterable<Joined<L, R>> {
  check(left, config);
  source(right);
  callback(leftKey);
  callback(rightKey);
  const maxRight = integer(config.maxRightRows ?? 10_000, 'maxRightRows');
  const maxPairs = integer(config.maxPairs ?? 100_000, 'maxPairs');
  const mode = config.mode ?? 'inner';
  if (mode !== 'inner' && mode !== 'left') throw new TypeError('Unsupported join mode.');
  return (async function* () {
    const index = new Map<K, R[]>();
    let row = 0;
    for await (const value of read(right, config)) {
      if (row === maxRight) throw new RangeError('Right side exceeds maxRightRows.');
      const key = await rightKey(value, row++);
      config.signal?.throwIfAborted();
      const bucket = index.get(key);
      if (bucket) bucket.push(value);
      else index.set(key, [value]);
    }
    let pairs = 0;
    row = 0;
    for await (const value of read(left, config)) {
      const key = await leftKey(value, row++);
      config.signal?.throwIfAborted();
      const matches = index.get(key);
      if (matches) {
        for (const match of matches) {
          config.signal?.throwIfAborted();
          if (pairs++ === maxPairs) throw new RangeError('Join exceeds maxPairs.');
          yield { left: value, right: match, matched: true };
        }
      } else if (mode === 'left') {
        if (pairs++ === maxPairs) throw new RangeError('Join exceeds maxPairs.');
        yield { left: value, right: undefined, matched: false };
      }
    }
  })();
}
export function observe<T>(
  input: Source<T>,
  report: (metric: StreamMetric) => void | PromiseLike<void>,
  config: ObserveOptions = {},
): AsyncIterable<T> {
  check(input, config);
  callback(report);
  const clock = config.clock ?? Date.now;
  callback(clock);
  return (async function* () {
    let previous = -Infinity;
    const now = () => {
      const value = clock();
      if (!Number.isFinite(value)) throw new TypeError('Clock must return finite milliseconds.');
      previous = Math.max(previous, value);
      return previous;
    };
    const start = now();
    let count = 0;
    let completed = false;
    try {
      for await (const value of read(input, config)) {
        await report({ count: ++count, elapsedMs: now() - start, completed: false });
        config.signal?.throwIfAborted();
        yield value;
      }
      completed = true;
    } finally {
      await report({ count, elapsedMs: now() - start, completed });
    }
  })();
}
