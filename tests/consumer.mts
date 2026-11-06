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
  type Source,
  type StreamMetric,
} from '@lp-labs/stream-transform';
const input: Source<number> = [1, 2];
const output: Promise<string[]> = collect(map(input, async (value, index) => `${index}:${value}`));
const records = hashJoin(
  [{ id: 1 }],
  [{ owner: 1, label: 'a' }],
  (row) => row.id,
  (row) => row.owner,
  { mode: 'left' },
);
for await (const row of records) {
  if (row.matched) {
    const label: string = row.right.label;
    void label;
  }
}
const metrics: StreamMetric[] = [];
await collect(
  observe(
    batch(
      take(
        drop(
          filter(input, (n) => n > 0),
          0,
        ),
        2,
      ),
      1,
    ),
    (event) => {
      metrics.push(event);
    },
  ),
);
await collect(flatMap(input, (n) => [n, n]));
const totals: AsyncIterable<number> = scan(input, (sum, n) => sum + n, 0);
const total: number = await reduce(totals, (sum, n) => sum + n, 0);
// @ts-expect-error map does not discard its result type.
const invalid: AsyncIterable<number> = map(input, (n) => String(n));
void [output, total, invalid];
