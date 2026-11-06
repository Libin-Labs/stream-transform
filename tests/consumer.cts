import { collect, map, hashJoin } from '@lp-labs/stream-transform';
const result: Promise<number[]> = collect(map([1, 2], (value) => value + 1));
const joined = hashJoin(
  [1],
  ['1'],
  (n) => String(n),
  (s) => s,
);
// @ts-expect-error predicate and key callbacks retain their input types.
map([1], (value: string) => value);
void [result, joined];
