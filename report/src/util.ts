const thousandsFormatter = Intl.NumberFormat(undefined, {
  maximumFractionDigits: 1,
});
export const formatThousands = (n: number) => thousandsFormatter.format(n);

export function formatNano(nanos: number): string {
  if (nanos < 1_000) {
    return `${formatThousands(nanos)}ns`;
  }
  return `${(nanos / 1_000).toFixed(1)}µs`;
}
