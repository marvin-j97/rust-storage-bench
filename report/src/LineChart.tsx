import { JSXElement } from "solid-js";

import { COMMON_CHART_OPTS } from "./chart";
import { TimeSeries } from "./data";
import { SolidApexCharts } from "./SolidApex";

type Props = {
  title: string;
  timeseries?: TimeSeries[];
  formatter?: (val: number, opts?: any) => string;
  filterZeroValues?: boolean;
}

export default function LineChart(props: Props): JSXElement {
  return <div class="p-2 bg-neutral-100 dark:bg-neutral-900 rounded">
    {(() => {
      const series = () =>
        (props.timeseries ?? []).map((series) => {
          let data = series.data;
          if (props.filterZeroValues) {
            data = data.filter(([_, value]) => value)
          }

          return {
            name: series.displayName,
            data: data.map(([ts_milli, value]) => ({
              x: ts_milli / 1_000,
              y: value,
            })),
            color: series.colour,
          } satisfies ApexAxisChartSeries[0];
        });

      return (
        <SolidApexCharts
          type="line"
          width="100%"
          options={{
            title: {
              text: props.title,
              style: {
                color: "white",
              },
            },
            ...COMMON_CHART_OPTS({
              yFormatter: props.formatter,
              dashed: 0,
            }),
          }}
          series={series()}
        />
      );
    })()}
  </div>;
}
