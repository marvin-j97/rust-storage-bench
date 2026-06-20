import { COMMON_CHART_OPTS } from "./chart";
import { GroupedHistograms } from "./data";
import { SolidApexCharts } from "./SolidApex";
import { formatNano } from "./util";

export default function PercentileChart(props: { title: string; percentiles: GroupedHistograms[] }) {
  return (
    <div class="p-2 bg-neutral-100 dark:bg-neutral-900 rounded">
      {(() => {
        return (
          <SolidApexCharts
            type="bar"
            width="100%"
            options={{
              title: {
                text: props.title,
                style: {
                  color: "white",
                },
              },
              ...COMMON_CHART_OPTS({
                yFormatter: formatNano,
                dashed: 0,
              }),
              xaxis: {
                categories: ["Mean", "P50", "P90", "P95", "P99"],
                labels: {
                  style: {
                    colors: ["white", "white", "white", "white", "white"],
                  },
                },
              },
              dataLabels: {
                enabled: true,
                formatter: formatNano,
                dropShadow: {
                  enabled: true,
                },
              },
              stroke: {
                show: false,
              },
            }}
            series={props.percentiles.map(p => ({
              ...p,
              data: [p.data.mean, p.data.p50, p.data.p90, p.data.p95, p.data.p99],
            }))}
          />
        );
      })()}
    </div>
  );
}