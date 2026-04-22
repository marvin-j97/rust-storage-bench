
import { COMMON_CHART_OPTS } from "./chart";
import { GroupedHistograms } from "./data";
import { SolidApexCharts } from "./SolidApex";
import { formatNano } from "./util";

function toApexBoxPlotSeries(input: GroupedHistograms[]): ApexAxisChartSeries {
  return [
    {
      name: 'Latency (ns)',
      data: input.map(item => ({
        x: item.name,
        y: [
          item.data.min,
          item.data.p25,
          item.data.p50,
          item.data.p75,
          item.data.max,
        ],
        fillColor: item.color,
      })),
      type: "boxPlot",
    }
  ];
}

function BoxPlotChart(props: { title: string; percentiles: GroupedHistograms[] }) {
  return <div class="p-2 bg-neutral-100 dark:bg-neutral-900 rounded">
    <SolidApexCharts
      type="line"
      width="100%"
      options={{
        ...COMMON_CHART_OPTS({
          dashed: 0,
        }),
        chart: {
          type: "boxPlot",
        },
        stroke: {
          show: true,
          colors: ["#ffffff"],
        },
        title: {
          text: props.title,
          style: {
            color: "white",
          },
        },
        xaxis: {
          axisBorder: {
            show: true,
          },
          type: "category",
          labels: {
            style: {
              colors: "white",
            },
          },
        },
        yaxis: {
          axisBorder: {
            show: true,
          },
          labels: {
            style: {
              colors: "white",
            },
            formatter: formatNano,
          },
        },
      }}
      series={toApexBoxPlotSeries(props.percentiles)}
    />
  </div>
}