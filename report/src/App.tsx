import { ApexOptions } from "apexcharts";
import millify from "millify";
import { createSignal, For, onMount } from "solid-js";
import { createStore, produce } from "solid-js/store";
import prettyBytes from "pretty-bytes";

import { SolidApexCharts } from "./SolidApex";

import devData from "../log.jsonl?raw";
import devData2 from "../log2.jsonl?raw";

const thousandsFormatter = Intl.NumberFormat(undefined, {
  maximumFractionDigits: 1,
});
const formatThousands = (n: number) => thousandsFormatter.format(n);

type Setup = {
  displayName: string;
  args: any;
};

type Series = {
  displayName: string;
  colour: string;
  data: [number, number][];
};

const COLORS = [
  "#a78bfa",
  "#38bdf8",
  "#4ade80",
  "#fbbf24",
  "#4455FF",
  "#f472b6",
  "#ee5555",
];

function formatNano(nanos: number): string {
  if (nanos < 1_000) {
    return `${formatThousands(nanos)}ns`;
  }
  return `${(nanos / 1_000).toFixed(1)}µs`;
}

function App() {
  const [setups, setSetups] = createSignal<Setup[]>([]);

  const [state, setState] = createStore({
    cpu: [] as Series[],
    memory: [] as Series[],
    diskSpace: [] as Series[],

    writeOps: [] as Series[],
    writeLatency: [] as Series[],
    writeRate: [] as Series[],
    writtenBytes: [] as Series[],
    writePotential: [] as Series[],
    writeAmp: [] as Series[],

    pointReadLatency: [] as Series[],
    pointReadRate: [] as Series[],
    pointReadPotential: [] as Series[],
  });

  onMount(() => {
    // NOTE: Patch HTML with dev data
    if (import.meta.env.DEV) {
      console.log("hello dev");

      const dataContainer = document.querySelector("#data-container")!;

      if (
        [...dataContainer.childNodes.values()].every((x) => x.nodeType !== 1)
      ) {
        dataContainer.innerHTML += `
				<script type="data" compressed="false">
					${devData}
				</script>
				<script type="data" compressed="false">
					${devData2}
				</script>
        `;
      }
    }

    const setups: Setup[] = [];

    const cpuUsage: Series[] = [];
    const memoryUsage: Series[] = [];
    const diskSpaceUsage: Series[] = [];

    const writeOps: Series[] = [];
    const writeLatency: Series[] = [];
    const writeRate: Series[] = [];
    const writtenBytes: Series[] = [];
    const writePotential: Series[] = [];
    const writeAmp: Series[] = [];

    //const pointReadOps: Series[] = [];
    const pointReadLatency: Series[] = [];
    const pointReadRate: Series[] = [];
    const pointReadPotential: Series[] = [];

    const els = document.querySelectorAll("script[type=data]");

    // NOTE: Load data
    for (let i = 0; i < els.length; i++) {
      const item = els[i];

      const txt = item.textContent!.trim();
      const lines = txt.split("\n");
      const system = JSON.parse(lines[0]);
      const args = JSON.parse(lines[1]);

      setups.push({
        displayName: args.display_name,
        args,
      });

      //const timeStart = system.ts;

      const cpuSeries: Series = {
        displayName: args.display_name,
        colour: COLORS[i],
        data: [],
      };
      const memorySeries: Series = {
        displayName: args.display_name,
        colour: COLORS[i],
        data: [],
      };
      const diskSpaceUsageSeries: Series = {
        displayName: args.display_name,
        colour: COLORS[i],
        data: [],
      };

      const writeSeries: Series = {
        displayName: args.display_name,
        colour: COLORS[i],
        data: [],
      };
      const writeLatSeries: Series = {
        displayName: args.display_name,
        colour: COLORS[i],
        data: [],
      };
      const writeRateSeries: Series = {
        displayName: args.display_name,
        colour: COLORS[i],
        data: [],
      };
      const writtenBytesSeries: Series = {
        displayName: args.display_name,
        colour: COLORS[i],
        data: [],
      };
      const writePotentialSeries: Series = {
        displayName: args.display_name,
        colour: COLORS[i],
        data: [],
      };
      const writeAmpSeries: Series = {
        displayName: args.display_name,
        colour: COLORS[i],
        data: [],
      };

      const pointReadSeries: Series = {
        displayName: args.display_name,
        colour: COLORS[i],
        data: [],
      };
      const pointReadRateSeries: Series = {
        displayName: args.display_name,
        colour: COLORS[i],
        data: [],
      };
      const pointReadLatSeries: Series = {
        displayName: args.display_name,
        colour: COLORS[i],
        data: [],
      };
      const pointReadPotentialSeries: Series = {
        displayName: args.display_name,
        colour: COLORS[i],
        data: [],
      };

      for (const line of lines.slice(3, -1)) {
        const metrics = JSON.parse(line);

        const [
          ts,
          cpu,
          memKib,
          //
          diskSpaceKib,
          diskWriteKib,
          diskReadKib,
          //
          writeOps,
          pointReadOps,
          rangeOps,
          deleteOps,
          //
          writeLatency,
          pointReadLatency,
          rangeLatency,
          deleteLatency,
          //
          writeRate,
          pointReadRate,
          rangeRate,
          deleteRate,
          //
          writePotential,
          pointReadPotential,
          rangePotential,
          deletePotential,
          //
          writeAmp,
          space_amp,
          read_amp,
        ] = metrics;

        cpuSeries.data.push([ts, cpu]);
        memorySeries.data.push([ts, memKib]);

        if (diskSpaceKib) {
          // NOTE: disk space is 0 if an I/O error occurred
          // this can happen sometimes when the folder size is summed up
          // because files might come and go in an LSM-tree
          diskSpaceUsageSeries.data.push([ts, diskSpaceKib]);
        }

        writeSeries.data.push([ts, writeOps]);
        writeLatSeries.data.push([ts, writeLatency]);
        writeRateSeries.data.push([ts, writeRate]);
        writtenBytesSeries.data.push([ts, diskWriteKib]);
        writeAmpSeries.data.push([ts, writeAmp]);
        writePotentialSeries.data.push([ts, writePotential]);

        pointReadSeries.data.push([ts, pointReadOps]);
        pointReadLatSeries.data.push([ts, pointReadLatency]);
        pointReadRateSeries.data.push([ts, pointReadRate]);
        pointReadPotentialSeries.data.push([ts, pointReadPotential]);
      }

      cpuUsage.push(cpuSeries);
      memoryUsage.push(memorySeries);
      diskSpaceUsage.push(diskSpaceUsageSeries);

      writeOps.push(writeSeries);
      writeLatency.push(writeLatSeries);
      writeRate.push(writeRateSeries);
      writtenBytes.push(writtenBytesSeries);
      writePotential.push(writePotentialSeries);
      writeAmp.push(writeAmpSeries);

      //	pointReadOps.push(pointReadSeries);
      pointReadLatency.push(pointReadLatSeries);
      pointReadRate.push(pointReadRateSeries);
      pointReadPotential.push(pointReadPotentialSeries);
    }

    // TODO: file input if there are no embedded metrics file

    setSetups(setups);

    setState(
      produce((state) => {
        state.cpu = cpuUsage;
        state.memory = memoryUsage;
        state.diskSpace = diskSpaceUsage;

        state.writeOps = writeOps;
        state.writeLatency = writeLatency;
        state.writeRate = writeRate;
        state.writtenBytes = writtenBytes;
        state.writePotential = writePotential;
        state.writeAmp = writeAmp;

        state.pointReadLatency = pointReadLatency;
        state.pointReadRate = pointReadRate;
        state.pointReadLatency = pointReadLatency;
        state.pointReadPotential = pointReadPotential;
      }),
    );
  });

  const defaultYFormatter = (x: number) => (~~x).toString();

  const commonChartOptions = (
    opts = {
      yFormatter: defaultYFormatter,
      dashed: 0,
    },
  ) =>
    ({
      stroke: {
        colors: ["#aaffff"],
        width: 2,
        dashArray: opts.dashed || undefined,
      },
      grid: {
        strokeDashArray: 4,
        borderColor: "#252525",
        xaxis: {
          lines: {
            show: true,
          },
        },
        yaxis: {
          lines: {
            show: true,
          },
        },
      },
      chart: {
        background: "#1c1917",
        animations: {
          enabled: false,
        },
        zoom: {
          enabled: true,
          type: "xy",
          allowMouseWheelZoom: false,
        },
      },
      tooltip: {
        enabled: false,
      },
      dataLabels: {
        enabled: false,
      },
      legend: {
        position: "top",
        horizontalAlign: "right",
        labels: {
          colors: "white",
        },
      },
      xaxis: {
        axisBorder: {
          show: true,
        },
        type: "numeric",
        labels: {
          style: {
            colors: "white",
          },
          formatter: (value) => `${Math.floor(+value)}s`,
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
          formatter: opts.yFormatter,
        },
      },
    }) satisfies ApexOptions;

  return (
    <div class="flex flex-col gap-5">
      {/* topbar */}
      <div class="p-2 border-b border-stone-200 dark:border-stone-800">
        <h1 class="text-sm">rust-storage-bench 1.0.0</h1>
      </div>

      {/* content */}
      <div class="px-2">
        <h2 class="text-lg mb-3">Results</h2>
        {/* graphs */}
        <div class="grid md:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4 gap-2">
          <div class="p-2 bg-stone-100 dark:bg-stone-900 rounded">
            {(() => {
              const series = () =>
                state.cpu.map((series) => {
                  return {
                    name: series.displayName,
                    data: series.data.map(([ts_milli, value]) => ({
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
                      text: "CPU usage",
                      style: {
                        color: "white",
                      },
                    },
                    ...commonChartOptions({
                      yFormatter: (pct) => `${pct}%`,
                      dashed: 0,
                    }),
                  }}
                  series={series()}
                />
              );
            })()}
          </div>
          <div class="p-2 bg-stone-100 dark:bg-stone-900 rounded">
            {(() => {
              const series = () =>
                state.memory.map((series) => {
                  return {
                    name: series.displayName,
                    data: series.data.map(([ts_milli, value]) => ({
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
                      text: "memory usage",
                      style: {
                        color: "white",
                      },
                    },
                    ...commonChartOptions({
                      yFormatter: (bytes) =>
                        `${formatThousands(bytes / 1_024)} MiB`,
                      dashed: 0,
                    }),
                  }}
                  series={series()}
                />
              );
            })()}
          </div>
          <div class="p-2 bg-stone-100 dark:bg-stone-900 rounded">
            {(() => {
              const series = () =>
                state.diskSpace.map((series) => {
                  return {
                    name: series.displayName,
                    data: series.data.map(([ts_milli, value]) => ({
                      x: ts_milli / 1_000,
                      y: value * 1_024, // convert back to bytes
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
                      text: "disk space used",
                      style: {
                        color: "white",
                      },
                    },
                    ...commonChartOptions({
                      yFormatter: prettyBytes,
                      dashed: 0,
                    }),
                  }}
                  series={series()}
                />
              );
            })()}
          </div>
          <div>TODO: calculate space amp</div>
          <div class="p-2 bg-stone-100 dark:bg-stone-900 rounded">
            {(() => {
              const series = () =>
                state.writeLatency.map((series) => {
                  return {
                    name: series.displayName,
                    data: series.data.map(([ts_milli, value]) => ({
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
                      text: "write latency (µs)",
                      style: {
                        color: "white",
                      },
                    },
                    ...commonChartOptions({
                      yFormatter: formatNano,
                      dashed: 0,
                    }),
                  }}
                  series={series()}
                />
              );
            })()}
          </div>
          <div class="p-2 bg-stone-100 dark:bg-stone-900 rounded">
            {(() => {
              const series = () =>
                state.writeRate.map((series) => {
                  return {
                    name: series.displayName,
                    data: series.data.map(([ts_milli, value]) => ({
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
                      text: "writes per second",
                      style: {
                        color: "white",
                      },
                    },
                    ...commonChartOptions({
                      yFormatter: millify,
                      dashed: 0,
                    }),
                  }}
                  series={series()}
                />
              );
            })()}
          </div>
          <div class="p-2 bg-stone-100 dark:bg-stone-900 rounded">
            {(() => {
              const series = () =>
                state.writtenBytes.map((series) => {
                  return {
                    name: series.displayName,
                    data: series.data.map(([ts_milli, value]) => ({
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
                      text: "disk write I/O",
                      style: {
                        color: "white",
                      },
                    },
                    ...commonChartOptions({
                      yFormatter: (bytes) =>
                        `${formatThousands(bytes / 1_024 / 1_024)} GB`,
                      dashed: 0,
                    }),
                  }}
                  series={series()}
                />
              );
            })()}
          </div>
          <div class="p-2 bg-stone-100 dark:bg-stone-900 rounded">
            {(() => {
              const series = () =>
                state.writeAmp.map((series) => {
                  return {
                    name: series.displayName,
                    data: series.data.map(([ts_milli, value]) => ({
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
                      text: "write amplification",
                      style: {
                        color: "white",
                      },
                    },
                    ...commonChartOptions({
                      yFormatter: (pct) => `${pct}x`,
                      dashed: 0,
                    }),
                  }}
                  series={series()}
                />
              );
            })()}
          </div>
          <div class="p-2 bg-stone-100 dark:bg-stone-900 rounded">
            {(() => {
              const series = () =>
                state.pointReadLatency.map((series) => {
                  return {
                    name: series.displayName,
                    data: series.data.map(([ts_milli, value]) => ({
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
                      text: "point read latency (µs)",
                      style: {
                        color: "white",
                      },
                    },
                    ...commonChartOptions({
                      yFormatter: formatNano,
                      dashed: 0,
                    }),
                  }}
                  series={series()}
                />
              );
            })()}
          </div>
          <div class="p-2 bg-stone-100 dark:bg-stone-900 rounded">
            {(() => {
              const series = () =>
                state.pointReadRate.map((series) => {
                  return {
                    name: series.displayName,
                    data: series.data.map(([ts_milli, value]) => ({
                      x: ts_milli / 1_000,
                      y: value,
                    })),
                    color: series.colour,
                  } satisfies ApexAxisChartSeries[0];
                });

              // TODO: store refresh granularity (ms) in system object

              return (
                <SolidApexCharts
                  type="line"
                  width="100%"
                  options={{
                    title: {
                      text: "reads per second",
                      style: {
                        color: "white",
                      },
                    },
                    ...commonChartOptions({
                      yFormatter: millify,
                      dashed: 0,
                    }),
                  }}
                  series={series()}
                />
              );
            })()}
          </div>
          <div class="p-2 bg-stone-100 dark:bg-stone-900 rounded">
            {(() => {
              const series = () =>
                state.writePotential.map((series) => {
                  return {
                    name: series.displayName,
                    data: series.data.map(([ts_milli, value]) => ({
                      x: ts_milli / 1_000,
                      y: value,
                    })),
                    color: series.colour,
                  } satisfies ApexAxisChartSeries[0];
                });

              // TODO: store refresh granularity (ms) in system object

              return (
                <SolidApexCharts
                  type="line"
                  width="100%"
                  options={{
                    title: {
                      text: "write ops (cumulative)",
                      style: {
                        color: "white",
                      },
                    },
                    ...commonChartOptions({
                      yFormatter: millify,
                      dashed: 0,
                    }),
                  }}
                  series={series()}
                />
              );
            })()}
          </div>
          <div class="p-2 bg-stone-100 dark:bg-stone-900 rounded">
            {(() => {
              const series = () =>
                state.pointReadPotential.map((series) => {
                  return {
                    name: series.displayName,
                    data: series.data.map(([ts_milli, value]) => ({
                      x: ts_milli / 1_000,
                      y: value,
                    })),
                    color: series.colour,
                  } satisfies ApexAxisChartSeries[0];
                });

              // TODO: store refresh granularity (ms) in system object

              return (
                <SolidApexCharts
                  type="line"
                  width="100%"
                  options={{
                    title: {
                      text: "read ops (cumulative)",
                      style: {
                        color: "white",
                      },
                    },
                    ...commonChartOptions({
                      yFormatter: millify,
                      dashed: 0,
                    }),
                  }}
                  series={series()}
                />
              );
            })()}
          </div>
        </div>
      </div>

      {/* footer */}
      {/* <div class="px-2">
        <h2 class="text-lg mb-3">Setups</h2>
        <pre>
          {JSON.stringify(setups(), null, 2)}
        </pre>
      </div> */}

      {/* TODO: copy button */}
      <div class="px-2 mb-10">
        <h2 class="text-lg mb-3">Reproduce</h2>
        <div class="rounded-lg overflow-x-scroll p-2 font-mono whitespace-pre text-stone-50 bg-stone-950">
          <For each={setups()}>
            {(series) => (
              <div>
                cargo run -r -- run{" "}
                {Object.entries(series.args)
                  .map(([key, value]) =>
                    [`--${key.replace(/_/g, "-")}`, `"${value}"`].join(" "),
                  )
                  .join(" ")}
              </div>
            )}
          </For>
        </div>
      </div>
    </div>
  );
}

export default App;
