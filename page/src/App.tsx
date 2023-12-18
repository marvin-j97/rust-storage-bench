import { ApexChartProps, SolidApexCharts } from 'solid-apexcharts';
import { For, Show, createSignal, onMount } from 'solid-js'

const numberFormatter = Intl.NumberFormat(undefined, {
  maximumFractionDigits: 0,
});
const formatNumber = (n: number) => numberFormatter.format(n);

export function readFile(file: File): Promise<string> {
  const fileReader = new FileReader();

  const promise = new Promise<string>((resolve, reject) => {
    fileReader.onloadend = (ev) => {
      resolve(ev.target!.result!.toString());
    };
    fileReader.onerror = (error) => {
      reject(error);
    };
  });

  fileReader.readAsText(file);

  return promise;
}

export function parseJsonl<T>(text: string): T[] {
  return text
    .split("\n")
    .filter(Boolean)
    .map((line) => JSON.parse(line));
}

type HistoryEntry = MetricEntry;

type MetricEntry = {
  cpu: number;
  mem_mib: number;
  time_micro: number;
  du_mib: number;
  disk_mib_w: number;
  disk_mib_r: number;
};

const chartOptions: ApexChartProps["options"]["chart"] = {
  background: "transparent",
  animations: {
    enabled: false,
  },
  toolbar: {
    show: false
  },
  zoom: {
    enabled: false
  },
}

const xaxisOptions: ApexChartProps["options"]["xaxis"] = {
  axisBorder: {
    show: true,
  },
  type: "numeric",
  labels: {
    style: {
      colors: "white"
    },
    formatter: (value) => `${Math.floor(+value)}s`
  }
}

const colors = ["#a78bfa", "#38bdf8", "#4ade80", "#fbbf24", "#f87171", "#f472b6"];

const baseOptions: ApexChartProps["options"] = {
  grid: {
    show: false,
  },
  tooltip: {
    enabled: false,
  },
  dataLabels: {
    enabled: false
  },
  legend: {
    position: "top",
    horizontalAlign: 'right',
    labels: {
      colors: "white"
    }
  }
}

function LineChart(props: { title: string, yFormatter: (val: number) => string, series: { name: string, data: { x: number, y: number }[] }[] }) {
  const options = () => ({
    ...baseOptions,
    title: {
      text: props.title,
      style: {
        color: "white"
      }
    },
    stroke: {
      colors: ["#aaffff"],
      width: 2
    },
    chart: {
      id: 'mem',
      ...chartOptions,
    },
    xaxis: {
      ...xaxisOptions,
    },
    yaxis: {
      axisBorder: {
        show: true,
      },
      labels: {
        style: {
          colors: "white"
        },
        formatter: props.yFormatter,
      },

    },
  } satisfies ApexChartProps["options"]);

  const series = () => ({
    list: [
      ...props.series.map(({ name, data }, idx) => {

        return {
          name,
          data: data.map(({ x, y }) => ({
            x,
            y,
          })),
          color: colors[idx % colors.length]
        } satisfies ApexAxisChartSeries[0]
      }),
    ] satisfies ApexAxisChartSeries
  });

  return <SolidApexCharts
    type="line"
    width="100%"
    options={options()}
    series={series().list}
  />
}

function WriteAmpHistory(props: { series: HistoryEntry[][] }) {
  const series = () => props.series.map((series, idx) => {
    const metrics = series.slice(2);
    const start = metrics[0].time_micro;

    const setupInfo = series[1] as unknown as { backend: string, workload: string };

    return {
      name: setupInfo.backend,
      data: metrics.map(({ time_micro, du_mib, disk_mib_w }) => ({
        x: (time_micro - start) / 1000 / 1000,
        y: disk_mib_w / du_mib,
      })),
      color: colors[idx % colors.length]
    } satisfies ApexAxisChartSeries[0]
  });

  return <LineChart
    yFormatter={(n) => `${n}x`}
    title="Write amplification"
    series={series()}
  />;
}

function DiskSpaceUsageHistory(props: { series: HistoryEntry[][] }) {
  const series = () => props.series.map((series, idx) => {
    const metrics = series.slice(2);
    const start = metrics[0].time_micro;

    const setupInfo = series[1] as unknown as { backend: string, workload: string };

    return {
      name: setupInfo.backend,
      data: metrics.map(({ time_micro, du_mib }) => ({
        x: (time_micro - start) / 1000 / 1000,
        y: du_mib,
      })),
      color: colors[idx % colors.length]
    } satisfies ApexAxisChartSeries[0]
  });

  return <LineChart
    yFormatter={(n) => `${n} MiB`}
    title="Disk space usage"
    series={series()}
  />;
}

function MemoryUsageHistory(props: { series: HistoryEntry[][] }) {
  const series = () => props.series.map((series, idx) => {
    const metrics = series.slice(2);
    const start = metrics[0].time_micro;

    const setupInfo = series[1] as unknown as { backend: string, workload: string };

    return {
      name: setupInfo.backend,
      data: metrics.map(({ time_micro, mem_mib }) => ({
        x: (time_micro - start) / 1000 / 1000,
        y: mem_mib,
      })),
      color: colors[idx % colors.length]
    } satisfies ApexAxisChartSeries[0]
  });

  return <LineChart
    yFormatter={(n) => `${n} MiB`}
    title="Memory pressure"
    series={series()}
  />;
}

function CpuUsageHistory(props: { series: HistoryEntry[][] }) {
  const series = () => props.series.map((series, idx) => {
    const metrics = series.slice(2);
    const start = metrics[0].time_micro;

    const setupInfo = series[1] as unknown as { backend: string, workload: string };

    return {
      name: setupInfo.backend,
      data: metrics.map(({ time_micro, cpu }) => ({
        x: (time_micro - start) / 1000 / 1000,
        y: cpu,
      })),
      color: colors[idx % colors.length]
    } satisfies ApexAxisChartSeries[0]
  });

  return <LineChart
    yFormatter={(n) => `${n} %`}
    title="CPU usage"
    series={series()}
  />;
}

function PerformanceChart(props: { title: string, values: { backend: string, value: number }[] }) {
  const series = () => ({
    list: [
      {
        name: "ops",
        data: [
          ...props.values.map(({ backend, value }, idx) => ({
            x: backend,
            y: value,
            fillColor: colors[idx % colors.length],
          }))
        ].sort((a, b) => b.y - a.y),
      }
    ] satisfies ApexAxisChartSeries,
  });

  const options = () => ({
    ...baseOptions,
    title: {
      text: props.title,
      style: {
        color: "white"
      }
    },
    stroke: {
      show: false,
    },
    chart: {
      id: 'mem',
      ...chartOptions,
    },
    xaxis: {
      categories: series().list[0].data.map(({ x }) => x),
      type: "category",
      labels: {
        style: {
          colors: "white",
        }
      }
    },
    yaxis: {
      axisBorder: {
        show: true,
      },
      labels: {
        style: {
          colors: "white"
        },
        formatter: (value) => `${formatNumber(value)} ops`
      },
    },
  } satisfies ApexChartProps["options"]);



  return <SolidApexCharts
    type="bar"
    width="100%"
    options={options()}
    series={series().list}
  />
}

type OpsObject = { backend: string, write_ops: number; read_ops: number; scan_ops: number; delete_ops: number };

function App() {
  const [items, setItems] = createSignal<(HistoryEntry)[][]>([]);
  const [ops, setOps] = createSignal<OpsObject[]>([]);

  async function handleFileUpload(file: File) {

    await readFile(file)
      .then((text) => {
        const items = parseJsonl<HistoryEntry & OpsObject>(text);
        setItems(x => [...x, items]);

        setOps(x => {
          const copy = structuredClone(x);
          let item = items.at(-1)!;
          return [...copy, item];
        });
      })
      .catch((error) => {
        console.error(error);
      });
  }

  onMount(() => {
    const handler = async (ev: DragEvent) => {
      ev.preventDefault();

      setItems([]);

      const fileList = [...(ev.dataTransfer?.files ?? [])];
      fileList.sort((a, b) => a.name.localeCompare(b.name));

      for (const file of fileList) {
        await handleFileUpload(file);
      }
    };

    document.addEventListener("drop", handler);
    document.addEventListener("dragover", (ev) => ev.preventDefault());
  });

  const sysInfo = () => items()[0][0] as unknown as {
    cpu: string;
    mem: number;
    os: string;
    kernel: string;
  };

  const runtimeSecs = () => {
    if (!items().length) {
      return 0;
    }
    return (items().at(0)!.at(-1)!.time_micro - items().at(0)!.at(0)!.time_micro) / 1000 / 1000
  };

  return (
    <>
      <Show when={items().length > 0} fallback={"Drag a .jsonl file here!!"}>
        <div style="display: flex; gap: 20px; flex-direction: column">
          <div>
            <div>
              System: {sysInfo().os} - {sysInfo().cpu} - {(sysInfo().mem / 1024 / 1024 / 1024).toFixed(2)} GB
            </div>
          </div>
          <div>
            <For each={items()}>
              {item => {
                const setupInfo = () => item[1] as unknown as { backend: string, workload: string };

                return <div>
                  <div>
                    Backend: {setupInfo().backend} - Workload: {setupInfo().workload} - Runtime: {(runtimeSecs() / 60).toFixed(2)} min
                  </div>
                </div>
              }}
            </For>
          </div>
          <div class="grid md:grid-cols-2 gap-4">
            <CpuUsageHistory series={items()} />
            <MemoryUsageHistory series={items()} />
            <DiskSpaceUsageHistory series={items()} />
            <WriteAmpHistory series={items()} />
          </div>
        </div>
      </Show>
      <Show when={ops().length > 0}>
        <div class="grid md:grid-cols-2 gap-4">
          <PerformanceChart
            title="Write performance"
            values={
              ops().map(({ backend, write_ops: value }) => ({
                backend,
                value
              }))
            }
          />
          <PerformanceChart
            title="Read performance"
            values={
              ops().map(({ backend, read_ops: value }) => ({
                backend,
                value
              }))
            }
          />
          <PerformanceChart
            title="Delete performance"
            values={
              ops().map(({ backend, delete_ops: value }) => ({
                backend,
                value
              }))
            }
          />
          <PerformanceChart
            title="Scan performance"
            values={
              ops().map(({ backend, scan_ops: value }) => ({
                backend,
                value
              }))
            }
          />
        </div>
        <div>
          <table class="min-w-full border border-gray-900 divide-y divide-gray-800">
            <thead>
              <tr>
                <th class="py-2 px-4 bg-gray-800">Backend</th>
                <th class="py-2 px-4 bg-gray-800">Writes</th>
                <th class="py-2 px-4 bg-gray-800">Reads</th>
                <th class="py-2 px-4 bg-gray-800">Scans</th>
                <th class="py-2 px-4 bg-gray-800">Deletes</th>
              </tr>
            </thead>
            <tbody>
              {
                ops().slice().sort((a, b) => {
                  if (b.read_ops > b.write_ops) {
                    return b.read_ops - a.read_ops;
                  }
                  return b.write_ops - a.write_ops;
                }).map(({ backend, write_ops, read_ops, scan_ops, delete_ops }) =>
                  <tr class="hover:bg-gray-100">
                    <td class="py-2 px-4">{backend}</td>
                    <td class="py-2 px-4">{formatNumber(write_ops)}</td>
                    <td class="py-2 px-4">{formatNumber(read_ops)}</td>
                    <td class="py-2 px-4">{formatNumber(scan_ops)}</td>
                    <td class="py-2 px-4">{formatNumber(delete_ops)}</td>
                  </tr>
                )
              }
            </tbody>
          </table>
        </div>
      </Show>
    </>
  )
}

export default App
