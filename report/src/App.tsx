import { ApexOptions } from "apexcharts";
import millify from "millify";
import { createSignal, For, onMount } from "solid-js";

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

function App() {
	const [setups, setSetups] = createSignal<Setup[]>([]);

	const [memoryUsage, setMemoryUsage] = createSignal<Series[]>([]);
	const [diskSpaceUsage, setDiskSpaceUsage] = createSignal<Series[]>([]);

	const [writeOps, setWriteOps] = createSignal<Series[]>([]);
	const [writeLatency, setWriteLatency] = createSignal<Series[]>([]);
	const [writtenBytes, setWrittenBytes] = createSignal<Series[]>([]);
	const [writeAmp, setWriteAmp] = createSignal<Series[]>([]);

	const [pointReadOps, setPointReadOps] = createSignal<Series[]>([]);
	const [pointReadLatency, setPointReadLatency] = createSignal<Series[]>([]);

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

		const memoryUsage: Series[] = [];
		const diskSpaceUsage: Series[] = [];

		const writeOps: Series[] = [];
		const writeLatency: Series[] = [];
		const writtenBytes: Series[] = [];
		const writeAmp: Series[] = [];

		const pointReadOps: Series[] = [];
		const pointReadLatency: Series[] = [];

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
			const writtenBytesSeries: Series = {
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
			const pointReadLatSeries: Series = {
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
					diskSpaceKib,
					diskWriteKib,
					diskReadKib,
					writeOps,
					pointReadOps,
					rangeOps,
					deleteOps,
					writeLatency,
					pointReadLatency,
					rangeLatency,
					deleteLatency,
					writeAmp,
				] = metrics;

				memorySeries.data.push([ts, memKib]);

				if (diskSpaceKib) {
					// NOTE: disk space is 0 if an I/O error occurred
					// this can happen sometimes when the folder size is summed up
					// because files might come and go in an LSM-tree
					diskSpaceUsageSeries.data.push([ts, diskSpaceKib]);
				}

				writeSeries.data.push([ts, writeOps]);
				writeLatSeries.data.push([ts, writeLatency]);
				writtenBytesSeries.data.push([ts, diskWriteKib]);
				writeAmpSeries.data.push([ts, writeAmp]);

				pointReadSeries.data.push([ts, pointReadOps]);
				pointReadLatSeries.data.push([ts, pointReadLatency]);
			}

			memoryUsage.push(memorySeries);
			diskSpaceUsage.push(diskSpaceUsageSeries);

			writeOps.push(writeSeries);
			writeLatency.push(writeLatSeries);
			writtenBytes.push(writtenBytesSeries);
			writeAmp.push(writeAmpSeries);

			pointReadOps.push(pointReadSeries);
			pointReadLatency.push(pointReadLatSeries);
		}
		// TODO: file input if there are no embedded metrics file

		setSetups(setups);

		setMemoryUsage(memoryUsage);
		setDiskSpaceUsage(diskSpaceUsage);

		setWriteOps(writeOps);
		setWriteLatency(writeLatency);
		setWrittenBytes(writtenBytes);
		setWriteAmp(writeAmp);

		console.log(writeAmp);

		setPointReadOps(pointReadOps);
		setPointReadLatency(pointReadLatency);
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
					colors: "white"
				}
			},
			xaxis: {
				axisBorder: {
					show: true,
				},
				type: "numeric",
				labels: {
					style: {
						colors: "white"
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
						colors: "white"
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
				<div class="grid md:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4 gap-3">
					<div>cpu</div>
					<div class="p-2 bg-stone-100 dark:bg-stone-900 rounded">
						{(() => {
							const series = () =>
								memoryUsage().map((series) => {
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
											text: "Memory usage",
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
								diskSpaceUsage().map((series) => {
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
											text: "Disk space used",
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
								writeOps().map((series) => {
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
								writeLatency().map((series) => {
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
											yFormatter: (ns) => `${(ns / 1_000).toFixed(1)}µs`,
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
								writeLatency().map((series) => {
									return {
										name: series.displayName,
										data: series.data.map(([ts_milli, value]) => ({
											x: ts_milli / 1_000,
											y: value ? 1_000_000_000 / value : 0,
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
								writtenBytes().map((series) => {
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
											text: "Disk write I/O",
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
								writeAmp().map((series) => {
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
											text: "Write amplification",
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
								pointReadOps().map((series) => {
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
											text: "point reads (cumulative)",
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
								pointReadLatency().map((series) => {
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
											yFormatter: (ns) => `${(ns / 1_000).toFixed(1)}µs`,
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
								pointReadLatency().map((series) => {
									return {
										name: series.displayName,
										data: series.data.map(([ts_milli, value]) => ({
											x: ts_milli / 1_000,
											y: value ? 1_000_000_000 / value : 0,
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
								cargo run -r --{" "}
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
