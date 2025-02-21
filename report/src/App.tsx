import millify from "millify";
import { createSignal, For, JSXElement, Show } from "solid-js";
import prettyBytes from "pretty-bytes";

import { SolidApexCharts } from "./SolidApex";
import { formatNano } from "./util";
import { TimeSeries, useMetricsData } from "./data";
import { COMMON_CHART_OPTS } from "./chart";

type Props = {
	title: string;
	timeseries?: TimeSeries[];
	formatter?: (val: number, opts?: any) => string;
}

function LineChart(props: Props): JSXElement {
	return <div class="p-2 bg-neutral-100 dark:bg-neutral-900 rounded">
		{(() => {
			const series = () =>
				(props.timeseries ?? []).map((series) => {
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

function App() {
	const [showLsmStats, toggleLsmStats] = createSignal(true);
	const [showBtreeStats, toggleBtreeStats] = createSignal(true);

	const { percentiles, reactiveTimeseries, setups } = useMetricsData();

	return (
		<div class="flex flex-col gap-5">
			{/* topbar */}
			<div class="p-2 border-b border-neutral-200 dark:border-neutral-800">
				<h1 class="text-sm">rust-storage-bench 1.0.0</h1>
			</div>

			{/* content */}
			<div class="px-2">
				<h2 class="text-lg mb-3">Results</h2>
				{/* graphs */}
				<div class="grid md:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4 gap-2">
					{/* LINE CHARTS */}
					<LineChart
						title="CPU usage"
						timeseries={reactiveTimeseries.get("cpu")}
						formatter={(pct) => `${pct}%`}
					/>
					<LineChart
						title="Memory usage"
						timeseries={reactiveTimeseries.get("mem_kib")}
						formatter={kib => prettyBytes(kib * 1_024)}
					/>
					<LineChart
						title="Disk space usage"
						timeseries={reactiveTimeseries.get("disk_space_kib")}
						formatter={kib => prettyBytes(kib * 1_024)}
					/>
					<LineChart
						title="Space amplification"
						timeseries={reactiveTimeseries.get("space_amp")}
						formatter={(pct) => `${pct}x`}
					/>
					<LineChart
						title="Write latency"
						timeseries={reactiveTimeseries.get("write_latency")}
						formatter={formatNano}
					/>
					<LineChart
						title="Writes per second"
						timeseries={reactiveTimeseries.get("write_rate")}
						formatter={millify}
					/>
					<LineChart
						title="Disk write I/O"
						timeseries={reactiveTimeseries.get("disk_writes_kib")}
						formatter={kib => prettyBytes(kib * 1_024)}
					/>
					<LineChart
						title="Write amplification"
						timeseries={reactiveTimeseries.get("write_amp")}
						formatter={(pct) => `${pct}x`}
					/>
					<LineChart
						title="Point read latency"
						timeseries={reactiveTimeseries.get("point_read_latency")}
						formatter={formatNano}
					/>
					<LineChart
						title="Point reads per second"
						timeseries={reactiveTimeseries.get("point_read_rate")}
						formatter={millify}
					/>
					<LineChart
						title="Range latency"
						timeseries={reactiveTimeseries.get("range_latency")}
						formatter={formatNano}
					/>
					<LineChart
						title="Ranges per second"
						timeseries={reactiveTimeseries.get("range_rate")}
						formatter={millify}
					/>
					<LineChart
						title="Write ops (cumulative)"
						timeseries={reactiveTimeseries.get("write_potential")}
						formatter={millify}
					/>
					<LineChart
						title="Point read ops (cumulative)"
						timeseries={reactiveTimeseries.get("point_read_potential")}
						formatter={millify}
					/>
					<LineChart
						title="Range ops (cumulative)"
						timeseries={reactiveTimeseries.get("range_potential")}
						formatter={millify}
					/>

					{/* PERCENTILES */}
					<div class="p-2 bg-neutral-100 dark:bg-neutral-900 rounded">
						{(() => {
							return (
								<SolidApexCharts
									type="bar"
									width="100%"
									options={{
										title: {
											text: "write percentiles",
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
									series={percentiles.writePercentiles}
								/>
							);
						})()}
					</div>
					<div class="p-2 bg-neutral-100 dark:bg-neutral-900 rounded">
						{(() => {
							return (
								<SolidApexCharts
									type="bar"
									width="100%"
									options={{
										title: {
											text: "point read percentiles",
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
									series={percentiles.pointReadPercentiles}
								/>
							);
						})()}
					</div>
				</div>

				<div class="mt-3 flex flex-col gap-3">
					<div class="ml-2 cursor-pointer" onClick={() => toggleLsmStats((x) => !x)}>
						LSM-specific metrics
					</div>
					<Show when={showLsmStats()}>
						<div class="rounded-lg p-3 mx-3 dark:text-yellow-100 dark:bg-yellow-950">
							Only work for Fjall currently
						</div>
						<div class="grid md:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4 gap-2">
							<LineChart
								title="Bloom filter size"
								timeseries={reactiveTimeseries.get("bloom_filter_size")}
								formatter={prettyBytes}
							/>
							<LineChart
								title="# disk segments"
								timeseries={reactiveTimeseries.get("disk_segment_count")}
							/>
							<LineChart
								title="Average L0 segment lifetime"
								timeseries={reactiveTimeseries.get("l0_segment_avg_lifetime_ms")}
								formatter={(x) => `${x} ms`}
							/>
							<LineChart
								title="# active compactions"
								timeseries={reactiveTimeseries.get("running_compactions")}
							/>
						</div>
					</Show>
				</div>

				<div class="mt-3 flex flex-col gap-3">
					<div class="ml-2 cursor-pointer" onClick={() => toggleBtreeStats((x) => !x)}>B-tree-specific metrics</div>
					<Show when={showBtreeStats()}>
						<div class="rounded-lg p-3 mx-3 dark:text-yellow-100 dark:bg-yellow-950">
							Only work for Heed and ReDB currently
						</div>
						<div class="grid md:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4 gap-2">
							<LineChart
								title="Tree height"
								timeseries={reactiveTimeseries.get("tree_height")}
							/>
						</div>
					</Show>
				</div>
			</div>

			{/* TODO: copy button & generate report */}
			<div class="px-2 mb-10">
				<h2 class="ml-2 mb-3">Reproduce</h2>
				<div class="rounded-lg overflow-x-scroll p-2 font-mono whitespace-pre text-neutral-50 bg-neutral-950">
					<For each={setups()}>
						{(series) => (
							<div>
								{/* TODO: --out */}
								cargo run -r -- run{" "}
								{Object.entries(series.args)
									.filter(
										([_, value]) =>
											typeof value === "string" ||
											typeof value === "number" ||
											(typeof value === "boolean" && value),
									)
									.map(([key, value]) => {
										const kekabKey = key.replace(/_/g, "-");

										if (typeof value === "boolean") {
											return [`--${kekabKey}`].join(" ");
										}
										if (typeof value === "number") {
											return [`--${kekabKey}`, `${value}`].join(" ");
										}
										return [`--${kekabKey}`, `"${value}"`].join(" ");
									})
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
