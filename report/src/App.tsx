import millify from "millify";
import { createSignal, For, Show } from "solid-js";
import prettyBytes from "pretty-bytes";

import { SolidApexCharts } from "./SolidApex";
import { formatNano, formatThousands } from "./util";
import { useMetricsData } from "./data";
import { COMMON_CHART_OPTS } from "./chart";

function App() {
	const [showLsmStats, toggleLsmStats] = createSignal(true);
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
					<div class="p-2 bg-neutral-100 dark:bg-neutral-900 rounded">
						{(() => {
							const series = () =>
								reactiveTimeseries.get("cpu")!.map((series) => {
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
										...COMMON_CHART_OPTS({
											yFormatter: (pct) => `${pct}%`,
											dashed: 0,
										}),
									}}
									series={series()}
								/>
							);
						})()}
					</div>
					<div class="p-2 bg-neutral-100 dark:bg-neutral-900 rounded">
						{(() => {
							const series = () =>
								reactiveTimeseries.get("mem_kib")!.map((series) => {
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
										...COMMON_CHART_OPTS({
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
					<div class="p-2 bg-neutral-100 dark:bg-neutral-900 rounded">
						{(() => {
							const series = () =>
								reactiveTimeseries.get("disk_space_kib")!.map((series) => {
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
										...COMMON_CHART_OPTS({
											yFormatter: prettyBytes,
											dashed: 0,
										}),
									}}
									series={series()}
								/>
							);
						})()}
					</div>
					<div class="p-2 bg-neutral-100 dark:bg-neutral-900 rounded">
						{(() => {
							const series = () =>
								reactiveTimeseries.get("space_amp")!.map((series) => {
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
											text: "space amplification",
											style: {
												color: "white",
											},
										},
										...COMMON_CHART_OPTS({
											yFormatter: (pct) => `${pct}x`,
											dashed: 0,
										}),
									}}
									series={series()}
								/>
							);
						})()}
					</div>
					<div class="p-2 bg-neutral-100 dark:bg-neutral-900 rounded">
						{(() => {
							const series = () =>
								reactiveTimeseries.get("write_latency")!.map((series) => {
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
											text: "write latency",
											style: {
												color: "white",
											},
										},
										...COMMON_CHART_OPTS({
											yFormatter: formatNano,
											dashed: 0,
										}),
									}}
									series={series()}
								/>
							);
						})()}
					</div>
					<div class="p-2 bg-neutral-100 dark:bg-neutral-900 rounded">
						{(() => {
							const series = () =>
								reactiveTimeseries.get("write_rate")!.map((series) => {
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
										...COMMON_CHART_OPTS({
											yFormatter: millify,
											dashed: 0,
										}),
									}}
									series={series()}
								/>
							);
						})()}
					</div>
					<div class="p-2 bg-neutral-100 dark:bg-neutral-900 rounded">
						{(() => {
							const series = () =>
								reactiveTimeseries.get("disk_writes_kib")!.map((series) => {
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
										...COMMON_CHART_OPTS({
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
					<div class="p-2 bg-neutral-100 dark:bg-neutral-900 rounded">
						{(() => {
							const series = () =>
								reactiveTimeseries.get("write_amp")!.map((series) => {
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
										...COMMON_CHART_OPTS({
											yFormatter: (pct) => `${pct}x`,
											dashed: 0,
										}),
									}}
									series={series()}
								/>
							);
						})()}
					</div>
					<div class="p-2 bg-neutral-100 dark:bg-neutral-900 rounded">
						{(() => {
							const series = () =>
								reactiveTimeseries.get("point_read_latency")!.map((series) => {
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
											text: "point read latency",
											style: {
												color: "white",
											},
										},
										...COMMON_CHART_OPTS({
											yFormatter: formatNano,
											dashed: 0,
										}),
									}}
									series={series()}
								/>
							);
						})()}
					</div>
					<div class="p-2 bg-neutral-100 dark:bg-neutral-900 rounded">
						{(() => {
							const series = () =>
								reactiveTimeseries.get("point_read_rate")!.map((series) => {
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
											text: "point reads per second",
											style: {
												color: "white",
											},
										},
										...COMMON_CHART_OPTS({
											yFormatter: millify,
											dashed: 0,
										}),
									}}
									series={series()}
								/>
							);
						})()}
					</div>
					<div class="p-2 bg-neutral-100 dark:bg-neutral-900 rounded">
						{(() => {
							const series = () =>
								reactiveTimeseries.get("range_latency")!.map((series) => {
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
											text: "range latency",
											style: {
												color: "white",
											},
										},
										...COMMON_CHART_OPTS({
											yFormatter: formatNano,
											dashed: 0,
										}),
									}}
									series={series()}
								/>
							);
						})()}
					</div>
					<div class="p-2 bg-neutral-100 dark:bg-neutral-900 rounded">
						{(() => {
							const series = () =>
								reactiveTimeseries.get("range_rate")!.map((series) => {
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
											text: "ranges per second",
											style: {
												color: "white",
											},
										},
										...COMMON_CHART_OPTS({
											yFormatter: millify,
											dashed: 0,
										}),
									}}
									series={series()}
								/>
							);
						})()}
					</div>
					<div class="p-2 bg-neutral-100 dark:bg-neutral-900 rounded">
						{(() => {
							const series = () =>
								reactiveTimeseries.get("write_potential")!.map((series) => {
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
										...COMMON_CHART_OPTS({
											yFormatter: millify,
											dashed: 0,
										}),
									}}
									series={series()}
								/>
							);
						})()}
					</div>
					<div class="p-2 bg-neutral-100 dark:bg-neutral-900 rounded">
						{(() => {
							const series = () =>
								reactiveTimeseries
									.get("point_read_potential")!
									.map((series) => {
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
										...COMMON_CHART_OPTS({
											yFormatter: millify,
											dashed: 0,
										}),
									}}
									series={series()}
								/>
							);
						})()}
					</div>
					<div class="p-2 bg-neutral-100 dark:bg-neutral-900 rounded">
						{(() => {
							const series = () =>
								reactiveTimeseries.get("range_potential")!.map((series) => {
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
											text: "range ops (cumulative)",
											style: {
												color: "white",
											},
										},
										...COMMON_CHART_OPTS({
											yFormatter: millify,
											dashed: 0,
										}),
									}}
									series={series()}
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
					<div class="ml-2" onClick={() => toggleLsmStats((x) => !x)}>
						LSM-specific metrics
					</div>
					<Show when={showLsmStats()}>
						<div class="grid md:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4 gap-2">
							<div class="p-2 bg-neutral-100 dark:bg-neutral-900 rounded">
								{(() => {
									const series = () =>
										reactiveTimeseries
											.get("bloom_filter_size")!
											.map((series) => {
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
													text: "Bloom filter size",
													style: {
														color: "white",
													},
												},
												...COMMON_CHART_OPTS({
													yFormatter: prettyBytes,
													dashed: 0,
												}),
											}}
											series={series()}
										/>
									);
								})()}
							</div>
							<div class="p-2 bg-neutral-100 dark:bg-neutral-900 rounded">
								{(() => {
									const series = () =>
										reactiveTimeseries
											.get("disk_segment_count")!
											.map((series) => {
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
													text: "# disk segments",
													style: {
														color: "white",
													},
												},
												...COMMON_CHART_OPTS({
													yFormatter: (x) => x.toString(),
													dashed: 0,
												}),
											}}
											series={series()}
										/>
									);
								})()}
							</div>
							<div class="p-2 bg-neutral-100 dark:bg-neutral-900 rounded">
								{(() => {
									const series = () =>
										reactiveTimeseries
											.get("l0_segment_avg_lifetime_ms")!
											.map((series) => {
												return {
													name: series.displayName,
													data: series.data.map(([ts_milli, value]) => ({
														x: ts_milli / 1_000,
														y: Math.min(value, /* 30sec */ 30_000),
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
													text: "average L0 segment lifetime",
													style: {
														color: "white",
													},
												},
												...COMMON_CHART_OPTS({
													yFormatter: (x) => `${x} ms`,
													dashed: 0,
												}),
											}}
											series={series()}
										/>
									);
								})()}
							</div>
							<div class="p-2 bg-neutral-100 dark:bg-neutral-900 rounded">
								{(() => {
									const series = () =>
										reactiveTimeseries
											.get("running_compactions")!
											.map((series) => {
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
													text: "# active compactions",
													style: {
														color: "white",
													},
												},
												...COMMON_CHART_OPTS({
													yFormatter: (x) => x.toString(),
													dashed: 0,
												}),
											}}
											series={series()}
										/>
									);
								})()}
							</div>
						</div>
					</Show>
				</div>

				<div class="mt-3 flex flex-col gap-3">
					<div class="ml-2">B-tree-specific metrics</div>
					<div class="grid md:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4 gap-2">
						<div class="p-2 bg-neutral-100 dark:bg-neutral-900 rounded">
							{(() => {
								const series = () =>
									reactiveTimeseries.get("tree_height")!.map((series) => {
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
												text: "Tree depth",
												style: {
													color: "white",
												},
											},
											...COMMON_CHART_OPTS({
												yFormatter: (x) => x.toString(),
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
