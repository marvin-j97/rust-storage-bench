import { createScheduled, debounce } from '@solid-primitives/scheduled';
import millify from "millify";
import prettyBytes from "pretty-bytes";
import { createMemo, createSignal, For, Show } from "solid-js";

import BoxPlotChart from './BoxPlotChart';
import { COMMON_CHART_OPTS } from './chart';
import { useMetricsData } from "./data";
import LineChart from "./LineChart";
import { SolidApexCharts } from './SolidApex';
import { formatNano } from "./util";
import PercentileChart from './PercentileChart';

function throttledSignal<T>(value: T, delay: number) {
	const [signal, set] = createSignal(value);
	const scheduled = createScheduled((fn) =>
		debounce(fn, delay)
	);
	const throttled = createMemo<T>((prev) => {
		const next = signal();
		return scheduled() ? next : prev;
	}, value);

	return [signal, set, throttled] as const;
}


function App() {
	const [showLsmStats, toggleLsmStats] = createSignal(true);
	const [showBtreeStats, toggleBtreeStats] = createSignal(true);
	const [smoothingLevel, setSmoothingLevel, debouncedSmoothingLevel] = throttledSignal(0, 250);

	const { percentiles, reactiveTimeseries, setups, markers } = useMetricsData(debouncedSmoothingLevel);

	return (
		<div class="flex flex-col gap-5">
			{/* topbar */}
			<div class="p-2 border-b border-neutral-200 dark:border-neutral-800">
				<h1 class="text-sm">rust-storage-bench 1.0.0</h1>
			</div>

			<div class="px-2">
				<h2 class="text-sm">Time series smoothing: {smoothingLevel()}</h2>
				<input
					type="range"
					min={0}
					max={100}
					value={smoothingLevel()}
					onInput={ev => setSmoothingLevel(ev.currentTarget.valueAsNumber)}
				/>
			</div>

			{/* content */}
			<div class="px-2">
				<h2 class="text-lg mb-3">Results</h2>

				{/* graphs */}
				<div class="grid gap-2" style="grid-template-columns: repeat(auto-fill, minmax(480px, 1fr))">
					{/* LINE CHARTS */}
					<LineChart
						title="CPU usage"
						timeseries={reactiveTimeseries.get("cpu")}
						formatter={(pct) => `${pct}%`}
						markers={markers()}
					/>
					<LineChart
						title="Memory usage"
						timeseries={reactiveTimeseries.get("mem_kib")}
						formatter={kib => prettyBytes(kib * 1_024)}
						markers={markers()}
					/>
					<LineChart
						title="Disk space usage"
						timeseries={reactiveTimeseries.get("disk_space_kib")}
						formatter={kib => prettyBytes(kib * 1_024)}
						markers={markers()}
						filterZeroValues
					/>
					<LineChart
						title="Space amplification"
						timeseries={reactiveTimeseries.get("space_amp")}
						formatter={(pct) => `${pct}x`}
						markers={markers()}
						filterZeroValues
					/>
					<LineChart
						title="Write latency"
						timeseries={reactiveTimeseries.get("write_latency")}
						formatter={formatNano}
						markers={markers()}
					/>
					<LineChart
						title="Writes per second"
						timeseries={reactiveTimeseries.get("write_rate")}
						formatter={millify}
						markers={markers()}
					/>
					<LineChart
						title="Disk write I/O"
						timeseries={reactiveTimeseries.get("disk_writes_kib")}
						formatter={kib => prettyBytes(kib * 1_024)}
						markers={markers()}
					/>
					<LineChart
						title="Write amplification"
						timeseries={reactiveTimeseries.get("write_amp")}
						formatter={(pct) => `${pct}x`}
						markers={markers()}
					/>
					<LineChart
						title="Disk read I/O"
						timeseries={reactiveTimeseries.get("disk_reads_kib")}
						formatter={kib => prettyBytes(kib * 1_024)}
						markers={markers()}
					/>
					<LineChart
						title="Read amplification"
						timeseries={reactiveTimeseries.get("read_amp")}
						formatter={(pct) => `${pct}x`}
						markers={markers()}
					/>
					<LineChart
						title="Point read latency"
						timeseries={reactiveTimeseries.get("point_read_latency")}
						formatter={formatNano}
						markers={markers()}
					/>
					<LineChart
						title="Point reads per second"
						timeseries={reactiveTimeseries.get("point_read_rate")}
						formatter={millify}
						markers={markers()}
					/>
					<LineChart
						title="Range latency"
						timeseries={reactiveTimeseries.get("range_latency")}
						formatter={formatNano}
						markers={markers()}
					/>
					<LineChart
						title="Ranges per second"
						timeseries={reactiveTimeseries.get("range_rate")}
						formatter={millify}
						markers={markers()}
					/>
					<LineChart
						title="Delete latency"
						timeseries={reactiveTimeseries.get("delete_latency")}
						formatter={formatNano}
						markers={markers()}
					/>
					<LineChart
						title="Deletes per second"
						timeseries={reactiveTimeseries.get("delete_rate")}
						formatter={millify}
						markers={markers()}
					/>
					<LineChart
						title="Write ops (cumulative)"
						timeseries={reactiveTimeseries.get("write_ops")}
						formatter={millify}
						markers={markers()}
					/>
					<LineChart
						title="Point read ops (cumulative)"
						timeseries={reactiveTimeseries.get("point_read_ops")}
						formatter={millify}
						markers={markers()}
					/>
					<LineChart
						title="Range ops (cumulative)"
						timeseries={reactiveTimeseries.get("range_ops")}
						formatter={millify}
						markers={markers()}
					/>
					<LineChart
						title="Delete ops (cumulative)"
						timeseries={reactiveTimeseries.get("delete_ops")}
						formatter={millify}
						markers={markers()}
					/>
				</div>

				{/* lsm stats */}
				<div class="mt-3 flex flex-col gap-3">
					<div class="ml-2 cursor-pointer" onClick={() => toggleLsmStats((x) => !x)}>
						LSM-specific metrics
					</div>
					<Show when={showLsmStats()}>
						<div class="grid gap-2" style="grid-template-columns: repeat(auto-fill, minmax(480px, 1fr))">
							<LineChart
								markers={markers()}
								title="Write buffer size"
								timeseries={reactiveTimeseries.get("write_buffer_size")}
								formatter={prettyBytes}
							/>
							<LineChart
								markers={markers()}
								title="# disk tables"
								timeseries={reactiveTimeseries.get("disk_table_count")}
							/>
							<LineChart
								markers={markers()}
								title="# blob files"
								timeseries={reactiveTimeseries.get("blob_file_count")}
							/>
							<LineChart
								markers={markers()}
								title="# active compactions"
								timeseries={reactiveTimeseries.get("running_compactions")}
							/>
							<LineChart
								markers={markers()}
								title="Fragmented blob bytes"
								timeseries={reactiveTimeseries.get("stale_blob_bytes")}
								formatter={prettyBytes}
							/>
							{/* TODO: move somewhere else if redb gets cache size stats */}
							<LineChart
								markers={markers()}
								title="Block cache size"
								timeseries={reactiveTimeseries.get("cache_size")}
								formatter={prettyBytes}
							/>
							<LineChart
								markers={markers()}
								title="Block cache hit rate"
								timeseries={reactiveTimeseries.get("block_cache_hit_rate")}
								formatter={x => `${(x * 100.0).toFixed(1)}%`}
							/>
							<LineChart
								markers={markers()}
								title="Data block cache hit rate"
								timeseries={reactiveTimeseries.get("data_block_cache_hit_rate")}
								formatter={x => `${(x * 100.0).toFixed(1)}%`}
							/>
							<LineChart
								markers={markers()}
								title="Index block cache hit rate"
								timeseries={reactiveTimeseries.get("index_block_cache_hit_rate")}
								formatter={x => `${(x * 100.0).toFixed(1)}%`}
							/>
						</div>
						<div class="rounded-lg p-3 mx-3 dark:text-yellow-100 dark:bg-yellow-950">
							Only work for Fjall currently
						</div>
						<div class="grid gap-2" style="grid-template-columns: repeat(auto-fill, minmax(480px, 1fr))">
							<LineChart
								markers={markers()}
								title="Filter size (on disk)"
								timeseries={reactiveTimeseries.get("filter_size")}
								formatter={prettyBytes}
							/>
							<LineChart
								markers={markers()}
								title="(Pinned) filter size"
								timeseries={reactiveTimeseries.get("pinned_filter_size")}
								formatter={prettyBytes}
							/>
							<LineChart
								markers={markers()}
								title="(Pinned) block index size"
								timeseries={reactiveTimeseries.get("pinned_block_index_size")}
								formatter={prettyBytes}
							/>
							<LineChart
								markers={markers()}
								title="L0 runs"
								timeseries={reactiveTimeseries.get("l0_runs")}
							/>
							<LineChart
								markers={markers()}
								title="Average L0 table lifetime"
								timeseries={reactiveTimeseries.get("l0_table_avg_lifetime_ms")}
								formatter={(x) => `${x} ms`}
							/>
							<LineChart
								markers={markers()}
								title="Journal count"
								timeseries={reactiveTimeseries.get("journal_count")}
							/>
							<LineChart
								markers={markers()}
								title="Journal size"
								timeseries={reactiveTimeseries.get("journal_size")}
								formatter={prettyBytes}
							/>
							<LineChart
								markers={markers()}
								title="# tombstones"
								timeseries={reactiveTimeseries.get("tombstone_count")}
								formatter={millify}
							/>
							{/* TODO: seems to be buggy in RocksDB */}
							<LineChart
								markers={markers()}
								title="Filter block cache hit rate"
								timeseries={reactiveTimeseries.get("filter_block_cache_hit_rate")}
								formatter={x => `${(x * 100.0).toFixed(1)}%`}
							/>
							<LineChart
								markers={markers()}
								title="Filter true negative rate"
								timeseries={reactiveTimeseries.get("filter_true_negative_ratio")}
								formatter={x => `${(x * 100.0).toFixed(1)}%`}
							/>
							<LineChart
								markers={markers()}
								title="Table file cache hit rate"
								timeseries={reactiveTimeseries.get("table_file_cache_hit_rate")}
								formatter={x => `${(x * 100.0).toFixed(1)}%`}
							/>

							<LineChart
								markers={markers()}
								title="Data block I/O"
								timeseries={reactiveTimeseries.get("data_block_io")}
								formatter={prettyBytes}
							/>
							<LineChart
								markers={markers()}
								title="Index block I/O"
								timeseries={reactiveTimeseries.get("index_block_io")}
								formatter={prettyBytes}
							/>
							<LineChart
								markers={markers()}
								title="Filter block I/O"
								timeseries={reactiveTimeseries.get("filter_block_io")}
								formatter={prettyBytes}
							/>
						</div>
					</Show>
				</div>

				{/* b-tree stats */}
				<div class="mt-3 flex flex-col gap-3">
					<div class="ml-2 cursor-pointer" onClick={() => toggleBtreeStats((x) => !x)}>B-tree-specific metrics</div>
					<Show when={showBtreeStats()}>
						<div class="rounded-lg p-3 mx-3 dark:text-yellow-100 dark:bg-yellow-950">
							Only work for Heed and ReDB currently
						</div>
						<div class="grid md:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4 gap-2">
							<LineChart
								markers={markers()}
								title="Tree height"
								timeseries={reactiveTimeseries.get("tree_height")}
							/>
						</div>

						<div class="rounded-lg p-3 mx-3 dark:text-yellow-100 dark:bg-yellow-950">
							Only work for ReDB currently
						</div>
						<div class="grid md:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4 gap-2">
							<LineChart
								markers={markers()}
								title="Fragmented bytes"
								timeseries={reactiveTimeseries.get("fragmented_bytes")}
								formatter={prettyBytes}
							/>
						</div>
					</Show>
				</div>

				{/* percentiles */}
				<div class="mt-3 flex flex-col gap-3">
					<div class="ml-2 cursor-pointer">
						Percentiles
					</div>

					{/* TODO: apex charts does not allow disabling min/max... making box plots useless */}
					<div class="grid gap-2" style="grid-template-columns: repeat(auto-fill, minmax(480px, 1fr))">
						<div class="flex flex-col gap-2">
							<BoxPlotChart title="Write percentiles" percentiles={percentiles.writePercentiles} />
							<PercentileChart title="Write percentiles" percentiles={percentiles.writePercentiles} />
						</div>
						<div class="flex flex-col gap-2">
							<BoxPlotChart title="Point read percentiles" percentiles={percentiles.pointReadPercentiles} />
							<PercentileChart title="Point read percentiles" percentiles={percentiles.pointReadPercentiles} />
						</div>
						<div class="flex flex-col gap-2">
							<BoxPlotChart title="Range read percentiles" percentiles={percentiles.rangeReadPercentiles} />
							<PercentileChart title="Range read percentiles" percentiles={percentiles.rangeReadPercentiles} />
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
								{series.args.cmd.join(" ")}
							</div>
						)}
					</For>
				</div>
			</div>
		</div>
	);
}

export default App;
