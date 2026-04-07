import { ReactiveMap } from "@solid-primitives/map";
import { createSignal, onMount } from "solid-js";
import { createStore, produce } from "solid-js/store";

import { chooseColor, isLsm } from "./util";

import devData from "../log.jsonl?raw";
import devData2 from "../log2.jsonl.gzip?raw";
import devData3 from "../log3.jsonl.gzip?raw";
import devData4 from "../log4.jsonl.gzip?raw";

export type Setup = {
	displayName: string;
	args: any;
};

export type GroupedSeries = {
	name: string;
	color: string;
	data: number[];
};

export type TimeSeries = {
	displayName: string;
	colour: string;
	data: [number, number][];
};

type ColumnKey =
	| "time_ms"
	| "cpu"
	| "mem_kib"
	| "disk_space_kib"
	| "disk_writes_kib"
	| "disk_reads_kib"
	| "disk_table_count"
	| "data_block_io"
	| "index_block_io"
	| "filter_block_io"
	| "blob_file_count"
	| "journal_count"
	| "journal_size"
	| "filter_size"
	| "pinned_filter_size"
	| "pinned_block_index_size"
	| "cache_size"
	| "write_buffer_size"
	| "tree_height"
	| "fragmented_bytes"
	| "stale_blob_bytes"
	| "running_compactions"
	| "time_compacting_us"
	| "tombstone_count"
	| "l0_runs"
	| "l0_table_avg_lifetime_ms"
	| "filter_true_negative_ratio"
	| "block_cache_hit_rate"
	| "data_block_cache_hit_rate"
	| "index_block_cache_hit_rate"
	| "filter_block_cache_hit_rate"
	| "table_file_cache_hit_rate"
	| "write_ops"
	| "point_read_ops"
	| "range_ops"
	| "delete_ops"
	| "write_latency"
	| "point_read_latency"
	| "range_latency"
	| "delete_latency"
	| "write_rate"
	| "point_read_rate"
	| "range_rate"
	| "delete_rate"
	| "write_potential"
	| "point_read_potential"
	| "range_potential"
	| "delete_potential"
	| "write_amp"
	| "space_amp"
	| "read_amp";

const LSM_ONLY_PARAMETERS = new Set([
	"l0_table_avg_lifetime_ms",
	"bloom_filter_size",
	"block_index_size",
	"running_compactions",
	"time_compacting_us",
	"disk_table_count",
]);

const BTREE_ONLY_PARAMETERS = new Set([
	"tree_height",
	"fragmented_bytes",
]);

async function gunzip(text: string) {
	const binaryString = atob(text);
	const len = binaryString.length;
	const bytes = new Uint8Array(len);
	for (let i = 0; i < len; i++) {
		bytes[i] = binaryString.charCodeAt(i);
	}

	const ds = new DecompressionStream('gzip');
	const decompressedStream = new Response(
		new Blob([bytes]).stream().pipeThrough(ds)
	).body;

	const decompressedText = await new Response(decompressedStream).text();

	return decompressedText;
}

export function useMetricsData() {
	const [setups, setSetups] = createSignal<Setup[]>([]);

	const reactiveTimeseries = new ReactiveMap<ColumnKey, TimeSeries[]>();

	const [percentiles, setPercentiles] = createStore({
		writePercentiles: [] as GroupedSeries[],
		pointReadPercentiles: [] as GroupedSeries[],
		rangeReadPercentiles: [] as GroupedSeries[],
	});

	onMount(async () => {
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
        <script type="data" compressed="gzip">
					${devData2}
				</script>
        <script type="data" compressed="gzip">
					${devData3}
				</script>
        <script type="data" compressed="gzip">
					${devData4}
				</script>
        `;
			}
		}

		const setups: Setup[] = [];

		const els = document.querySelectorAll("script[type=data]");

		const backendTimeseries: Record<string, TimeSeries[]> = {};

		// NOTE: Load data
		for (let i = 0; i < els.length; i++) {
			const item = els[i];

			const isCompressed = item.getAttribute("compressed") ?? "false";

			let txt = item.textContent!.trim();
			if (isCompressed === "gzip") {
				txt = (await gunzip(txt)).trim();
			}

			const lines = txt.split("\n");
			const _system = JSON.parse(lines[0]);
			const args = JSON.parse(lines[1]);

			const color = args.color || chooseColor(args.backend);

			setups.push({
				displayName: args.display_name,
				args,
			});

			{
				const writeHistogram = lines.at(-3)!;
				const parsed = JSON.parse(writeHistogram) as {
					histogram: true;
					mean: number;
					p50: number;
					p90: number;
					p95: number;
					p99: number;
				};

				if (parsed.histogram) {
					const { mean, p50, p90, p95, p99 } = parsed;

					setPercentiles(
						produce((x) => {
							x.writePercentiles.push({
								data: [mean, p50, p90, p95, p99],
								name: args.display_name,
								color,
							});
						}),
					);
				}
			}

			{
				const pointReadHistogram = lines.at(-2)!;
				const parsed = JSON.parse(pointReadHistogram) as {
					histogram: true;
					mean: number;
					p50: number;
					p90: number;
					p95: number;
					p99: number;
				};

				if (parsed.histogram) {
					const { mean, p50, p90, p95, p99 } = parsed;

					setPercentiles(
						produce((x) => {
							x.pointReadPercentiles.push({
								data: [mean, p50, p90, p95, p99],
								name: args.display_name,
								color,
							});
						}),
					);
				}
			}

			{
				const rangeReadHistogram = lines.at(-1)!;
				const parsed = JSON.parse(rangeReadHistogram) as {
					histogram: true;
					mean: number;
					p50: number;
					p90: number;
					p95: number;
					p99: number;
				};

				if (parsed.histogram) {
					const { mean, p50, p90, p95, p99 } = parsed;

					setPercentiles(
						produce((x) => {
							x.rangeReadPercentiles.push({
								data: [mean, p50, p90, p95, p99],
								name: args.display_name,
								color,
							});
						}),
					);
				}
			}

			const columnNames = (JSON.parse(lines[2]) as string[]);

			for (const name of columnNames) {
				reactiveTimeseries.set(name as ColumnKey, []);
			}

			const timeseries: Partial<Record<ColumnKey, TimeSeries>> = {};

			for (const line of lines.slice(3, -4)) {
				const metrics = JSON.parse(line) as number[];

				// IMPORTANT: Skip first column because it is time_ms
				for (let j = 1; j < columnNames.length; j++) {
					const name = columnNames[j] as ColumnKey;

					if (!isLsm(args.backend) && LSM_ONLY_PARAMETERS.has(name)) {
						continue;
					}
					if (isLsm(args.backend) && BTREE_ONLY_PARAMETERS.has(name)) {
						continue;
					}

					const [ts] = metrics;

					if (!timeseries[name]) {
						timeseries[name] = {
							data: [],
							displayName: args.display_name,
							colour: color,
						};
					}
					timeseries[name].data.push([ts, metrics[j]]);
				}
			}

			for (const columnKey in timeseries) {
				const series = timeseries[columnKey as ColumnKey]!;
				const prev = backendTimeseries[columnKey];

				if (prev) {
					backendTimeseries[columnKey].push(series);
				} else {
					backendTimeseries[columnKey] = [series];
				}
			}
		}

		for (const columnKey in backendTimeseries) {
			reactiveTimeseries.set(
				columnKey as ColumnKey,
				backendTimeseries[columnKey],
			);
		}

		// TODO: file input if there are no embedded metrics file

		setSetups(setups);
	});

	return {
		setups,
		reactiveTimeseries,
		percentiles,
	};
}
