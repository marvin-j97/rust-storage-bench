import { ReactiveMap } from "@solid-primitives/map";
import { Accessor, createEffect, createSignal } from "solid-js";
import { createStore, produce } from "solid-js/store";

import { chooseColor, isLsm } from "./util";

import devData1 from "../log1.jsonl?raw";
import devData2 from "../log2.jsonl?raw";
import devData3 from "../log3.jsonl?raw";

export type HistogramData = {
	histogram: true;
	min: number;
	max: number;
	mean: number;
	p25: number;
	p50: number;
	p75: number;
	p90: number;
	p95: number;
	p99: number;
};

export type GroupedHistograms = {
	name: string;
	color: string;
	data: HistogramData;
};

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

function smoothTimeseries(data: [number, number][], windowSize: number): [number, number][] {
	if (data.length <= 2 || windowSize <= 1) {
		return data;
	}

	const smoothedData: [number, number][] = [];

	// Always add the first data point
	smoothedData.push(data[0]);

	let window = [];

	for (let i = 0; i < (data.length - 1); i++) {
		window.push(data[i]);

		if (window.length >= windowSize) {
			const sum = window.reduce((acc, [_, x]) => acc + x, 0);
			const avg = sum / windowSize;
			const timestamp = window[0][0] /* (window[0][0] + window.at(-1)![0]) / 2 */;
			smoothedData.push([timestamp, avg]);

			window = [];
		}
	}

	// Always add the last data point
	smoothedData.push(data.at(-1)!);

	return smoothedData;
}

function cleanupTimeseries(data: [number, number][]): [number, number][] {
	// return data;

	if (data.length <= 1) {
		return data;
	}

	const cleanedData: [number, number][] = [data[0]];

	for (let i = 1; i < data.length; i++) {
		const currentValue = data[i][1];
		const previousValue = data[i - 1][1];

		// If the current value is different from the previous value,
		// it signifies the end of a sequence of the previous value (or the start of a new one).
		// We always want to keep the first point of a new value sequence.
		if (currentValue !== previousValue) {
			// Before adding the new point, if the previous point added to `cleanedData`
			// has the same value as the point *before* the current one in the original data,
			// it means we skipped some points in the original data. In this case, we need
			// to add the *last* point of the previous sequence before adding the current one.
			const lastCleanedValue = cleanedData[cleanedData.length - 1][1];
			if (lastCleanedValue === previousValue && i > 1) {
				// Check if the point before the current one in the original data
				// was part of the previous sequence of identical values.
				if (data[i - 2] && data[i - 2][1] === previousValue) {
					cleanedData.push(data[i - 1]); // Add the last point of the previous sequence
				}
			}

			cleanedData.push(data[i]); // Add the current point (the start of a new sequence)

		} else {
			// If the current value is the same as the previous, and it's the last element
			// in the original data, we need to add it as it's the end of a sequence.
			if (i === data.length - 1) {
				cleanedData.push(data[i]);
			}
		}
	}

	// A final check to ensure the very last point of the original data is included
	// if it wasn't already added as the start of a new sequence.
	if (cleanedData[cleanedData.length - 1] !== data[data.length - 1] && data.length > 0) {
		// Check if the last point is a duplicate of the second to last point in the cleaned data
		if (cleanedData.length > 1 && cleanedData[cleanedData.length - 1][1] === data[data.length - 1][1]) {
			// If it is, and the second to last point in the *original* data was the same value
			// as the last point in the *cleaned* data, it means we need to add the true last point
			// to ensure we have the end of the last sequence.
			if (data[data.length - 2] && data[data.length - 2][1] === cleanedData[cleanedData.length - 1][1]) {
				cleanedData.push(data[data.length - 1]);
			}
		} else {
			// If the last point in original data is not a duplicate of the last point in cleaned data,
			// or if cleanedData only has one point, always add the last original point.
			cleanedData.push(data[data.length - 1]);
		}
	}

	return cleanedData;
}

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

export function useMetricsData(smoothing: Accessor<number>) {
	const [setups, setSetups] = createSignal<Setup[]>([]);

	const reactiveTimeseries = new ReactiveMap<ColumnKey, TimeSeries[]>();

	const [percentiles, setPercentiles] = createStore({
		writePercentiles: [] as GroupedHistograms[],
		pointReadPercentiles: [] as GroupedHistograms[],
		rangeReadPercentiles: [] as GroupedHistograms[],
	});

	const [markers, setMarkers] = createSignal<(MarkerShapeOptions | null)[]>([]);

	createEffect(async () => {
		// NOTE: Patch HTML with dev data
		if (import.meta.env.DEV) {
			console.log("hello dev");

			const dataContainer = document.querySelector("#data-container")!;

			if (
				[...dataContainer.childNodes.values()].every((x) => x.nodeType !== 1)
			) {
				dataContainer.innerHTML += `
				<script type="data" compressed="false">
					${devData1}
				</script>
        <script type="data" compressed="false">
					${devData2}
				</script>
        <script type="data" compressed="false">
					${devData3}
				</script>
        `;
			}
		}

		setPercentiles({
			pointReadPercentiles: [],
			rangeReadPercentiles: [],
			writePercentiles: [],
		});

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

			if (args.marker_shape) {
				setMarkers(prev => [...prev, args.marker_shape]);
			}
			else {
				setMarkers(prev => [...prev, null]);
			}

			{
				const writeHistogram = lines.at(-3)!;
				const parsed = JSON.parse(writeHistogram) as HistogramData;

				if (parsed.histogram) {
					setPercentiles(
						produce((x) => {
							x.writePercentiles.push({
								data: parsed,
								name: args.display_name,
								color,
							});
						}),
					);
				}
			}

			{
				const pointReadHistogram = lines.at(-2)!;
				const parsed = JSON.parse(pointReadHistogram) as HistogramData;

				if (parsed.histogram) {
					setPercentiles(
						produce((x) => {
							x.pointReadPercentiles.push({
								data: parsed,
								name: args.display_name,
								color,
							});
						}),
					);
				}
			}

			{
				const rangeReadHistogram = lines.at(-1)!;
				const parsed = JSON.parse(rangeReadHistogram) as HistogramData;

				if (parsed.histogram) {
					setPercentiles(
						produce((x) => {
							x.rangeReadPercentiles.push({
								data: parsed,
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

				if (smoothing() > 1) {
					series.data = smoothTimeseries(series.data, smoothing());
				}
				series.data = cleanupTimeseries(series.data);

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
		markers,
	};
}
