import { ApexOptions } from "apexcharts";

const DEFAULT_Y_FORMATTER = (x: number) => (~~x).toString();

export const COMMON_CHART_OPTS = (
	opts = {
		yFormatter: DEFAULT_Y_FORMATTER,
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
			background: "#171717",
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
