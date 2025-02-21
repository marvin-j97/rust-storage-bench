import { ApexOptions } from "apexcharts";

const DEFAULT_Y_FORMATTER = (x: number) => (~~x).toString();

type Options = {
	yFormatter?: typeof DEFAULT_Y_FORMATTER;
	dashed?: number;
};

export function COMMON_CHART_OPTS(opts: Options) {
	return ({
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
				formatter: opts.yFormatter ?? DEFAULT_Y_FORMATTER,
			},
		},
	}) satisfies ApexOptions;
}
