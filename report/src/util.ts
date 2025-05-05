const thousandsFormatter = Intl.NumberFormat(undefined, {
	maximumFractionDigits: 1,
});

export const formatThousands = (n: number) => thousandsFormatter.format(n);

export function formatNano(nanos: number): string {
	if (nanos < 1_000) {
		return `${formatThousands(nanos)}ns`;
	}
	if (nanos < 1_000_000) {
		return `${(nanos / 1_000).toFixed(1)}µs`;
	}
	return `${(nanos / 1_000 / 1_000).toFixed(1)}ms`;
}

const fallbackColors = [
	"#ffffff",
	"#ee5555",
	"#4455FF",
	"#38bdf8",
	"#fbbf24",
	"#ff7700",
	"#00ffdd",
	"#f472b6",
	"#99aabb",
];

export function chooseColor(backendName: string): string {
	if (backendName.includes("redb")) {
		return "#ffffff";
	}
	if (backendName.includes("sled")) {
		return "#ee5555";
	}
	if (backendName.includes("local")) {
		return "#4455FF";
	}
	if (backendName.includes("fjall")) {
		return "#38bdf8";
	}
	if (backendName.includes("rocksdb")) {
		return "#fbbf24";
	}
	if (backendName.includes("heed")) {
		return "#ff7700";
	}
	if (backendName.includes("sqlite")) {
		return "#00ffdd";
	}
	if (backendName.includes("canopy")) {
		return "#f472b6";
	}
	if (backendName.includes("leveldb")) {
		return "#4499ff";
	}
	const code = [...backendName]
		.reduce((acc, c) => acc + c.charCodeAt(0), backendName.length);

	return fallbackColors[code % fallbackColors.length];
}

export function isLsm(backend: string): boolean {
	if (backend.includes("fjall")) {
		return true;
	}
	if (backend.includes("rocksdb")) {
		return true;
	}
	if (backend.includes("leveldb")) {
		return true;
	}
	return false;
}

