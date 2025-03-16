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

export function chooseColor(backend: string): string {
	if (backend.includes("redb")) {
		return "#ffffff";
	}
	if (backend.includes("sled")) {
		return "#ee5555";
	}
	if (backend.includes("localfjall")) {
		return "#4455FF";
	}
	if (backend.includes("fjall")) {
		return "#38bdf8";
	}
	if (backend.includes("rocksdb")) {
		return "#fbbf24";
	}
	if (backend.includes("heed")) {
		return "#ff7700";
	}
	if (backend.includes("sqlite")) {
		return "#00ffdd";
	}
	if (backend.includes("canopy")) {
		return "#f472b6";
	}
	return "#99aabb"
}

export function isLsm(backend: string): boolean {
	if (backend.includes("redb")) {
		return false;
	}
	if (backend.includes("sled")) {
		return false;
	}
	if (backend.includes("localfjall")) {
		return true;
	}
	if (backend.includes("fjall")) {
		return true;
	}
	if (backend.includes("rocksdb")) {
		return true;
	}
	if (backend.includes("heed")) {
		return false;
	}
	if (backend.includes("sqlite")) {
		return false;
	}
	return false;
}

