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
	"#800000",
	"#9A6324",
	"#dcbeff",
	"#fabed4",
	"#ffd8b1",
	"#fffac8",
];

export function chooseColor(backendName: string): string {
	backendName = backendName.toLowerCase();

	if (backendName.includes("redb")) {
		return "#ffffff";
	}
	if (backendName.includes("sled")) {
		return "#e6194B";
	}
	if (backendName.includes("fjall_3")) {
		return "#42d4f4";
	}
	if (backendName === "fjall" || backendName === "fjall_2") {
		return "#4132ea";
	}
	if (backendName.includes("rocksdb")) {
		return "#ffe119";
	}
	if (backendName.includes("heed")) {
		return "#f58231";
	}
	if (backendName.includes("sqlite")) {
		return "#aaffc3";
	}
	if (backendName.includes("canopy")) {
		return "#d963df";
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

