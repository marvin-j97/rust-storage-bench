import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { existsSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { rm } from "node:fs/promises";
import { resolve } from "node:path";

import { Spinner } from "@topcli/spinner";
import chalk from "chalk";
import asyncPool from "tiny-async-pool";

import combinate from "./combinate.mjs";

const dataDir = "/devssd/code/rust/rust-storage-bench/.data";
// const dataDir = ".data";

if (existsSync(dataDir)) {
  rmSync(dataDir, {
    recursive: true,
  });
}

const PARALLELISM = 1;

const BACKENDS = [
  {
    name: "fjall",
    canSync: true,
    canNoSync: true,
    isLsm: true,
  },
  {
    name: "persy",
    canSync: true,
    canNoSync: true,
    isLsm: false,
  },
  {
    name: "redb",
    canSync: true,
    canNoSync: true,
    isLsm: false,
  },
  {
    name: "sled",
    canSync: false,
    canNoSync: true,
    isLsm: false,
  },
  {
    name: "bloodstone",
    canSync: false,
    canNoSync: true,
    isLsm: false,
  },
  {
    name: "jamm-db",
    canSync: true,
    canNoSync: false,
    isLsm: false,
  },
  {
    name: "nebari",
    canSync: true,
    canNoSync: false,
    isLsm: false,
  },
  {
    name: "heed",
    canSync: true,
    canNoSync: true,
    isLsm: false,
  },
  {
    name: "rocks-db",
    canSync: true,
    canNoSync: true,
    isLsm: true,
  },
];

const config = {
  args: {
    task: ["monotonic-fixed-random"],
    items: [100_000_000],
    minutes: [1],
    threads: [1],
    fsync: [false],
    backend: ["fjall"],
    valueSize: [100],
    cacheSize: [4_000_000],
    lsmBlockSize: [undefined, 4_096],
    lsmKvSeparation: [undefined, false],
    lsmCompression: [undefined, "lz4"],
    lsmCompaction: [undefined, /* "tiered", */ "leveled"],
  },
  formatDisplayName: ((args) =>
    [args.backend, args.lsmKvSeparation ? "blob" : ""]
      .filter(Boolean)
      .map(String)
      .join(" ")) satisfies (args: (typeof config)["args"]) => string,
};

const benchPermutations = combinate(config.args).filter((cfg) => {
  const backend = BACKENDS.find((x) => x.name === cfg.backend)!;

  if (cfg.fsync && !backend.canSync) {
    return false;
  }
  if (!cfg.fsync && !backend.canNoSync) {
    return false;
  }
  if (!backend.isLsm && cfg.lsmBlockSize) {
    return false;
  }
  if (backend.isLsm && !cfg.lsmBlockSize) {
    return false;
  }
  if (!backend.isLsm && typeof cfg.lsmKvSeparation === "boolean") {
    return false;
  }
  if (backend.isLsm && typeof cfg.lsmKvSeparation === "undefined") {
    return false;
  }
  if (!backend.isLsm && cfg.lsmCompression) {
    return false;
  }
  if (backend.isLsm && !cfg.lsmCompression) {
    return false;
  }
  if (!backend.isLsm && cfg.lsmCompaction) {
    return false;
  }
  if (backend.isLsm && !cfg.lsmCompaction) {
    return false;
  }
  return true;
});

const tasks = benchPermutations.map((args) => {
  const id = randomUUID();

  return {
    id,
    args,
    spinner: new Spinner().start(`${id} pending`),
  };
});

async function runTask(task: (typeof tasks)[0]) {
  const benchArgs = [
    ...["--workload", task.args.task],
    ...["--backend", task.args.backend],
    ...(task.args.fsync ? ["--fsync"] : []),
    ...(task.args.items ? [`--items ${task.args.items}`] : []),
    ...["--lsm-compression", task.args.lsmCompression ?? "lz4"],
    ...["--threads", String(task.args.threads)],
    ...["--minutes", String(task.args.minutes)],
    ...["--key-size", String(8)],
    ...["--value-size", String(task.args.valueSize)],
    ...["--cache-size", String(task.args.cacheSize)],
    ...["--lsm-block-size", String(task.args.lsmBlockSize ?? 4_096)],
    ...(task.args.lsmKvSeparation ? ["--lsm-kv-separation"] : []),
    ...["--out", resolve(".results", task.id, "stats.jsonl")],
    ...["--data-dir", resolve(dataDir, task.id)],
    ...(config.formatDisplayName
      ? ["--display-name", `"${config.formatDisplayName(task.args)}"`]
      : []),
  ];

  await new Promise<void>((resolve, reject) => {
    const args = ["run", "-r", "--", ...benchArgs];

    console.log(`Spawning: cargo ${args.join(" ")}`);

    const childProcess = spawn("cargo", args, {
      shell: true,
      stdio: "pipe",
      env: {
        ...process.env,
        RUST_LOG: process.env.RUST_LOG ?? "warn",
        // RUST_BACKTRACE: "full"
      },
    });

    childProcess.stdout.on("data", (buf) =>
      console.log(chalk.white(`${String(buf)}`)),
    );

    if (process.env.RUST_LOG) {
      childProcess.stderr.on("data", (buf) =>
        console.error(chalk.grey(`${String(buf)}`)),
      );
    }

    task.spinner.text = `${task.id} working...`;

    childProcess.on("exit", (code) => {
      if (code === 777) {
        console.log(`${task.args.backend} died by OOM`);
      }
      resolve();
    });
    childProcess.on("error", reject);
  });

  return task;
}

// console.log(tasks);

for (const task of tasks) {
  const dataFolder = resolve(".data", task.id);
  const resultFolder = resolve(".results", task.id);

  mkdirSync(dataFolder, { recursive: true });
  mkdirSync(resultFolder, { recursive: true });
  writeFileSync(
    resolve(resultFolder, "config.json"),
    JSON.stringify(task.args),
    "utf-8",
  );
}

for await (const task of asyncPool(PARALLELISM, tasks, (task) => {
  task.spinner.text = `${task.id} in progress...`;
  return runTask(task);
})) {
  // TODO: time is not right
  const timeS = task.spinner.elapsedTime / 1_000;
  task.spinner.succeed(`${task.id} done in ${timeS.toFixed(2)}s!`);

  await rm(resolve(dataDir, task.id), { recursive: true });
}
Spinner.reset();

process.exit(0);

/* const CLEAN_DATA_FOLDER_AFTER_EACH_TASK = true; */

const steps = [
  /*   {
      tasks: ["d", "e", "f", "g", "h"],
      backends: ["fjall_lcs", "fjall_stcs", "persy", "redb" ,"sled"],
      minutes: 5,
      outFolder: ".results/nosync/5m/low_cache",
      fsync: false,
      valueSize: 128,
      cacheSize: 128_000,
    },
    {
      tasks: ["d", "e", "f", "g", "h"],
      backends: ["fjall_lcs", "fjall_stcs", "persy", "redb", "sled"],
      minutes: 5,
      outFolder: ".results/nosync/5m/high_cache",
      fsync: false,
      valueSize: 128,
      cacheSize: 32_000_000,
    }, */
  /* {
    tasks: ["f"],
    backends: ["fjall_lcs"],
    minutes: 2,
    outFolder: ".results/readheavy",
    fsync: true,
    valueSize: 128,
    cacheSize: 16_000_000,
  }, */
  {
    tasks: ["g"],
    backends: ["fjall_lcs"],
    minutes: 3,
    outFolder: ".results/memtable_64M",
    fsync: false,
    valueSize: 128,
    cacheSize: 8_000_000,
  },
];

for (const config of steps) {
  let tasks = [];

  for (const task of config.tasks) {
    for (const backend of config.backends) {
      const args = [
        ...(config.fsync ? ["--fsync"] : []),
        ...["--threads", "1"],
        ...["--minutes", config.minutes],
        ...["--key-size", 8],
        ...["--value-size", config.valueSize],
        ...["--items", 100],
        ...["--cache-size", config.cacheSize],
        ...["--lsm-block-size", config.blockSize ?? 4_096],
      ];

      const folder = resolve(config.outFolder, `task_${task}`);
      const out = resolve(folder, `${backend}.jsonl`);
      mkdirSync(folder, { recursive: true });

      if (backend === "fjall_stcs") {
        args.push("--lsm-compaction", "tiered");
      }

      const be = backend.startsWith("fjall_") ? "fjall" : backend;

      args.push(
        ...["--out", out],
        ...["--workload", `task-${task}`],
        ...["--backend", be],
      );

      tasks.push(args);
    }
  }

  console.log(
    "Running tasks",
    tasks.map((x) => x.join(" ")),
  );

  async function processTask(task) {
    await new Promise((resolve, reject) => {
      const args = ["run", "-r", "--", ...task];

      console.error(chalk.blueBright(`Spawning: cargo ${args.join(" ")}`));

      const childProcess = spawn("cargo", args, {
        shell: true,
        stdio: "pipe",
        env: {
          ...process.env,
          RUST_LOG: process.env.RUST_LOG ?? "trace",
        },
      });
      childProcess.stdout.on("data", (buf) =>
        console.log(chalk.grey(`${String(buf)}`)),
      );
      childProcess.stderr.on("data", (buf) =>
        console.error(chalk.yellow(`${String(buf)}`)),
      );

      // @ts-ignore
      childProcess.on("exit", () => resolve());
      childProcess.on("error", reject);
    });

    // TODO: need to only delete subfolder of specific task
    // TODO: also each invocation needs its own .data subfolder...
    /* if (CLEAN_DATA_FOLDER_AFTER_EACH_TASK) {
      if (existsSync(".data")) {
        rmSync(".data", {
          recursive: true,
        });
      }
    } */

    return task;
  }

  // Filter out sled, if fsync, because it doesn't actually call fsync??? UNLESS it's Workload C (read-only)
  if (config.fsync) {
    tasks = tasks.filter((args) =>
      ["sled", "bloodstone"].every(
        (term) =>
          args.join(" ").includes("task_c") || !args.join(" ").includes(term),
      ),
    );
  } else {
    // Filter out jammdb & nebari, if !fsync, because they always fsync??? UNLESS it's Workload C (read-only)
    tasks = tasks.filter((args) =>
      ["jamm", "nebari"].every(
        (term) =>
          args.join(" ").includes("task_c") || !args.join(" ").includes(term),
      ),
    );
  }

  for await (const name of asyncPool(PARALLELISM, tasks, processTask)) {
    console.log(chalk.greenBright(`${name.join(" ")} done`));
  }
}
