// @ts-check

import { spawn } from "node:child_process";
import { existsSync, mkdirSync, rmSync, rmdirSync } from "node:fs";
import { resolve } from "node:path";

import chalk from "chalk";
import asyncPool from "tiny-async-pool";

if (existsSync(".data")) {
  rmdirSync(".data", {
    recursive: true,
  });
}

/* const CLEAN_DATA_FOLDER_AFTER_EACH_TASK = true; */
const PARALLELISM = 1;

const steps = [
  {
    tasks: ["d", "e", "f", "g", "h"],
    backends: ["fjall_lcs", "fjall_stcs", "persy", "redb", "sled"],
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
  },
]

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
        ...["--backend", be]
      );

      tasks.push(
        args
      );
    }
  }

  console.log("Running tasks", tasks.map(x => x.join(" ")));

  async function processTask(task) {
    await new Promise((resolve, reject) => {
      const args = ["run", "-r", "--", ...task];

      console.error(
        chalk.blueBright(`Spawning: cargo ${args.join(" ")}`)
      );

      const childProcess = spawn("cargo", args, {
        shell: true,
        stdio: "pipe"
      });
      childProcess.stdout.on("data", (buf) => console.log(
        chalk.grey(`${String(buf)}`)
      ));
      childProcess.stderr.on("data", (buf) => console.error(
        chalk.yellow(`${String(buf)}`)
      ));

      // @ts-ignore
      childProcess.on('exit', () => resolve());
      childProcess.on('error', reject);
    });

    // TODO: need to only delete subfolder of specific task
    // TODO: also each invocation needs its own .data subfolder...
    /* if (CLEAN_DATA_FOLDER_AFTER_EACH_TASK) {
      if (existsSync(".data")) {
        rmdirSync(".data", {
          recursive: true,
        });
      }
    } */

    return task;
  }

  // Filter out sled, if fsync, because it doesn't actually call fsync??? UNLESS it's Workload C (read-only)
  if (config.fsync) {
    tasks = tasks.filter(args => ["sled", "bloodstone"].every(term => (args.join(" ")).includes("task_c") || !(args.join(" ")).includes(term)));
  }
  else {
    // Filter out jammdb & nebari, if !fsync, because they always fsync??? UNLESS it's Workload C (read-only)
    tasks = tasks.filter(args => ["jamm", "nebari"].every(term => (args.join(" ")).includes("task_c") || !(args.join(" ")).includes(term)));
  }

  for await (const name of asyncPool(PARALLELISM, tasks, processTask)) {
    console.log(`${name} done`);
  }
}
