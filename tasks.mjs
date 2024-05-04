// @ts-check

import { spawn } from "node:child_process";
import { mkdirSync, rmSync } from "node:fs";
import { resolve } from "node:path";

import chalk from "chalk";
import asyncPool from "tiny-async-pool";

rmSync(".data", {
  recursive: true,
  force: true,
});

const CLEAN_DATA_FOLDER_AFTER_EACH_TASK = true;
const PARALLELISM = 2;

const steps = [
  /* {
    tasks: ["f", "g"],
    backends: ["fjall_lcs", "fjall_stcs", "persy", "redb"],
    minutes: 1,
    outFolder: ".results/sync",
    fsync: true,
    valueSize: 128,
    cacheSize: 1_000_000,
  }, */
  {
    tasks: ["f", "g"],
    backends: ["fjall_lcs", "fjall_stcs", "persy", "redb", "sled"],
    minutes: 10,
    outFolder: ".results/longbois/nosync",
    fsync: false,
    valueSize: 128,
    cacheSize: 1_000_000,
  }
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
      console.error(
        chalk.blueBright(`Spawning: cargo ${task}`)
      );

      const childProcess = spawn("cargo", ["run", "-r", "--", ...task], {
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

    if (CLEAN_DATA_FOLDER_AFTER_EACH_TASK) {
      rmSync(".data", {
        recursive: true,
        force: true,
      });
    }

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
