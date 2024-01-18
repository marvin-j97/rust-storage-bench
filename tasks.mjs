import { spawn } from "node:child_process";
import { mkdirSync, rmSync } from "node:fs";
import { cpus } from "node:os";

import asyncPool from "tiny-async-pool";

const bencher = "run -r --";

rmSync(".data", {
  recursive: true,
  force: true,
});

// NOTE: Divide by 2 if hyper-threading
const CPU_CORES = cpus().length / 2;

const RUNTIME_MINUTES = 5;
const KEY_SIZE = 8; // TODO: doesn't do anything yet
const VALUE_SIZE = 256;
const ITEM_COUNT = 1_000; // Prefill with data before running actual runtime minutes
const CACHE_SIZE_MB = 1;
const FSYNC = false;
const THREADS = 1;

const PARALLELISM = Math.min(
  4, CPU_CORES / THREADS
);

const OUT_FOLDER = "dc";
const OUT_SUFFIX = "";

/////////////////////////////

const fsync = FSYNC ? "--fsync" : "";
const cacheBytes = CACHE_SIZE_MB * 1000 * 1000;

const args = `${fsync} --threads ${THREADS} --minutes ${RUNTIME_MINUTES} --key-size ${KEY_SIZE} --value-size ${VALUE_SIZE} --items ${ITEM_COUNT} --cache-size ${cacheBytes}`;

const formatOut = (task, backend) => {
  const folder = `${OUT_FOLDER}/${FSYNC ? "sync" : "bulk"}/${task}/${THREADS}t`;
  mkdirSync(folder, { recursive: true });
  return `${folder}/${RUNTIME_MINUTES}m_${CACHE_SIZE_MB}CM_${backend}_${OUT_SUFFIX}.jsonl`
};

let tasks = [
  `${bencher} --out ${formatOut("task_a", "fjall_lcs")} --workload task-a --backend fjall ${args}`,
  `${bencher} --out ${formatOut("task_a", "persy")} --workload task-a --backend persy ${args}`,
  `${bencher} --out ${formatOut("task_a", "sled")} --workload task-a --backend sled ${args}`,
  //`${bencher} --${formatOut("task_a", "bloodstone")} --workload task-a --backend bloodstone ${args}`,
  `${bencher} --out ${formatOut("task_a", "fjall_stcs")} --workload task-a --backend fjall --lsm-compaction tiered ${args}`,
  `${bencher} --out ${formatOut("task_a", "jammdb")} --workload task-a --backend jamm-db ${args}`,
  `${bencher} --out ${formatOut("task_a", "redb")} --workload task-a --backend redb ${args}`,
  `${bencher} --out ${formatOut("task_a", "nebari")} --workload task-a --backend nebari ${args}`,

  `${bencher} --out ${formatOut("task_b", "fjall_lcs")} --workload task-b --backend fjall ${args}`,
  `${bencher} --out ${formatOut("task_b", "persy")} --workload task-b --backend persy ${args}`,
  `${bencher} --out ${formatOut("task_b", "sled")} --workload task-b --backend sled ${args}`,
  //`${bencher} --${formatOut("task_b", "bloodstone")} --workload task-b --backend bloodstone ${args}`,
  `${bencher} --out ${formatOut("task_b", "fjall_stcs")} --workload task-b --backend fjall --lsm-compaction tiered ${args}`,
  `${bencher} --out ${formatOut("task_b", "jammdb")} --workload task-b --backend jamm-db ${args}`,
  `${bencher} --out ${formatOut("task_b", "redb")} --workload task-b --backend redb ${args}`,
  `${bencher} --out ${formatOut("task_b", "nebari")} --workload task-b --backend nebari ${args}`,

  `${bencher} --out ${formatOut("task_c", "fjall_lcs")} --workload task-c --backend fjall ${args}`,
  `${bencher} --out ${formatOut("task_c", "persy")} --workload task-c --backend persy ${args}`,
  `${bencher} --out ${formatOut("task_c", "sled")} --workload task-c --backend sled ${args}`,
  //`${bencher} --${formatOut("task_c", "bloodstone")} --workload task-c --backend bloodstone ${args}`,
  `${bencher} --out ${formatOut("task_c", "fjall_stcs")} --workload task-c --backend fjall --lsm-compaction tiered ${args}`,
  `${bencher} --out ${formatOut("task_c", "jammdb")} --workload task-c --backend jamm-db ${args}`,
  `${bencher} --out ${formatOut("task_c", "redb")} --workload task-c --backend redb ${args}`,
  `${bencher} --out ${formatOut("task_c", "nebari")} --workload task-c --backend nebari ${args}`,

  `${bencher} --out ${formatOut("task_d", "fjall_lcs")} --workload task-d --backend fjall ${args}`,
  `${bencher} --out ${formatOut("task_d", "persy")} --workload task-d --backend persy ${args}`,
  `${bencher} --out ${formatOut("task_d", "sled")} --workload task-d --backend sled ${args}`,
  //`${bencher} --${formatOut("task_d", "bloodstone")} --workload task-d --backend bloodstone ${args}`,
  `${bencher} --out ${formatOut("task_d", "fjall_stcs")} --workload task-d --backend fjall --lsm-compaction tiered ${args}`,
  `${bencher} --out ${formatOut("task_d", "jammdb")} --workload task-d --backend jamm-db ${args}`,
  `${bencher} --out ${formatOut("task_d", "redb")} --workload task-d --backend redb ${args}`,
  `${bencher} --out ${formatOut("task_d", "nebari")} --workload task-d --backend nebari ${args}`,

  `${bencher} --out ${formatOut("task_e", "fjall_lcs")} --workload task-e --backend fjall ${args}`,
  `${bencher} --out ${formatOut("task_e", "persy")} --workload task-e --backend persy ${args}`,
  `${bencher} --out ${formatOut("task_e", "sled")} --workload task-e --backend sled ${args}`,
  //`${bencher} --${formatOut("task_e", "bloodstone")} --workload task-e --backend bloodstone ${args}`,
  `${bencher} --out ${formatOut("task_e", "fjall_stcs")} --workload task-e --backend fjall --lsm-compaction tiered ${args}`,
  `${bencher} --out ${formatOut("task_e", "jammdb")} --workload task-e --backend jamm-db ${args}`,
  `${bencher} --out ${formatOut("task_e", "redb")} --workload task-e --backend redb ${args}`,
  `${bencher} --out ${formatOut("task_e", "nebari")} --workload task-e --backend nebari ${args}`,

  `${bencher} --out ${formatOut("task_f", "fjall_lcs")} --workload task-f --backend fjall ${args}`,
  `${bencher} --out ${formatOut("task_f", "persy")} --workload task-f --backend persy ${args}`,
  `${bencher} --out ${formatOut("task_f", "sled")} --workload task-f --backend sled ${args}`,
  //`${bencher} --${formatOut("task_f", "bloodstone")} --workload task-f --backend bloodstone ${args}`,
  `${bencher} --out ${formatOut("task_f", "fjall_stcs")} --workload task-f --backend fjall --lsm-compaction tiered ${args}`,
  `${bencher} --out ${formatOut("task_f", "jammdb")} --workload task-f --backend jamm-db ${args}`,
  `${bencher} --out ${formatOut("task_f", "redb")} --workload task-f --backend redb ${args}`,
  `${bencher} --out ${formatOut("task_f", "nebari")} --workload task-f --backend nebari ${args}`,

  `${bencher} --out ${formatOut("task_g", "fjall_lcs")} --workload task-g --backend fjall ${args}`,
  `${bencher} --out ${formatOut("task_g", "persy")} --workload task-g --backend persy ${args}`,
  `${bencher} --out ${formatOut("task_g", "sled")} --workload task-g --backend sled ${args}`,
  //`${bencher} --${formatOut("task_g", "bloodstone")} --workload task-g --backend bloodstone ${args}`,
  `${bencher} --out ${formatOut("task_g", "fjall_stcs")} --workload task-g --backend fjall --lsm-compaction tiered ${args}`,
  `${bencher} --out ${formatOut("task_g", "jammdb")} --workload task-g --backend jamm-db ${args}`,
  `${bencher} --out ${formatOut("task_g", "redb")} --workload task-g --backend redb ${args}`,
  `${bencher} --out ${formatOut("task_g", "nebari")} --workload task-g --backend nebari ${args}`,
];

const filters = process.argv.slice(2);
if (filters.length) {
  tasks = tasks
    .filter(x => filters.some(f => x.includes(f)));
}

// Filter out sled, if fsync, because it doesn't actually call fsync??? UNLESS it's Workload C (read-only)
if (FSYNC) {
  tasks = tasks.filter(str => ["sled", "bloodstone"].every(term => str.includes("task_c") || !str.includes(term)));
}
else {
  // Filter out jammdb & nebari, if !fsync, because they always fsync??? UNLESS it's Workload C (read-only)
  tasks = tasks.filter(str => ["jamm", "nebari"].every(term => str.includes("task_c") || !str.includes(term)));
}

console.error("Running", tasks);

async function processTask(task) {
  await new Promise((resolve, reject) => {
    console.error(`Spawning: cargo ${task}`);

    const childProcess = spawn("cargo", task.split(" "), {
      shell: true,
      stdio: "pipe"
    });
    childProcess.stdout.on("data", (buf) => console.log(`${String(buf)}`));
    childProcess.stderr.on("data", (buf) => console.error(`${String(buf)}`));
    childProcess.on('exit', () => resolve());
    childProcess.on('error', reject);
  });

  return task;
}

for await (const name of asyncPool(PARALLELISM, tasks, processTask)) {
  console.log(`${name} done`);
}
