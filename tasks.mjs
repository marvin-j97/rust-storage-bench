import { spawn } from "node:child_process";
import { rmSync } from "node:fs";

import asyncPool from "tiny-async-pool";

const bencher = "run -r --";

rmSync(".data", {
  recursive: true,
  force: true,
});

const RUNTIME_MINUTES = 5;
const KEY_SIZE = 8; // TODO: doesn't do anything yet
const VALUE_SIZE = 256;
const ITEM_COUNT = 1_000;
const CACHE_SIZE_MB = 1;

let tasks = [
  `${bencher} --out task_a_lsmt_lcs.jsonl --workload task-a --backend lsm-tree --minutes ${RUNTIME_MINUTES} --key-size ${KEY_SIZE} --value-size ${VALUE_SIZE} --items ${ITEM_COUNT} --cache-size ${CACHE_SIZE_MB * 1000 * 1000}`,
  `${bencher} --out task_a_persy.jsonl --workload task-a --backend persy --minutes ${RUNTIME_MINUTES} --key-size ${KEY_SIZE} --value-size ${VALUE_SIZE} --items ${ITEM_COUNT} --cache-size ${CACHE_SIZE_MB * 1000 * 1000}`,
  `${bencher} --out task_a_sled.jsonl --workload task-a --backend sled --minutes ${RUNTIME_MINUTES} --key-size ${KEY_SIZE} --value-size ${VALUE_SIZE} --items ${ITEM_COUNT} --cache-size ${CACHE_SIZE_MB * 1000 * 1000}`,
  //`${bencher} --out task_a_bloodstone.jsonl --workload task-a --backend bloodstone --minutes ${RUNTIME_MINUTES} --key-size ${KEY_SIZE} --value-size ${VALUE_SIZE} --items ${ITEM_COUNT} --cache-size ${CACHE_SIZE_MB * 1000 * 1000}`,
  `${bencher} --out task_a_lsmt_stcs.jsonl --workload task-a --backend lsm-tree --lsm-compaction tiered --minutes ${RUNTIME_MINUTES} --key-size ${KEY_SIZE} --value-size ${VALUE_SIZE} --items ${ITEM_COUNT} --cache-size ${CACHE_SIZE_MB * 1000 * 1000}`,
  `${bencher} --out task_a_jammdb.jsonl --workload task-a --backend jamm-db --minutes ${RUNTIME_MINUTES} --key-size ${KEY_SIZE} --value-size ${VALUE_SIZE} --items ${ITEM_COUNT} --cache-size ${CACHE_SIZE_MB * 1000 * 1000}`,
  `${bencher} --out task_a_redb.jsonl --workload task-a --backend redb --minutes ${RUNTIME_MINUTES} --key-size ${KEY_SIZE} --value-size ${VALUE_SIZE} --items ${ITEM_COUNT} --cache-size ${CACHE_SIZE_MB * 1000 * 1000}`,

  `${bencher} --out task_b_lsmt_lcs.jsonl --workload task-b --backend lsm-tree --minutes ${RUNTIME_MINUTES} --key-size ${KEY_SIZE} --value-size ${VALUE_SIZE} --items ${ITEM_COUNT} --cache-size ${CACHE_SIZE_MB * 1000 * 1000}`,
  `${bencher} --out task_b_persy.jsonl --workload task-b --backend persy --minutes ${RUNTIME_MINUTES} --key-size ${KEY_SIZE} --value-size ${VALUE_SIZE} --items ${ITEM_COUNT} --cache-size ${CACHE_SIZE_MB * 1000 * 1000}`,
  `${bencher} --out task_b_sled.jsonl --workload task-b --backend sled --minutes ${RUNTIME_MINUTES} --key-size ${KEY_SIZE} --value-size ${VALUE_SIZE} --items ${ITEM_COUNT} --cache-size ${CACHE_SIZE_MB * 1000 * 1000}`,
  //`${bencher} --out task_b_bloodstone.jsonl --workload task-b --backend bloodstone --minutes ${RUNTIME_MINUTES} --key-size ${KEY_SIZE} --value-size ${VALUE_SIZE} --items ${ITEM_COUNT} --cache-size ${CACHE_SIZE_MB * 1000 * 1000}`,
  `${bencher} --out task_b_lsmt_stcs.jsonl --workload task-b --backend lsm-tree --lsm-compaction tiered --minutes ${RUNTIME_MINUTES} --key-size ${KEY_SIZE} --value-size ${VALUE_SIZE} --items ${ITEM_COUNT} --cache-size ${CACHE_SIZE_MB * 1000 * 1000}`,
  `${bencher} --out task_b_jammdb.jsonl --workload task-b --backend jamm-db --minutes ${RUNTIME_MINUTES} --key-size ${KEY_SIZE} --value-size ${VALUE_SIZE} --items ${ITEM_COUNT} --cache-size ${CACHE_SIZE_MB * 1000 * 1000}`,
  `${bencher} --out task_b_redb.jsonl --workload task-b --backend redb --minutes ${RUNTIME_MINUTES} --key-size ${KEY_SIZE} --value-size ${VALUE_SIZE} --items ${ITEM_COUNT} --cache-size ${CACHE_SIZE_MB * 1000 * 1000}`,

  `${bencher} --out task_c_lsmt_lcs.jsonl --workload task-c --backend lsm-tree --minutes ${RUNTIME_MINUTES} --key-size ${KEY_SIZE} --value-size ${VALUE_SIZE} --items ${ITEM_COUNT} --cache-size ${CACHE_SIZE_MB * 1000 * 1000}`,
  `${bencher} --out task_c_persy.jsonl --workload task-c --backend persy --minutes ${RUNTIME_MINUTES} --key-size ${KEY_SIZE} --value-size ${VALUE_SIZE} --items ${ITEM_COUNT} --cache-size ${CACHE_SIZE_MB * 1000 * 1000}`,
  `${bencher} --out task_c_sled.jsonl --workload task-c --backend sled --minutes ${RUNTIME_MINUTES} --key-size ${KEY_SIZE} --value-size ${VALUE_SIZE} --items ${ITEM_COUNT} --cache-size ${CACHE_SIZE_MB * 1000 * 1000}`,
  //`${bencher} --out task_c_bloodstone.jsonl --workload task-c --backend bloodstone --minutes ${RUNTIME_MINUTES} --key-size ${KEY_SIZE} --value-size ${VALUE_SIZE} --items ${ITEM_COUNT} --cache-size ${CACHE_SIZE_MB * 1000 * 1000}`,
  `${bencher} --out task_c_lsmt_stcs.jsonl --workload task-c --backend lsm-tree --lsm-compaction tiered --minutes ${RUNTIME_MINUTES} --key-size ${KEY_SIZE} --value-size ${VALUE_SIZE} --items ${ITEM_COUNT} --cache-size ${CACHE_SIZE_MB * 1000 * 1000}`,
  `${bencher} --out task_c_jammdb.jsonl --workload task-c --backend jamm-db --minutes ${RUNTIME_MINUTES} --key-size ${KEY_SIZE} --value-size ${VALUE_SIZE} --items ${ITEM_COUNT} --cache-size ${CACHE_SIZE_MB * 1000 * 1000}`,
  `${bencher} --out task_c_redb.jsonl --workload task-c --backend redb --minutes ${RUNTIME_MINUTES} --key-size ${KEY_SIZE} --value-size ${VALUE_SIZE} --items ${ITEM_COUNT} --cache-size ${CACHE_SIZE_MB * 1000 * 1000}`,

  `${bencher} --out task_d_lsmt_lcs.jsonl --workload task-d --backend lsm-tree --minutes ${RUNTIME_MINUTES} --key-size ${KEY_SIZE} --value-size ${VALUE_SIZE} --items ${ITEM_COUNT} --cache-size ${CACHE_SIZE_MB * 1000 * 1000}`,
  `${bencher} --out task_d_persy.jsonl --workload task-d --backend persy --minutes ${RUNTIME_MINUTES} --key-size ${KEY_SIZE} --value-size ${VALUE_SIZE} --items ${ITEM_COUNT} --cache-size ${CACHE_SIZE_MB * 1000 * 1000}`,
  `${bencher} --out task_d_sled.jsonl --workload task-d --backend sled --minutes ${RUNTIME_MINUTES} --key-size ${KEY_SIZE} --value-size ${VALUE_SIZE} --items ${ITEM_COUNT} --cache-size ${CACHE_SIZE_MB * 1000 * 1000}`,
  //`${bencher} --out task_d_bloodstone.jsonl --workload task-d --backend bloodstone --minutes ${RUNTIME_MINUTES} --key-size ${KEY_SIZE} --value-size ${VALUE_SIZE} --items ${ITEM_COUNT} --cache-size ${CACHE_SIZE_MB * 1000 * 1000}`,
  `${bencher} --out task_d_lsmt_stcs.jsonl --workload task-d --backend lsm-tree --lsm-compaction tiered --minutes ${RUNTIME_MINUTES} --key-size ${KEY_SIZE} --value-size ${VALUE_SIZE} --items ${ITEM_COUNT} --cache-size ${CACHE_SIZE_MB * 1000 * 1000}`,
  `${bencher} --out task_d_jammdb.jsonl --workload task-d --backend jamm-db --minutes ${RUNTIME_MINUTES} --key-size ${KEY_SIZE} --value-size ${VALUE_SIZE} --items ${ITEM_COUNT} --cache-size ${CACHE_SIZE_MB * 1000 * 1000}`,
  `${bencher} --out task_d_redb.jsonl --workload task-d --backend redb --minutes ${RUNTIME_MINUTES} --key-size ${KEY_SIZE} --value-size ${VALUE_SIZE} --items ${ITEM_COUNT} --cache-size ${CACHE_SIZE_MB * 1000 * 1000}`,

  `${bencher} --out task_e_lsmt_lcs.jsonl --workload task-e --backend lsm-tree --minutes ${RUNTIME_MINUTES} --key-size ${KEY_SIZE} --value-size ${VALUE_SIZE} --items ${ITEM_COUNT} --cache-size ${CACHE_SIZE_MB * 1000 * 1000}`,
  `${bencher} --out task_e_persy.jsonl --workload task-e --backend persy --minutes ${RUNTIME_MINUTES} --key-size ${KEY_SIZE} --value-size ${VALUE_SIZE} --items ${ITEM_COUNT} --cache-size ${CACHE_SIZE_MB * 1000 * 1000}`,
  `${bencher} --out task_e_sled.jsonl --workload task-e --backend sled --minutes ${RUNTIME_MINUTES} --key-size ${KEY_SIZE} --value-size ${VALUE_SIZE} --items ${ITEM_COUNT} --cache-size ${CACHE_SIZE_MB * 1000 * 1000}`,
  //`${bencher} --out task_e_bloodstone.jsonl --workload task-e --backend bloodstone --minutes ${RUNTIME_MINUTES} --key-size ${KEY_SIZE} --value-size ${VALUE_SIZE} --items ${ITEM_COUNT} --cache-size ${CACHE_SIZE_MB * 1000 * 1000}`,
  `${bencher} --out task_e_lsmt_stcs.jsonl --workload task-e --backend lsm-tree --lsm-compaction tiered --minutes ${RUNTIME_MINUTES} --key-size ${KEY_SIZE} --value-size ${VALUE_SIZE} --items ${ITEM_COUNT} --cache-size ${CACHE_SIZE_MB * 1000 * 1000}`,
  `${bencher} --out task_e_jammdb.jsonl --workload task-e --backend jamm-db --minutes ${RUNTIME_MINUTES} --key-size ${KEY_SIZE} --value-size ${VALUE_SIZE} --items ${ITEM_COUNT} --cache-size ${CACHE_SIZE_MB * 1000 * 1000}`,
  `${bencher} --out task_e_redb.jsonl --workload task-e --backend redb --minutes ${RUNTIME_MINUTES} --key-size ${KEY_SIZE} --value-size ${VALUE_SIZE} --items ${ITEM_COUNT} --cache-size ${CACHE_SIZE_MB * 1000 * 1000}`,
];

const filters = process.argv.slice(2);
if (filters.length) {
  tasks = tasks
    .filter(x => filters.some(f => x.includes(f)));
}

console.error("Running", tasks);

async function processTask(task) {
  await new Promise((resolve, reject) => {
    console.error(`Spawning: cargo ${task}`);

    const childProcess = spawn("cargo", task.split(" "), {
      shell: true,
      stdio: "pipe",
    });
    childProcess.stdout.on("data", (buf) => console.log(`${String(buf)}`));
    childProcess.stderr.on("data", (buf) => console.error(`${String(buf)}`));
    childProcess.on('exit', () => resolve());
    childProcess.on('error', reject);
  });

  return task;
}

const PARALLELISM = 2;

for await (const name of asyncPool(PARALLELISM, tasks, processTask)) {
  console.log(`${name} done`);
}
