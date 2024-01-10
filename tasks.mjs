import { spawn } from "node:child_process";
import { rmSync } from "node:fs";

import asyncPool from "tiny-async-pool";

const bencher = "run -r --";

rmSync(".data", {
  recursive: true,
  force: true,
});

const PARALLELISM = 1;
const RUNTIME_MINUTES = 3;
const KEY_SIZE = 8; // TODO: doesn't do anything yet
const VALUE_SIZE = 256;
const ITEM_COUNT = 10_000;
const CACHE_SIZE_MB = 1;
const FSYNC = true;
const THREADS = 4;
const OUT_PREFIX = "sync_evo_";

/////////////////////////////

const fsync = FSYNC ? "--fsync" : "";
const cacheBytes = CACHE_SIZE_MB * 1000 * 1000;

const args = `${fsync} --threads ${THREADS} --minutes ${RUNTIME_MINUTES} --key-size ${KEY_SIZE} --value-size ${VALUE_SIZE} --items ${ITEM_COUNT} --cache-size ${cacheBytes}`;

let tasks = [
  `${bencher} --out ${OUT_PREFIX}task_a_fjall_lcs.jsonl --workload task-a --backend fjall ${args}`,
  `${bencher} --out ${OUT_PREFIX}task_a_persy.jsonl --workload task-a --backend persy ${args}`,
  `${bencher} --out ${OUT_PREFIX}task_a_sled.jsonl --workload task-a --backend sled ${args}`,
  //`${bencher} --out ${OUT_PREFIX}task_a_bloodstone.jsonl --workload task-a --backend bloodstone ${args}`,
  `${bencher} --out ${OUT_PREFIX}task_a_fjall_stcs.jsonl --workload task-a --backend fjall --lsm-compaction tiered ${args}`,
  `${bencher} --out ${OUT_PREFIX}task_a_jammdb.jsonl --workload task-a --backend jamm-db ${args}`,
  `${bencher} --out ${OUT_PREFIX}task_a_redb.jsonl --workload task-a --backend redb ${args}`,
  `${bencher} --out ${OUT_PREFIX}task_a_nebari.jsonl --workload task-a --backend nebari ${args}`,

  `${bencher} --out ${OUT_PREFIX}task_b_fjall_lcs.jsonl --workload task-b --backend fjall ${args}`,
  `${bencher} --out ${OUT_PREFIX}task_b_persy.jsonl --workload task-b --backend persy ${args}`,
  `${bencher} --out ${OUT_PREFIX}task_b_sled.jsonl --workload task-b --backend sled ${args}`,
  //`${bencher} --out ${OUT_PREFIX}task_b_bloodstone.jsonl --workload task-b --backend bloodstone ${args}`,
  `${bencher} --out ${OUT_PREFIX}task_b_fjall_stcs.jsonl --workload task-b --backend fjall --lsm-compaction tiered ${args}`,
  `${bencher} --out ${OUT_PREFIX}task_b_jammdb.jsonl --workload task-b --backend jamm-db ${args}`,
  `${bencher} --out ${OUT_PREFIX}task_b_redb.jsonl --workload task-b --backend redb ${args}`,
  `${bencher} --out ${OUT_PREFIX}task_b_nebari.jsonl --workload task-b --backend nebari ${args}`,

  `${bencher} --out ${OUT_PREFIX}task_c_fjall_lcs.jsonl --workload task-c --backend fjall ${args}`,
  `${bencher} --out ${OUT_PREFIX}task_c_persy.jsonl --workload task-c --backend persy ${args}`,
  `${bencher} --out ${OUT_PREFIX}task_c_sled.jsonl --workload task-c --backend sled ${args}`,
  //`${bencher} --out ${OUT_PREFIX}task_c_bloodstone.jsonl --workload task-c --backend bloodstone ${args}`,
  `${bencher} --out ${OUT_PREFIX}task_c_fjall_stcs.jsonl --workload task-c --backend fjall --lsm-compaction tiered ${args}`,
  `${bencher} --out ${OUT_PREFIX}task_c_jammdb.jsonl --workload task-c --backend jamm-db ${args}`,
  `${bencher} --out ${OUT_PREFIX}task_c_redb.jsonl --workload task-c --backend redb ${args}`,
  `${bencher} --out ${OUT_PREFIX}task_c_nebari.jsonl --workload task-c --backend nebari ${args}`,

  `${bencher} --out ${OUT_PREFIX}task_d_fjall_lcs.jsonl --workload task-d --backend fjall ${args}`,
  `${bencher} --out ${OUT_PREFIX}task_d_persy.jsonl --workload task-d --backend persy ${args}`,
  `${bencher} --out ${OUT_PREFIX}task_d_sled.jsonl --workload task-d --backend sled ${args}`,
  //`${bencher} --out ${OUT_PREFIX}task_d_bloodstone.jsonl --workload task-d --backend bloodstone ${args}`,
  `${bencher} --out ${OUT_PREFIX}task_d_fjall_stcs.jsonl --workload task-d --backend fjall --lsm-compaction tiered ${args}`,
  `${bencher} --out ${OUT_PREFIX}task_d_jammdb.jsonl --workload task-d --backend jamm-db ${args}`,
  `${bencher} --out ${OUT_PREFIX}task_d_redb.jsonl --workload task-d --backend redb ${args}`,
  `${bencher} --out ${OUT_PREFIX}task_d_nebari.jsonl --workload task-d --backend nebari ${args}`,

  `${bencher} --out ${OUT_PREFIX}task_e_fjall_lcs.jsonl --workload task-e --backend fjall ${args}`,
  `${bencher} --out ${OUT_PREFIX}task_e_persy.jsonl --workload task-e --backend persy ${args}`,
  `${bencher} --out ${OUT_PREFIX}task_e_sled.jsonl --workload task-e --backend sled ${args}`,
  //`${bencher} --out ${OUT_PREFIX}task_e_bloodstone.jsonl --workload task-e --backend bloodstone ${args}`,
  `${bencher} --out ${OUT_PREFIX}task_e_fjall_stcs.jsonl --workload task-e --backend fjall --lsm-compaction tiered ${args}`,
  `${bencher} --out ${OUT_PREFIX}task_e_jammdb.jsonl --workload task-e --backend jamm-db ${args}`,
  `${bencher} --out ${OUT_PREFIX}task_e_redb.jsonl --workload task-e --backend redb ${args}`,
  `${bencher} --out ${OUT_PREFIX}task_e_nebari.jsonl --workload task-e --backend nebari ${args}`,

  `${bencher} --out ${OUT_PREFIX}task_f_fjall_lcs.jsonl --workload task-f --backend fjall ${args}`,
  `${bencher} --out ${OUT_PREFIX}task_f_persy.jsonl --workload task-f --backend persy ${args}`,
  `${bencher} --out ${OUT_PREFIX}task_f_sled.jsonl --workload task-f --backend sled ${args}`,
  //`${bencher} --out ${OUT_PREFIX}task_f_bloodstone.jsonl --workload task-f --backend bloodstone ${args}`,
  `${bencher} --out ${OUT_PREFIX}task_f_fjall_stcs.jsonl --workload task-f --backend fjall --lsm-compaction tiered ${args}`,
  `${bencher} --out ${OUT_PREFIX}task_f_jammdb.jsonl --workload task-f --backend jamm-db ${args}`,
  `${bencher} --out ${OUT_PREFIX}task_f_redb.jsonl --workload task-f --backend redb ${args}`,
  `${bencher} --out ${OUT_PREFIX}task_f_nebari.jsonl --workload task-f --backend nebari ${args}`,

  `${bencher} --out ${OUT_PREFIX}task_g_fjall_lcs.jsonl --workload task-g --backend fjall ${args}`,
  `${bencher} --out ${OUT_PREFIX}task_g_persy.jsonl --workload task-g --backend persy ${args}`,
  `${bencher} --out ${OUT_PREFIX}task_g_sled.jsonl --workload task-g --backend sled ${args}`,
  //`${bencher} --out ${OUT_PREFIX}task_g_bloodstone.jsonl --workload task-g --backend bloodstone ${args}`,
  `${bencher} --out ${OUT_PREFIX}task_g_fjall_stcs.jsonl --workload task-g --backend fjall --lsm-compaction tiered ${args}`,
  `${bencher} --out ${OUT_PREFIX}task_g_jammdb.jsonl --workload task-g --backend jamm-db ${args}`,
  `${bencher} --out ${OUT_PREFIX}task_g_redb.jsonl --workload task-g --backend redb ${args}`,
  `${bencher} --out ${OUT_PREFIX}task_g_nebari.jsonl --workload task-g --backend nebari ${args}`,
];

const filters = process.argv.slice(2);
if (filters.length) {
  tasks = tasks
    .filter(x => filters.some(f => x.includes(f)));
}

// Filter out sled, if fsync, because it doesn't actually call fsync???
if (FSYNC) {
  tasks = tasks.filter(str => ["sled", "bloodstone"].every(term => !str.includes(term)));
}
else {
  // Filter out jammdb & nebari, if !fsync, because they always fsync
  tasks = tasks.filter(str => ["jamm", "nebari"].every(term => !str.includes(term)));
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
