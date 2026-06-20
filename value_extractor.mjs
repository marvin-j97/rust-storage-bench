
import rl from "node:readline";
import { createReadStream } from "node:fs";

const file = createReadStream(
  process.argv.at(2),
);

const lineReader = rl.createInterface({
  input: file,
});
let lineIndex = 0;

const columnName = process.argv.at(3);
let columnIndex = -1;
let firstEmit = true;

for await (const line of lineReader) {
  /**
   * @type string[]
   */
  const json = JSON.parse(line);

  if (lineIndex === 2) {
    columnIndex = json.findIndex(x => x === columnName);
    // console.error(`column index for ${columnName} is ${columnIndex}`)

    if (columnIndex === -1) {
      throw new Error(`column "${columnName}" not found`);
    }

    console.log("[");
  }
  else if (columnIndex > -1) {
    const timeMs = +json[0];
    const value = json[columnIndex];

    if (typeof value === "number") {
      if (firstEmit) {
        firstEmit = false;
      }
      else {
        console.log(`,`);
      }
      process.stdout.write(`  [${timeMs}, ${value}]`);
    }
  }

  lineIndex++;
}

process.stdout.write("\n]");
