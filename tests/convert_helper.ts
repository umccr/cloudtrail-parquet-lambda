import { Compression } from "parquet-wasm/node";
import { convertSingle } from "../src/converter";

export async function getFirstConversion(inputPath: string, eventTimePrefix: string) {
  for await (const pq of convertSingle(
    [inputPath], eventTimePrefix,
    100,
    1,
    Compression.SNAPPY,
  )) {
    if (pq)
      return pq;
  }

  throw new Error(
    `No Parquet data generated for ${inputPath} with eventTimePrefix of ${eventTimePrefix}`,
  );
}
