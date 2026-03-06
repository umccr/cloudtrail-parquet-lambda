import { convertSingleDayCloudTrailToParquets } from "./converter";

await convertSingleDayCloudTrailToParquets(
  "test_data/",
  null,
  "o-abcd",
  "811596193553",
  2020,
  9,
  30,
  5,
  2,
);
