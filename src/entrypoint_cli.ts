import { convertSingleDayCloudTrailToParquets } from "./converter";

await convertSingleDayCloudTrailToParquets(
  "test_data/",
  null,
  "o-abcd",
  2020,
  9,
  30,
  5,
  2
);
