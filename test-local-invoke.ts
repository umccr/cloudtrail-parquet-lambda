#!/usr/bin/env bun
/**
 * test-local-invoke.ts
 *
 * Runs the Lambda handler locally with a mock scheduled event content.
 *
 * Usage:
 *   bun run local-invoke.ts
 */

import { handler } from "./src/entrypoint_handler";

process.env.PROCESS_DATE = "2020-09-30";

const mockEvent = {
  baseInputPath: "test_input/",
  baseOutputPath: "test_output/",
  organisationId: "o-abcd",
  accountId: "811596193553",
};

const mockContext = {
  functionName: "cloudtrail-to-parquet",
  awsRequestId: "local-" + Date.now(),
  logGroupName: "/aws/lambda/cloudtrail-to-parquet",
};

try {
  await handler(mockEvent, mockContext);
  console.log("\n✅  Handler completed successfully.");
} catch (err) {
  console.error("\n❌  Handler threw an error:");
  console.error(err);
  process.exit(1);
}
