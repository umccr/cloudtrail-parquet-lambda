/**
 * entrypoint_lambda_runtime.ts
 *
 * Minimal Lambda Runtime API loop for Bun container images.
 * Implements the Lambda Runtime API protocol:
 * https://docs.aws.amazon.com/lambda/latest/dg/runtimes-api.html
 *
 * The handler module path and export name are taken from the Lambda
 * _HANDLER env var, which AWS sets from the container CMD, e.g. "handler.handler".
 *
 * Claude generated this under instruction.
 *
 * Replace this with a standard NPM package or Docker layer when or
 * if one becomes available.
 */

const RUNTIME_API = `http://${process.env.AWS_LAMBDA_RUNTIME_API}/2018-06-01/runtime`;

// ── Load handler ──────────────────────────────────────────────────────────────

const [modulePath, exportName] = (process.env._HANDLER ?? "").split(".");

if (!modulePath || !exportName) {
  const msg = `Invalid _HANDLER: "${process.env._HANDLER}" — expected "module.exportName"`;
  await reportInitError(new Error(msg));
  process.exit(1);
}

let handler: (event: unknown, context: unknown) => Promise<unknown>;

try {
  const mod = await import(`/app/${modulePath}.js`);
  handler = mod[exportName];
  if (typeof handler !== "function") {
    throw new Error(
      `Export "${exportName}" in "${modulePath}.js" is not a function`,
    );
  }
} catch (err) {
  await reportInitError(err as Error);
  process.exit(1);
}

// ── Event loop ────────────────────────────────────────────────────────────────

while (true) {
  // 1. Get next invocation
  const invocationRes = await fetch(`${RUNTIME_API}/invocation/next`);
  const requestId = invocationRes.headers.get("lambda-runtime-aws-request-id")!;
  const deadlineMs = Number(
    invocationRes.headers.get("lambda-runtime-deadline-ms"),
  );
  const functionArn =
    invocationRes.headers.get("lambda-runtime-invoked-function-arn") ?? "";
  const event = await invocationRes.json();

  const context = {
    awsRequestId: requestId,
    invokedFunctionArn: functionArn,
    functionName: process.env.AWS_LAMBDA_FUNCTION_NAME ?? "",
    functionVersion: process.env.AWS_LAMBDA_FUNCTION_VERSION ?? "$LATEST",
    memoryLimitInMB: process.env.AWS_LAMBDA_FUNCTION_MEMORY_SIZE ?? "128",
    logGroupName: process.env.AWS_LAMBDA_LOG_GROUP_NAME ?? "",
    logStreamName: process.env.AWS_LAMBDA_LOG_STREAM_NAME ?? "",
    getRemainingTimeInMillis: () => deadlineMs - Date.now(),
  };

  // 2. Invoke handler
  try {
    const result = await handler(event, context);
    await fetch(`${RUNTIME_API}/invocation/${requestId}/response`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(result ?? null),
    });
  } catch (err) {
    await reportInvocationError(requestId, err as Error);
  }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

async function reportInitError(err: Error) {
  console.error("Lambda init error:", err);
  await fetch(`${RUNTIME_API}/init/error`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Lambda-Runtime-Function-Error-Type": "Runtime.InitError",
    },
    body: JSON.stringify({
      errorMessage: err.message,
      errorType: err.name ?? "Error",
      stackTrace: err.stack?.split("\n") ?? [],
    }),
  }).catch(() => {});
}

async function reportInvocationError(requestId: string, err: Error) {
  console.error("Lambda invocation error:", err);
  await fetch(`${RUNTIME_API}/invocation/${requestId}/error`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Lambda-Runtime-Function-Error-Type": "Runtime.InvocationError",
    },
    body: JSON.stringify({
      errorMessage: err.message,
      errorType: err.name ?? "Error",
      stackTrace: err.stack?.split("\n") ?? [],
    }),
  });
}
