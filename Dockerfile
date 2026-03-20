# Bun builder of app
#
FROM oven/bun:1.3.10 AS builder

WORKDIR /app

# install dependencies first (layer-cache friendly)
COPY package.json bun.lock* ./
RUN bun install --frozen-lockfile

# copy source
COPY . .

# Bundle entrypoint_handler + all imports into a single JS file.
# --target=bun keeps Bun APIs available at runtime.
# --minify reduces cold-start size.
RUN bun build ./src/entrypoint_handler.ts \
      --outfile dist/handler.js \
      --target bun \
      --minify

# The lambda runtime is separate standalone code that is invoked
# by the actual lambda infrastructure and translates into
# an invoke at our entryoint_handler
RUN bun build ./src/entrypoint_lambda_runtime.ts \
      --outfile dist/runtime.js \
      --target bun \
      --minify

# Actual lambda target (production)
#
FROM oven/bun:1.3.10-slim AS runtime

WORKDIR /app

COPY --from=builder /app/dist/handler.js ./handler.js
COPY --from=builder /app/dist/runtime.js ./runtime.js

COPY --from=builder /app/node_modules/parquet-wasm ./node_modules/parquet-wasm

# AWS Lambda container contract:
#   ENTRYPOINT  = the process to run
#   CMD         = passed as _HANDLER env var by Lambda ("module.exportName")
ENTRYPOINT ["bun", "run", "/app/runtime.js"]
CMD ["handler.handler"]


# Local testing target (not used in production)
#
FROM runtime AS local

ADD https://github.com/aws/aws-lambda-runtime-interface-emulator/releases/download/v1.33/aws-lambda-rie-arm64 /usr/local/bin/aws-lambda-rie
RUN chmod +x /usr/local/bin/aws-lambda-rie

ENTRYPOINT ["/usr/local/bin/aws-lambda-rie", "bun", "run", "/app/runtime.js"]
CMD ["handler.handler"]
