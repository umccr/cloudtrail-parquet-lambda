FROM oven/bun:1.3.10 AS builder

WORKDIR /app

# install dependencies first (layer-cache friendly)
COPY package.json bun.lock* ./
RUN bun install --frozen-lockfile

# copy source
COPY . .

# Bundle handler + all imports into a single JS file.
# --target=bun keeps Bun APIs available at runtime.
# --minify reduces cold-start size.
RUN bun build ./handler.ts \
      --outfile dist/handler.js \
      --target bun \
      --minify


FROM oven/bun:1.3.10-slim AS runtime

WORKDIR /app

# the aws-lambda-ric (Runtime Interface Client) bootstraps the Lambda runtime
# protocol. bun-lambda wraps this cleanly for Bun.
RUN bun add bun-lambda

COPY --from=builder /app/dist/handler.js ./handler.js

# tell Lambda to use bun-lambda as the bootstrap
ENTRYPOINT ["/app/node_modules/bun-lambda/aws-lambda-rie"]

CMD ["handler.handler"]
