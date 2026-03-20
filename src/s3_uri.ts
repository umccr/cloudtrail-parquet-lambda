export const isS3Uri = (path: string): boolean => path.startsWith("s3://");

export function parseS3Uri(uri: string): { bucket: string; key: string } {
  const withoutScheme = uri.slice("s3://".length);
  const slashIndex = withoutScheme.indexOf("/");
  if (slashIndex === -1) return { bucket: withoutScheme, key: "" };
  return {
    bucket: withoutScheme.slice(0, slashIndex),
    key: withoutScheme.slice(slashIndex + 1),
  };
}
