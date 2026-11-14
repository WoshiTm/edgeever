import { createD1BlobStore } from "./d1-blob-store";
import type {
  BlobStoreAdapter,
  DatabaseAdapter,
  StorageAdapter,
} from "./storage-contract";

/** Native Worker bindings are mentioned only at this platform boundary. */
export type CloudflareStorageBindings = {
  DB: DatabaseAdapter;
  /**
   * Optional: deployments without an R2 bucket store resource bytes in D1
   * instead. When the binding is present it keeps serving every resource,
   * exactly as before.
   */
  RESOURCES?: BlobStoreAdapter;
};

/**
 * Adapts native Cloudflare bindings to the storage surface consumed by the
 * application. No route or service should construct this shape directly.
 *
 * With an R2 bucket bound, resources keep using it. Without one, resource
 * bytes fall back to chunked D1 storage so a deployment needs no R2 bucket
 * at all; diagnostics report which backend is live.
 */
export const createCloudflareStorageAdapter = (
  bindings: CloudflareStorageBindings,
): StorageAdapter => {
  const useR2 = Boolean(bindings.RESOURCES);
  return {
    db: bindings.DB,
    resources: bindings.RESOURCES ?? createD1BlobStore(bindings.DB),
    diagnostics: {
      database: "d1",
      resources: useR2 ? "r2" : "d1",
      migrationTable: "d1_migrations",
    },
  };
};
