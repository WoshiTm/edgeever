import { afterEach, describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtemp, readdir, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fetchEdgeEverApp } from "./index.ts";
import { createCloudflareStorageAdapter } from "./cloudflare-storage-adapter.ts";
import { RESOURCE_UPLOAD_PART_BYTES } from "./resource-upload-service.ts";
import { createSelfHostedStorageAdapter } from "./self-hosted-storage-adapter.ts";

const temporaryDirectories = [];
afterEach(async () => {
  await Promise.all(temporaryDirectories.splice(0).map((directory) => rm(directory, { recursive: true, force: true })));
});

const SMALL_PNG = Buffer.from(
  "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII=",
  "base64",
);

const patternBytes = (size) => {
  const bytes = new Uint8Array(size);
  for (let index = 0; index < size; index += 1) bytes[index] = index % 251;
  return bytes;
};

const requestJson = async (environment, path, init = {}) => {
  const response = await fetchEdgeEverApp(
    new Request(`http://edgeever.test${path}`, init),
    environment,
    { waitUntil: () => undefined, passThroughOnException: () => undefined },
  );
  const body = await response.json();
  if (!response.ok) throw new Error(`${response.status}: ${JSON.stringify(body)}`);
  return body;
};

const requestRaw = (environment, path, init = {}) => fetchEdgeEverApp(
  new Request(`http://edgeever.test${path}`, init),
  environment,
  { waitUntil: () => undefined, passThroughOnException: () => undefined },
);

describe("d1 blob store integration", () => {
  test("serves the full resource lifecycle over HTTP without an R2 binding", async () => {
    const directory = await mkdtemp(`${tmpdir()}/edgeever-d1-integration-`);
    temporaryDirectories.push(directory);
    const database = new Database(join(directory, "edgeever.sqlite"), { create: true });
    database.exec("PRAGMA foreign_keys = ON");
    const migrationDirectory = new URL("../../../migrations/", import.meta.url);
    for (const name of (await readdir(migrationDirectory)).filter((value) => value.endsWith(".sql")).sort()) {
      database.exec(await readFile(new URL(name, migrationDirectory), "utf8"));
    }
    database.query(
      "INSERT OR IGNORE INTO workspaces (id, name, is_personal) VALUES ('ws_default', 'Personal', 1)",
    ).run();

    const db = createSelfHostedStorageAdapter(database, join(directory, "unused-resources")).db;
    const environment = {
      // No RESOURCES binding: the Cloudflare adapter must fall back to D1.
      storage: createCloudflareStorageAdapter({ DB: db }),
      EDGE_EVER_ALLOW_UNAUTHENTICATED: "true",
      EDGE_EVER_RUNTIME: "cloudflare-workers",
    };

    const health = await requestJson(environment, "/api/health");
    expect(health.ok).toBe(true);
    // The health endpoint reports "r2" for the D1 blob store so legacy native
    // clients render a known label instead of "Unknown"; the live backend is
    // still D1 (see the mapping in the health handler).
    expect(health.storage).toEqual({ database: "d1", resources: "r2" });
    // Migrations were applied by raw exec, so the migration tracking table is
    // empty and the health endpoint reports "unknown"; the wrangler end-to-end
    // run asserts the real applied-migration value.
    expect(typeof health.migration).toBe("string");

    const notebook = await requestJson(environment, "/api/v1/notebooks", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: "D1 notes" }),
    });
    const memo = await requestJson(environment, "/api/v1/memos", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ notebookId: notebook.notebook.id, title: "D1 memo", contentMarkdown: "hello" }),
    });
    const memoId = memo.memo.id;

    const form = new FormData();
    form.append("file", new File([SMALL_PNG], "small.png", { type: "image/png" }));
    const legacy = await requestJson(environment, `/api/v1/memos/${memoId}/resources`, {
      method: "POST",
      body: form,
    });
    expect(legacy.resource).toMatchObject({
      filename: "small.png",
      kind: "image",
      mimeType: "image/png",
      byteSize: SMALL_PNG.byteLength,
    });
    const blobResponse = await requestRaw(environment, `/api/v1/resources/${legacy.resource.id}/blob`);
    expect(blobResponse.status).toBe(200);
    expect(new Uint8Array(await blobResponse.arrayBuffer())).toEqual(new Uint8Array(SMALL_PNG));

    const ranged = await requestRaw(environment, `/api/v1/resources/${legacy.resource.id}/blob`, {
      headers: { Range: "bytes=10-19" },
    });
    expect(ranged.status).toBe(206);
    expect(ranged.headers.get("Content-Range")).toBe(`bytes 10-19/${SMALL_PNG.byteLength}`);
    expect(new Uint8Array(await ranged.arrayBuffer())).toEqual(new Uint8Array(SMALL_PNG.slice(10, 20)));

    const bigBytes = patternBytes(9 * 1024 * 1024);
    const initiated = await requestJson(environment, `/api/v1/memos/${memoId}/resource-uploads`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ filename: "big.bin", mimeType: "application/octet-stream", byteSize: bigBytes.byteLength }),
    });
    expect(initiated.upload.partSize).toBe(RESOURCE_UPLOAD_PART_BYTES);
    expect(initiated.upload.partCount).toBe(Math.ceil(bigBytes.byteLength / RESOURCE_UPLOAD_PART_BYTES));
    const uploadId = initiated.upload.id;
    for (let partNumber = 1; partNumber <= initiated.upload.partCount; partNumber += 1) {
      const start = (partNumber - 1) * initiated.upload.partSize;
      const chunk = bigBytes.slice(start, Math.min(start + initiated.upload.partSize, bigBytes.byteLength));
      const uploaded = await requestJson(environment, `/api/v1/resource-uploads/${uploadId}/parts/${partNumber}`, {
        method: "PUT",
        headers: { "Content-Length": String(chunk.byteLength) },
        body: chunk,
      });
      expect(uploaded.part).toMatchObject({ partNumber, byteSize: chunk.byteLength });
    }
    const completed = await requestJson(environment, `/api/v1/resource-uploads/${uploadId}/complete`, {
      method: "POST",
    });
    expect(completed.resource.byteSize).toBe(bigBytes.byteLength);

    const bigBlob = await requestRaw(environment, `/api/v1/resources/${completed.resource.id}/blob`);
    expect(bigBlob.status).toBe(200);
    expect(new Uint8Array(await bigBlob.arrayBuffer())).toEqual(bigBytes);
    const bigRow = database
      .query("SELECT object_key FROM resources WHERE id = ?")
      .get(completed.resource.id);
    const chunkCount = database
      .query("SELECT COUNT(*) AS count FROM d1_blob_chunks WHERE object_key = ?")
      .get(bigRow.object_key);
    expect(chunkCount.count).toBe(9);

    const exported = await requestJson(environment, "/api/v1/exports/markdown");
    expect(exported.memos.map((entry) => entry.id)).toContain(memoId);
    // Unauthenticated access seeds example content, so assert containment.
    expect(exported.resources.map((entry) => entry.id)).toEqual(
      expect.arrayContaining([legacy.resource.id, completed.resource.id]),
    );

    const objectKeys = [legacy.resource.id, completed.resource.id].map((id) => database
      .query("SELECT object_key FROM resources WHERE id = ?")
      .get(id).object_key);
    await requestJson(environment, `/api/v1/resources/${legacy.resource.id}`, { method: "DELETE" });
    await requestJson(environment, `/api/v1/resources/${completed.resource.id}`, { method: "DELETE" });
    const gone = await requestRaw(environment, `/api/v1/resources/${completed.resource.id}/blob`);
    expect(gone.status).toBe(404);
    const placeholders = objectKeys.map(() => "?").join(", ");
    expect(database.query(`SELECT COUNT(*) AS count FROM d1_blob_chunks WHERE object_key IN (${placeholders})`).get(...objectKeys).count).toBe(0);
    expect(database.query(`SELECT COUNT(*) AS count FROM d1_blob_objects WHERE object_key IN (${placeholders})`).get(...objectKeys).count).toBe(0);
    database.close();
  });
});
