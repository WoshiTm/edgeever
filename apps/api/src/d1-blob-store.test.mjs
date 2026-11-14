import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { mkdtemp, readdir, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { createD1BlobStore } from "./d1-blob-store.ts";
import { createSelfHostedStorageAdapter } from "./self-hosted-storage-adapter.ts";

const MIGRATIONS_URL = new URL("../../../migrations/", import.meta.url);

const applyMigrations = async (database, filter = () => true) => {
  const names = (await readdir(MIGRATIONS_URL))
    .filter((name) => name.endsWith(".sql") && filter(name))
    .sort();
  for (const name of names) {
    database.exec(await readFile(new URL(name, MIGRATIONS_URL), "utf8"));
  }
  return names;
};

/**
 * Mimics the D1 Workers read shape: BLOB columns come back as plain arrays of
 * byte values instead of Uint8Array. Statements keep their raw SQLite
 * statement reachable so batch() still sees instances it accepts.
 */
const asD1Shape = (db) => {
  const convertValue = (value) => {
    if (value instanceof Uint8Array) return Array.from(value);
    if (value !== null && typeof value === "object" && !Array.isArray(value)) {
      return Object.fromEntries(
        Object.entries(value).map(([key, entry]) => [key, convertValue(entry)]),
      );
    }
    return value;
  };
  const wrapStatement = (statement) => ({
    __raw: statement,
    bind: (...values) => wrapStatement(statement.bind(...values)),
    first: async (...args) => convertValue(await statement.first(...args)),
    run: (...args) => statement.run(...args),
    all: async (...args) => {
      const result = await statement.all(...args);
      return { ...result, results: result.results.map(convertValue) };
    },
  });
  return {
    prepare: (sql) => wrapStatement(db.prepare(sql)),
    batch: (statements) => db.batch(statements.map((statement) => statement.__raw ?? statement)),
  };
};

const textBytes = (text) => new TextEncoder().encode(text);

const patternBytes = (size) => {
  const bytes = new Uint8Array(size);
  for (let index = 0; index < size; index += 1) bytes[index] = index % 251;
  return bytes;
};

const streamOf = (bytes, pieceSize = 7) => new ReadableStream({
  start(controller) {
    for (let index = 0; index < bytes.length; index += pieceSize) {
      controller.enqueue(bytes.slice(index, index + pieceSize));
    }
    controller.close();
  },
});

const readBody = async (stream) => new Uint8Array(await new Response(stream).arrayBuffer());

const sha256Hex = (bytes) => createHash("sha256").update(bytes).digest("hex");

const runBlobStoreSuite = (label, wrapDb) => {
  describe(`d1 blob store (${label})`, () => {
    let database;
    let directory;
    let store;

    beforeEach(async () => {
      database = new Database(":memory:");
      await applyMigrations(database);
      directory = await mkdtemp(join(tmpdir(), "edgeever-d1-blob-store-"));
      const db = wrapDb(createSelfHostedStorageAdapter(database, directory).db);
      store = createD1BlobStore(db, {
        chunkBytes: 16,
        maxObjectBytes: 4096,
        readChunksPerQuery: 2,
        writeChunksPerBatch: 3,
      });
    });

    afterEach(async () => {
      database.close();
      await rm(directory, { recursive: true, force: true });
    });

    const chunkRows = (key) => database
      .query("SELECT generation, chunk_index, length(data) AS size FROM d1_blob_chunks WHERE object_key = ? ORDER BY chunk_index")
      .all(key);
    const objectRow = (key) => database
      .query("SELECT * FROM d1_blob_objects WHERE object_key = ?")
      .get(key);

    test("round-trips a small object through put and get", async () => {
      const bytes = textBytes("hello d1");
      await store.put("a/small.txt", bytes, {
        httpMetadata: { contentType: "text/plain", cacheControl: "public, max-age=60" },
        customMetadata: { memoId: "m1" },
      });
      const object = await store.get("a/small.txt");
      expect(object).not.toBeNull();
      expect(object.size).toBe(bytes.byteLength);
      expect(await readBody(object.body)).toEqual(bytes);
      const headers = new Headers();
      object.writeHttpMetadata(headers);
      expect(headers.get("Content-Type")).toBe("text/plain");
      expect(headers.get("Cache-Control")).toBe("public, max-age=60");
      expect(headers.get("ETag")).toBe(`"${sha256Hex(bytes)}"`);
      expect(headers.get("Last-Modified")).toBeTruthy();
    });

    test("returns null for a missing key", async () => {
      expect(await store.get("nope/missing.bin")).toBeNull();
    });

    test("serves byte ranges across chunk boundaries", async () => {
      const bytes = patternBytes(100);
      await store.put("ranged.bin", bytes);
      const middle = await store.get("ranged.bin", { range: { offset: 10, length: 40 } });
      expect(middle.size).toBe(100);
      expect(middle.range).toEqual({ offset: 10, length: 40 });
      expect(await readBody(middle.body)).toEqual(bytes.slice(10, 50));
      const suffix = await store.get("ranged.bin", { range: { offset: 90, length: 1000 } });
      expect(suffix.range).toEqual({ offset: 90, length: 10 });
      expect(await readBody(suffix.body)).toEqual(bytes.slice(90));
      const beyond = await store.get("ranged.bin", { range: { offset: 500, length: 10 } });
      expect(beyond.range).toEqual({ offset: 100, length: 0 });
      expect(await readBody(beyond.body)).toEqual(new Uint8Array(0));
      const single = await store.get("ranged.bin", { range: { offset: 15, length: 2 } });
      expect(await readBody(single.body)).toEqual(bytes.slice(15, 17));
    });

    test("stores multi-chunk objects byte-exactly", async () => {
      const bytes = patternBytes(101);
      await store.put("multi.bin", bytes);
      expect(chunkRows("multi.bin").map((row) => row.size)).toEqual([16, 16, 16, 16, 16, 16, 5]);
      const object = await store.get("multi.bin");
      expect(await readBody(object.body)).toEqual(bytes);
    });

    test("overwriting a key replaces old chunks and generations", async () => {
      await store.put("overwrite.bin", patternBytes(64));
      const firstGeneration = objectRow("overwrite.bin").generation;
      expect(chunkRows("overwrite.bin")).toHaveLength(4);
      await store.put("overwrite.bin", textBytes("tiny"));
      const row = objectRow("overwrite.bin");
      expect(row.generation).not.toBe(firstGeneration);
      expect(row.byte_size).toBe(4);
      expect(row.chunk_count).toBe(1);
      expect(chunkRows("overwrite.bin")).toHaveLength(1);
      const object = await store.get("overwrite.bin");
      expect(await readBody(object.body)).toEqual(textBytes("tiny"));
    });

    test("growing overwrite replaces the previous object", async () => {
      await store.put("grow.bin", textBytes("tiny"));
      const bytes = patternBytes(80);
      await store.put("grow.bin", bytes);
      expect(chunkRows("grow.bin")).toHaveLength(5);
      const object = await store.get("grow.bin");
      expect(await readBody(object.body)).toEqual(bytes);
    });

    test("delete removes the object row and every chunk", async () => {
      await store.put("del/one.bin", patternBytes(40));
      await store.put("del/two.bin", patternBytes(20));
      await store.delete(["del/one.bin", "del/two.bin", "del/missing.bin"]);
      expect(objectRow("del/one.bin")).toBeNull();
      expect(objectRow("del/two.bin")).toBeNull();
      expect(chunkRows("del/one.bin")).toHaveLength(0);
      expect(chunkRows("del/two.bin")).toHaveLength(0);
      expect(await store.get("del/one.bin")).toBeNull();
      await store.delete([]);
      await store.delete("del/missing.bin");
    });

    test("accepts string, ArrayBuffer, view, Blob, and stream values", async () => {
      const cases = [
        ["string", "plain text", textBytes("plain text")],
        ["arraybuffer", textBytes("buffered").buffer, textBytes("buffered")],
        ["view", new DataView(textBytes("viewed").buffer), textBytes("viewed")],
        ["blob", new Blob([textBytes("blobbed")]), textBytes("blobbed")],
        ["stream", streamOf(textBytes("streamed"), 3), textBytes("streamed")],
      ];
      for (const [name, value, expected] of cases) {
        await store.put(`shapes/${name}`, value);
        const object = await store.get(`shapes/${name}`);
        expect(await readBody(object.body)).toEqual(expected);
      }
    });

    test("stores empty objects", async () => {
      await store.put("empty.bin", new Uint8Array(0));
      const row = objectRow("empty.bin");
      expect(row.byte_size).toBe(0);
      expect(row.chunk_count).toBe(0);
      expect(row.etag).toBe(sha256Hex(new Uint8Array(0)));
      const object = await store.get("empty.bin");
      expect(object.size).toBe(0);
      expect(await readBody(object.body)).toEqual(new Uint8Array(0));
    });

    test("single-chunk etag is the content SHA-256 hex; multi-chunk etag is composite", async () => {
      const small = textBytes("etag-me");
      const put = await store.put("etag/small", small);
      expect(put.etag).toBe(sha256Hex(small));
      const big = patternBytes(40);
      const putBig = await store.put("etag/big", big);
      const digests = [0, 16, 32].map((start) => {
        const digest = createHash("sha256").update(big.slice(start, start + 16)).digest();
        return digest;
      });
      const joined = Buffer.concat(digests);
      expect(putBig.etag).toBe(`${sha256Hex(joined)}-3`);
    });

    test("rejects empty keys", async () => {
      await expect(store.put("", textBytes("x"))).rejects.toThrow("non-empty string");
      await expect(store.get("")).rejects.toThrow("non-empty string");
    });

    test("fails a read loudly when a chunk is missing instead of shifting bytes", async () => {
      await store.put("broken.bin", patternBytes(48));
      database.exec("DELETE FROM d1_blob_chunks WHERE object_key = 'broken.bin' AND chunk_index = 1");
      const object = await store.get("broken.bin");
      await expect(readBody(object.body)).rejects.toThrow("incomplete");
    });

    test("enforces the object size cap on put without leaving a partial object", async () => {
      const error = await store.put("too-big.bin", patternBytes(5000)).catch((caught) => caught);
      expect(error.code).toBe("d1_object_too_large");
      expect(error.status).toBe(413);
      expect(objectRow("too-big.bin")).toBeNull();
      expect(chunkRows("too-big.bin")).toHaveLength(0);
      const streamError = await store.put("too-big-stream.bin", streamOf(patternBytes(5000), 997))
        .catch((caught) => caught);
      expect(streamError.code).toBe("d1_object_too_large");
      expect(objectRow("too-big-stream.bin")).toBeNull();
    });

    test("multipart upload assembles out-of-order parts in order", async () => {
      const upload = await store.createMultipartUpload("mp/assembled.bin", {
        httpMetadata: { contentType: "application/octet-stream" },
      });
      const first = patternBytes(40);
      const second = patternBytes(33).map((byte) => (byte + 7) % 251);
      const part2 = await upload.uploadPart(2, streamOf(second, 5));
      const part1 = await upload.uploadPart(1, first);
      await upload.complete([part2, part1]);
      const object = await store.get("mp/assembled.bin");
      expect(object.size).toBe(73);
      expect(await readBody(object.body)).toEqual(
        new Uint8Array([...first, ...second]),
      );
      const headers = new Headers();
      object.writeHttpMetadata(headers);
      expect(headers.get("Content-Type")).toBe("application/octet-stream");
      expect(database.query("SELECT COUNT(*) AS count FROM d1_blob_multipart_uploads").get().count).toBe(0);
      expect(database.query("SELECT COUNT(*) AS count FROM d1_blob_multipart_parts").get().count).toBe(0);
    });

    test("multipart upload resumes through a new adapter instance", async () => {
      const upload = await store.createMultipartUpload("mp/resumed.bin");
      const part1 = await upload.uploadPart(1, textBytes("first-"));
      const resumed = store.resumeMultipartUpload("mp/resumed.bin", upload.uploadId);
      const part2 = await resumed.uploadPart(2, textBytes("second"));
      await resumed.complete([part1, part2]);
      const object = await store.get("mp/resumed.bin");
      expect(await readBody(object.body)).toEqual(textBytes("first-second"));
    });

    test("multipart part re-upload overwrites the previous part", async () => {
      const upload = await store.createMultipartUpload("mp/replaced.bin");
      await upload.uploadPart(1, textBytes("stale"));
      const part1 = await upload.uploadPart(1, textBytes("fresh"));
      await upload.complete([part1]);
      const object = await store.get("mp/replaced.bin");
      expect(await readBody(object.body)).toEqual(textBytes("fresh"));
    });

    test("multipart abort removes the session and its parts", async () => {
      const upload = await store.createMultipartUpload("mp/aborted.bin");
      await upload.uploadPart(1, textBytes("part"));
      await upload.abort();
      expect(database.query("SELECT COUNT(*) AS count FROM d1_blob_multipart_uploads").get().count).toBe(0);
      expect(database.query("SELECT COUNT(*) AS count FROM d1_blob_multipart_parts").get().count).toBe(0);
      expect(await store.get("mp/aborted.bin")).toBeNull();
      await upload.abort();
    });

    test("multipart complete rejects a wrong part etag and a part that was never uploaded", async () => {
      const upload = await store.createMultipartUpload("mp/bad-etag.bin");
      await upload.uploadPart(1, textBytes("one"));
      await expect(upload.complete([{ partNumber: 1, etag: "deadbeef" }]))
        .rejects.toThrow("ETag");
      expect(await store.get("mp/bad-etag.bin")).toBeNull();

      const gapped = await store.createMultipartUpload("mp/gapped.bin");
      const first = await gapped.uploadPart(1, textBytes("one"));
      const second = await gapped.uploadPart(2, textBytes("two"));
      await expect(gapped.complete([first, second, { partNumber: 3, etag: "never-uploaded" }]))
        .rejects.toThrow("do not match");
      expect(await store.get("mp/gapped.bin")).toBeNull();
    });

    test("multipart rejects unknown uploads, empty parts, and duplicate parts", async () => {
      const ghost = store.resumeMultipartUpload("mp/ghost.bin", "missing-upload");
      await expect(ghost.uploadPart(1, textBytes("x"))).rejects.toThrow("not found");
      await expect(ghost.complete([{ partNumber: 1, etag: "x" }])).rejects.toThrow("not found");
      const upload = await store.createMultipartUpload("mp/empty-part.bin");
      await expect(upload.uploadPart(1, new Uint8Array(0))).rejects.toThrow("must not be empty");
      const part1 = await upload.uploadPart(1, textBytes("one"));
      await expect(upload.complete([part1, part1])).rejects.toThrow("unique");
    });

    test("multipart complete enforces the object size cap", async () => {
      const upload = await store.createMultipartUpload("mp/too-big.bin");
      const part1 = await upload.uploadPart(1, patternBytes(3000));
      const part2 = await upload.uploadPart(2, patternBytes(2000));
      const error = await upload.complete([part1, part2]).catch((caught) => caught);
      expect(error.code).toBe("d1_object_too_large");
      expect(await store.get("mp/too-big.bin")).toBeNull();
    });

    test("creating an upload sweeps multipart sessions older than the stale window", async () => {
      database.exec(
        "INSERT INTO d1_blob_multipart_uploads (upload_id, object_key, created_at) "
        + "VALUES ('stale-one', 'mp/stale.bin', datetime('now', '-49 hours'))",
      );
      database.exec(
        "INSERT INTO d1_blob_multipart_parts (upload_id, part_number, chunk_index, byte_size, etag, data) "
        + "VALUES ('stale-one', 1, 0, 1, 'x', X'00')",
      );
      await store.createMultipartUpload("mp/fresh.bin");
      expect(database.query("SELECT COUNT(*) AS count FROM d1_blob_multipart_uploads WHERE upload_id = 'stale-one'").get().count).toBe(0);
      expect(database.query("SELECT COUNT(*) AS count FROM d1_blob_multipart_parts WHERE upload_id = 'stale-one'").get().count).toBe(0);
    });
  });
};

runBlobStoreSuite("sqlite bytes", (db) => db);
runBlobStoreSuite("d1 array shape", asD1Shape);

describe("d1 blob store (production defaults)", () => {
  let database;
  let directory;

  beforeEach(async () => {
    database = new Database(":memory:");
    await applyMigrations(database);
    directory = await mkdtemp(join(tmpdir(), "edgeever-d1-blob-defaults-"));
  });

  afterEach(async () => {
    database.close();
    await rm(directory, { recursive: true, force: true });
  });

  test("round-trips a 2.5 MiB object at default 1 MiB chunks", async () => {
    const db = createSelfHostedStorageAdapter(database, directory).db;
    const store = createD1BlobStore(db);
    const bytes = patternBytes(2.5 * 1024 * 1024);
    await store.put("defaults/big.bin", bytes);
    const chunks = database
      .query("SELECT length(data) AS size FROM d1_blob_chunks WHERE object_key = 'defaults/big.bin' ORDER BY chunk_index")
      .all();
    expect(chunks.map((row) => row.size)).toEqual([1024 * 1024, 1024 * 1024, 512 * 1024]);
    const object = await store.get("defaults/big.bin");
    expect(object.size).toBe(bytes.byteLength);
    expect(await readBody(object.body)).toEqual(bytes);
    const ranged = await store.get("defaults/big.bin", { range: { offset: 1024 * 1024 - 10, length: 30 } });
    expect(await readBody(ranged.body)).toEqual(bytes.slice(1024 * 1024 - 10, 1024 * 1024 + 20));
  });

  test("streams a multipart upload at default chunk size", async () => {
    const db = createSelfHostedStorageAdapter(database, directory).db;
    const store = createD1BlobStore(db);
    const upload = await store.createMultipartUpload("defaults/mp.bin");
    const first = patternBytes(2 * 1024 * 1024);
    const second = patternBytes(512 * 1024);
    const part1 = await upload.uploadPart(1, streamOf(first, 65536));
    const part2 = await upload.uploadPart(2, streamOf(second, 65536));
    await upload.complete([part1, part2]);
    const object = await store.get("defaults/mp.bin");
    expect(await readBody(object.body)).toEqual(new Uint8Array([...first, ...second]));
  });

  test("streams a multipart upload from tiny pieces", async () => {
    const db = createSelfHostedStorageAdapter(database, directory).db;
    const store = createD1BlobStore(db);
    const upload = await store.createMultipartUpload("defaults/tiny-pieces.bin");
    const first = patternBytes(2 * 1024 * 1024 + 123);
    const second = patternBytes(777);
    const part1 = await upload.uploadPart(1, streamOf(first, 1024));
    const part2 = await upload.uploadPart(2, streamOf(second));
    await upload.complete([part1, part2]);
    const object = await store.get("defaults/tiny-pieces.bin");
    expect(await readBody(object.body)).toEqual(new Uint8Array([...first, ...second]));
  });
});

describe("d1 blob store migrations", () => {
  test("migration 0094 applies onto a database with 0001-0093 applied and keeps existing data", async () => {
    const database = new Database(":memory:");
    try {
      const applied = await applyMigrations(database, (name) => name < "0094_d1_blob_store.sql");
      expect(applied.length).toBeGreaterThan(0);
      expect(applied.at(-1)).toBe("0093_pending_resources.sql");
      const before = database
        .query("SELECT COUNT(*) AS count FROM object_storage_configs WHERE provider = 'builtin' AND is_active = 1")
        .get();
      expect(before.count).toBe(1);
      const added = await applyMigrations(database, (name) => name === "0094_d1_blob_store.sql");
      expect(added).toEqual(["0094_d1_blob_store.sql"]);
      const after = database
        .query("SELECT COUNT(*) AS count FROM object_storage_configs WHERE provider = 'builtin' AND is_active = 1")
        .get();
      expect(after.count).toBe(1);
      const tables = database
        .query("SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'd1_blob_%' ORDER BY name")
        .all()
        .map((row) => row.name);
      expect(tables).toEqual([
        "d1_blob_chunks",
        "d1_blob_multipart_parts",
        "d1_blob_multipart_uploads",
        "d1_blob_objects",
      ]);
      // Applying the migration twice is a no-op (IF NOT EXISTS).
      await applyMigrations(database, (name) => name === "0094_d1_blob_store.sql");
      const store = createD1BlobStore(
        createSelfHostedStorageAdapter(database, join(tmpdir(), "edgeever-d1-unused")).db,
      );
      await store.put("migrated.bin", textBytes("upgraded"));
      expect(await readBody((await store.get("migrated.bin")).body)).toEqual(textBytes("upgraded"));
    } finally {
      database.close();
    }
  });
});
