import { AppError } from "./app-error";
import type {
  BlobMultipartUploadAdapter,
  BlobObjectAdapter,
  BlobStoreAdapter,
  BlobUploadedPart,
  DatabaseAdapter,
  PreparedStatementAdapter,
} from "./storage-contract";

/**
 * D1-backed object storage for Cloudflare deployments without an R2 binding.
 *
 * Objects are split into chunks of at most 1 MiB because D1 rejects any row
 * larger than 2 MiB. Reads stream chunk windows so a download never
 * materializes the whole object, and writes go through chunk generations: a
 * new write stores its chunks under a fresh generation and only then swaps
 * the object row to point at it, so a failed or interrupted write leaves
 * only invisible orphan chunks that the next write of the same key removes.
 * Multipart sessions keep their parts chunked the same way; completion
 * moves the part bytes into the object with a single INSERT ... SELECT, so
 * no part data crosses the Worker. The service-level resource_uploads
 * tables remain the source of truth for which parts a client has sent.
 *
 * D1 read shapes differ by runtime: the Workers binding returns BLOB columns
 * as arrays of byte values, while local SQLite returns Uint8Array. Chunk
 * decoding accepts ArrayBuffer, views, and plain arrays alike.
 */

export const D1_BLOB_CHUNK_BYTES = 1024 * 1024;
export const D1_BLOB_MAX_OBJECT_BYTES = 100 * 1024 * 1024;

const D1_BLOB_READ_CHUNKS_PER_QUERY = 4;
const D1_BLOB_WRITE_CHUNKS_PER_BATCH = 4;
const D1_BLOB_MULTIPART_STALE_MS = 48 * 60 * 60 * 1000;
const D1_BLOB_DELETE_KEYS_PER_BATCH = 500;

export type D1BlobStoreOptions = {
  chunkBytes?: number;
  maxObjectBytes?: number;
  readChunksPerQuery?: number;
  writeChunksPerBatch?: number;
  staleMultipartMs?: number;
};

type ResolvedD1BlobStoreOptions = {
  chunkBytes: number;
  maxObjectBytes: number;
  readChunksPerQuery: number;
  writeChunksPerBatch: number;
  staleMultipartMs: number;
};

type BlobHttpMetadata = {
  contentType: string | null;
  cacheControl: string | null;
  contentDisposition: string | null;
  customMetadataJson: string;
};

type BlobObjectRow = {
  object_key: string;
  byte_size: number;
  chunk_count: number;
  generation: string;
  etag: string;
  content_type: string | null;
  cache_control: string | null;
  content_disposition: string | null;
  custom_metadata_json: string;
  created_at: string;
  updated_at: string;
};

type BlobChunkRow = {
  chunk_index: number;
  data: unknown;
};

type MultipartUploadRow = {
  upload_id: string;
  object_key: string;
  content_type: string | null;
  cache_control: string | null;
  content_disposition: string | null;
  custom_metadata_json: string;
  created_at: string;
};

type MultipartPartMetaRow = {
  part_number: number;
  chunk_index: number;
  byte_size: number;
  etag: string;
};

const formatByteSize = (bytes: number) => {
  if (bytes >= 1024 * 1024) return `${Math.round(bytes / 1024 / 1024)} MiB`;
  if (bytes >= 1024) return `${Math.round(bytes / 1024)} KiB`;
  return `${bytes} bytes`;
};

const d1ObjectTooLarge = (maxObjectBytes: number) => new AppError(
  "d1_object_too_large",
  `Objects stored in D1 must be at most ${formatByteSize(maxObjectBytes)}. `
    + "Use R2 or S3 object storage for larger files.",
  413,
);

const assertObjectKey = (objectKey: string) => {
  if (typeof objectKey !== "string" || objectKey.length === 0) {
    throw new Error("D1 blob object key must be a non-empty string.");
  }
};

const readMetadataField = (record: Record<string, unknown>, camel: string, kebab: string) => {
  const value = record[camel] ?? record[kebab];
  return typeof value === "string" && value.length > 0 ? value : null;
};

const parseBlobMetadata = (options: unknown): BlobHttpMetadata => {
  const root = options !== null && typeof options === "object"
    ? options as Record<string, unknown>
    : {};
  const http = root.httpMetadata !== null && typeof root.httpMetadata === "object"
    ? root.httpMetadata as Record<string, unknown>
    : {};
  return {
    contentType: readMetadataField(http, "contentType", "content-type"),
    cacheControl: readMetadataField(http, "cacheControl", "cache-control"),
    contentDisposition: readMetadataField(http, "contentDisposition", "content-disposition"),
    customMetadataJson: root.customMetadata === undefined ? "{}" : JSON.stringify(root.customMetadata ?? {}),
  };
};

const digestBytes = async (data: Uint8Array): Promise<Uint8Array> => {
  // Hash the backing buffer directly when the view covers it exactly;
  // otherwise slice() both copies the view's exact bytes and narrows the
  // type to Uint8Array<ArrayBuffer>, which subtle.digest requires.
  const source = data.byteOffset === 0 && data.buffer.byteLength === data.byteLength
    ? (data.buffer as ArrayBuffer)
    : data.slice();
  const digest = await crypto.subtle.digest("SHA-256", source);
  return new Uint8Array(digest);
};

const toHex = (bytes: Uint8Array) => Array.from(
  bytes,
  (byte) => byte.toString(16).padStart(2, "0"),
).join("");

const fromHex = (hex: string): Uint8Array => {
  if (hex.length % 2 !== 0 || !/^[0-9a-f]*$/i.test(hex)) {
    throw new Error("Invalid hex digest.");
  }
  const bytes = new Uint8Array(hex.length / 2);
  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
  }
  return bytes;
};

const compositeEtag = async (digests: Uint8Array[]): Promise<string> => {
  if (digests.length === 0) return toHex(await digestBytes(new Uint8Array(0)));
  if (digests.length === 1) return toHex(digests[0]);
  const joined = new Uint8Array(digests.length * 32);
  digests.forEach((digest, index) => joined.set(digest, index * 32));
  return `${toHex(await digestBytes(joined))}-${digests.length}`;
};

/** D1 binds binary data as an exact ArrayBuffer; views must not leak extra bytes. */
const toArrayBuffer = (bytes: Uint8Array): ArrayBuffer => {
  if (bytes.byteOffset === 0 && bytes.buffer.byteLength === bytes.byteLength) {
    return bytes.buffer as ArrayBuffer;
  }
  return bytes.slice().buffer as ArrayBuffer;
};

const decodeBlobValue = (value: unknown, what: string): Uint8Array => {
  if (value instanceof Uint8Array) return value;
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  if (Array.isArray(value)) return Uint8Array.from(value);
  throw new Error(`D1 blob ${what} has an unsupported value type.`);
};

const iterableOf = async function* (pieces: Uint8Array[]): AsyncIterable<Uint8Array> {
  yield* pieces;
};

const streamPieces = async function* (
  stream: ReadableStream<Uint8Array>,
): AsyncIterable<Uint8Array> {
  const reader = stream.getReader();
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      if (value.byteLength > 0) yield value;
    }
  } finally {
    reader.releaseLock();
  }
};

type NormalizedValue = {
  pieces: AsyncIterable<Uint8Array>;
  knownSize: number | null;
};

const normalizeValue = (value: unknown): NormalizedValue => {
  if (typeof value === "string") {
    const bytes = new TextEncoder().encode(value);
    return { pieces: iterableOf([bytes]), knownSize: bytes.byteLength };
  }
  if (value instanceof Uint8Array) {
    return { pieces: iterableOf([value]), knownSize: value.byteLength };
  }
  if (value instanceof ArrayBuffer) {
    const bytes = new Uint8Array(value);
    return { pieces: iterableOf([bytes]), knownSize: bytes.byteLength };
  }
  if (value instanceof Blob) {
    return { pieces: streamPieces(value.stream()), knownSize: value.size };
  }
  if (ArrayBuffer.isView(value)) {
    const bytes = new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
    return { pieces: iterableOf([bytes]), knownSize: bytes.byteLength };
  }
  if (value instanceof ReadableStream) {
    return { pieces: streamPieces(value), knownSize: null };
  }
  throw new Error("Unsupported D1 blob payload.");
};

/** Re-slices an arbitrary byte stream into fixed-size chunks with bounded memory. */
const splitIntoChunks = async function* (
  pieces: AsyncIterable<Uint8Array>,
  chunkBytes: number,
): AsyncGenerator<Uint8Array> {
  // Fill one preallocated buffer per chunk. The previous pending-combine
  // loop reallocated and recopied the partial chunk for every incoming
  // piece, which is quadratic when the runtime delivers small pieces and
  // blew the Workers Free CPU budget on 8 MiB part uploads (error 1102).
  // Every yielded chunk owns an exact-size buffer, so D1 binding needs no
  // further copy (see toArrayBuffer).
  let buffer = new Uint8Array(chunkBytes);
  let length = 0;
  for await (const piece of pieces) {
    let offset = 0;
    while (offset < piece.byteLength) {
      const take = Math.min(chunkBytes - length, piece.byteLength - offset);
      buffer.set(piece.subarray(offset, offset + take), length);
      length += take;
      offset += take;
      if (length === chunkBytes) {
        yield buffer;
        buffer = new Uint8Array(chunkBytes);
        length = 0;
      }
    }
  }
  if (length > 0) yield buffer.slice(0, length);
};

export const createD1BlobStore = (
  db: DatabaseAdapter,
  options: D1BlobStoreOptions = {},
): BlobStoreAdapter => {
  const resolved: ResolvedD1BlobStoreOptions = {
    chunkBytes: options.chunkBytes ?? D1_BLOB_CHUNK_BYTES,
    maxObjectBytes: options.maxObjectBytes ?? D1_BLOB_MAX_OBJECT_BYTES,
    readChunksPerQuery: options.readChunksPerQuery ?? D1_BLOB_READ_CHUNKS_PER_QUERY,
    writeChunksPerBatch: options.writeChunksPerBatch ?? D1_BLOB_WRITE_CHUNKS_PER_BATCH,
    staleMultipartMs: options.staleMultipartMs ?? D1_BLOB_MULTIPART_STALE_MS,
  };
  if (
    !Number.isInteger(resolved.chunkBytes)
    || resolved.chunkBytes < 1
    || resolved.chunkBytes > 2_000_000
  ) {
    throw new Error("D1 blob chunk size must be between 1 and 2,000,000 bytes.");
  }
  for (const [name, value] of [
    ["maxObjectBytes", resolved.maxObjectBytes],
    ["readChunksPerQuery", resolved.readChunksPerQuery],
    ["writeChunksPerBatch", resolved.writeChunksPerBatch],
    ["staleMultipartMs", resolved.staleMultipartMs],
  ] as const) {
    if (!Number.isInteger(value) || value < 1) {
      throw new Error(`D1 blob option ${name} must be a positive integer.`);
    }
  }

  /**
   * Stores chunked bytes under a fresh generation and swaps the object row to
   * it. Small objects land in a single atomic batch; larger ones flush chunk
   * batches progressively and finish with one atomic swap, so a failure can
   * only leave invisible orphan chunks.
   */
  const publishStatements = (
    objectKey: string,
    byteSize: number,
    chunkCount: number,
    generation: string,
    etag: string,
    metadata: BlobHttpMetadata,
  ): PreparedStatementAdapter[] => [
    db.prepare(
      "DELETE FROM d1_blob_chunks WHERE object_key = ? AND generation <> ?",
    ).bind(objectKey, generation),
    db.prepare(
      `INSERT INTO d1_blob_objects (
        object_key, byte_size, chunk_count, generation, etag, content_type,
        cache_control, content_disposition, custom_metadata_json, created_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
      ON CONFLICT(object_key) DO UPDATE SET
        byte_size = excluded.byte_size,
        chunk_count = excluded.chunk_count,
        generation = excluded.generation,
        etag = excluded.etag,
        content_type = excluded.content_type,
        cache_control = excluded.cache_control,
        content_disposition = excluded.content_disposition,
        custom_metadata_json = excluded.custom_metadata_json,
        updated_at = excluded.updated_at`,
    ).bind(
      objectKey,
      byteSize,
      chunkCount,
      generation,
      etag,
      metadata.contentType,
      metadata.cacheControl,
      metadata.contentDisposition,
      metadata.customMetadataJson,
    ),
  ];

  const writeObject = async (
    objectKey: string,
    pieces: AsyncIterable<Uint8Array>,
    metadata: BlobHttpMetadata,
  ): Promise<{ byteSize: number; chunkCount: number; etag: string }> => {
    const generation = crypto.randomUUID();
    const digests: Uint8Array[] = [];
    let byteSize = 0;
    let chunkIndex = 0;
    let pending: PreparedStatementAdapter[] = [];
    let flushed = false;
    for await (const chunk of splitIntoChunks(pieces, resolved.chunkBytes)) {
      byteSize += chunk.byteLength;
      if (byteSize > resolved.maxObjectBytes) {
        throw d1ObjectTooLarge(resolved.maxObjectBytes);
      }
      digests.push(await digestBytes(chunk));
      pending.push(
        db.prepare(
          "INSERT INTO d1_blob_chunks (object_key, generation, chunk_index, data) VALUES (?, ?, ?, ?)",
        ).bind(objectKey, generation, chunkIndex, toArrayBuffer(chunk)),
      );
      chunkIndex += 1;
      if (pending.length >= resolved.writeChunksPerBatch) {
        await db.batch(pending);
        pending = [];
        flushed = true;
      }
    }
    const etag = await compositeEtag(digests);
    const swap: PreparedStatementAdapter[] = publishStatements(
      objectKey, byteSize, chunkIndex, generation, etag, metadata,
    );
    if (!flushed) {
      await db.batch([...pending, ...swap]);
    } else {
      if (pending.length > 0) await db.batch(pending);
      await db.batch(swap);
    }
    return { byteSize, chunkCount: chunkIndex, etag };
  };

  const sweepStaleMultipartUploads = async () => {
    const cutoff = new Date(Date.now() - resolved.staleMultipartMs).toISOString();
    try {
      await db.batch([
        db.prepare(
          "DELETE FROM d1_blob_multipart_parts WHERE upload_id IN "
            + "(SELECT upload_id FROM d1_blob_multipart_uploads WHERE created_at < ?)",
        ).bind(cutoff),
        db.prepare(
          "DELETE FROM d1_blob_multipart_uploads WHERE created_at < ?",
        ).bind(cutoff),
      ]);
    } catch {
      // Best effort: a failed sweep must not block a new upload.
    }
  };

  const readMultipartUpload = (uploadId: string) => db.prepare(
    `SELECT upload_id, object_key, content_type, cache_control, content_disposition,
            custom_metadata_json, created_at
     FROM d1_blob_multipart_uploads WHERE upload_id = ?`,
  ).bind(uploadId).first<MultipartUploadRow>();

  const requireMultipartUpload = async (objectKey: string, uploadId: string) => {
    const upload = await readMultipartUpload(uploadId);
    if (!upload) throw new Error("Multipart upload was not found.");
    if (upload.object_key !== objectKey) {
      throw new Error("Multipart upload does not match the requested object key.");
    }
    return upload;
  };

  const multipartAdapter = (
    objectKey: string,
    uploadId: string,
  ): BlobMultipartUploadAdapter => ({
    uploadId,
    async uploadPart(partNumber, value) {
      if (!Number.isInteger(partNumber) || partNumber < 1) {
        throw new Error("Multipart part number must be a positive integer.");
      }
      await requireMultipartUpload(objectKey, uploadId);
      const normalized = normalizeValue(value);
      if (normalized.knownSize !== null && normalized.knownSize > resolved.maxObjectBytes) {
        throw d1ObjectTooLarge(resolved.maxObjectBytes);
      }
      // Stream the part through small D1 batches instead of buffering it all
      // and writing one giant batch: a multi-megabyte part written in a
      // single batch was killed on the Workers Free plan with error 1102
      // (the 128 MB isolate memory ceiling), while the same bytes written progressively
      // stay within a few MiB of peak heap. Rows carry placeholder
      // byte_size/etag until the stream ends, when one UPDATE stamps the
      // real part totals; a failed upload leaves only placeholder rows that
      // the next attempt for the same part deletes first, and the service
      // layer never records a part whose upload threw, so completion can
      // never observe them.
      const iterator = splitIntoChunks(normalized.pieces, resolved.chunkBytes)[
        Symbol.asyncIterator
      ]();
      const first = await iterator.next();
      if (first.done) throw new Error("Multipart upload part must not be empty.");
      await db.batch([
        db.prepare(
          "DELETE FROM d1_blob_multipart_parts WHERE upload_id = ? AND part_number = ?",
        ).bind(uploadId, partNumber),
      ]);
      const digests: Uint8Array[] = [];
      let pending: PreparedStatementAdapter[] = [];
      let byteSize = 0;
      let chunkIndex = 0;
      const consume = async (chunk: Uint8Array) => {
        byteSize += chunk.byteLength;
        if (byteSize > resolved.maxObjectBytes) {
          throw d1ObjectTooLarge(resolved.maxObjectBytes);
        }
        digests.push(await digestBytes(chunk));
        pending.push(
          db.prepare(
            `INSERT INTO d1_blob_multipart_parts
              (upload_id, part_number, chunk_index, byte_size, etag, data) VALUES (?, ?, ?, ?, ?, ?)`,
          ).bind(uploadId, partNumber, chunkIndex, 1, "pending", toArrayBuffer(chunk)),
        );
        chunkIndex += 1;
        if (pending.length >= resolved.writeChunksPerBatch) {
          await db.batch(pending);
          pending = [];
        }
      };
      await consume(first.value);
      for await (const chunk of iterator) {
        await consume(chunk);
      }
      const etag = await compositeEtag(digests);
      await db.batch([
        ...pending,
        db.prepare(
          "UPDATE d1_blob_multipart_parts SET byte_size = ?, etag = ? "
            + "WHERE upload_id = ? AND part_number = ?",
        ).bind(byteSize, etag, uploadId, partNumber),
      ]);
      return { partNumber, etag };
    },
    async complete(parts) {
      const upload = await requireMultipartUpload(objectKey, uploadId);
      if (parts.length === 0) {
        throw new Error("Multipart upload requires at least one part.");
      }
      const ordered = [...parts].sort((left, right) => left.partNumber - right.partNumber);
      for (const [index, part] of ordered.entries()) {
        if (!Number.isInteger(part.partNumber) || part.partNumber < 1) {
          throw new Error("Multipart part number must be a positive integer.");
        }
        if (index > 0 && part.partNumber <= ordered[index - 1].partNumber) {
          throw new Error("Multipart upload parts must have unique, ascending part numbers.");
        }
      }
      // Validate the stored parts from their metadata only, then move the
      // bytes inside D1 with INSERT ... SELECT: streaming every part through
      // the Worker (read + re-hash + re-write) was killed on the Workers Free
      // plan with error 1102 for a 20 MiB upload, while the metadata check
      // catches every failure the storage layer can produce (missing rows,
      // placeholder rows, inconsistent sizes, ETag mismatch) without
      // touching a byte of part data.
      const expectedEtags = new Map(ordered.map((part) => [part.partNumber, part.etag]));
      const stored = await db.prepare(
        `SELECT part_number, chunk_index, byte_size, etag
         FROM d1_blob_multipart_parts
         WHERE upload_id = ? ORDER BY part_number ASC, chunk_index ASC`,
      ).bind(uploadId).all<MultipartPartMetaRow>();
      let byteSize = 0;
      let chunkCount = 0;
      const partDigests: Uint8Array[] = [];
      let rowIndex = 0;
      for (const part of ordered) {
        let partRowCount = 0;
        let partByteSize: number | null = null;
        let partEtag: string | null = null;
        while (
          rowIndex < stored.results.length
          && stored.results[rowIndex].part_number === part.partNumber
        ) {
          const row = stored.results[rowIndex];
          rowIndex += 1;
          if (row.chunk_index !== partRowCount) {
            throw new Error(`Multipart upload part ${part.partNumber} is incomplete.`);
          }
          if (row.etag === "pending") {
            throw new Error(`Multipart upload part ${part.partNumber} is incomplete.`);
          }
          if (partByteSize === null) {
            partByteSize = row.byte_size;
            partEtag = row.etag;
          } else if (row.byte_size !== partByteSize || row.etag !== partEtag) {
            throw new Error(`Multipart upload part ${part.partNumber} has inconsistent metadata.`);
          }
          partRowCount += 1;
        }
        if (partRowCount === 0) {
          throw new Error("Multipart upload parts do not match the completed part list.");
        }
        if (partByteSize === null || partEtag === null) {
          throw new Error(`Multipart upload part ${part.partNumber} is incomplete.`);
        }
        if (partEtag !== expectedEtags.get(part.partNumber)) {
          throw new Error(`Multipart upload part ${part.partNumber} does not match its recorded ETag.`);
        }
        if (partRowCount !== Math.ceil(partByteSize / resolved.chunkBytes)) {
          throw new Error(`Multipart upload part ${part.partNumber} is incomplete.`);
        }
        byteSize += partByteSize;
        chunkCount += partRowCount;
        const digest = fromHex(partEtag.split("-")[0]);
        if (digest.byteLength !== 32) {
          throw new Error(`Multipart upload part ${part.partNumber} does not match its recorded ETag.`);
        }
        partDigests.push(digest);
      }
      if (rowIndex !== stored.results.length) {
        throw new Error("Multipart upload parts do not match the completed part list.");
      }
      if (byteSize > resolved.maxObjectBytes) {
        throw d1ObjectTooLarge(resolved.maxObjectBytes);
      }
      const etag = await compositeEtag(partDigests);
      const generation = crypto.randomUUID();
      const metadata: BlobHttpMetadata = {
        contentType: upload.content_type,
        cacheControl: upload.cache_control,
        contentDisposition: upload.content_disposition,
        customMetadataJson: upload.custom_metadata_json,
      };
      await db.batch([
        db.prepare(
          `INSERT INTO d1_blob_chunks (object_key, generation, chunk_index, data)
           SELECT ?, ?, ROW_NUMBER() OVER (ORDER BY part_number ASC, chunk_index ASC) - 1, data
           FROM d1_blob_multipart_parts WHERE upload_id = ?`,
        ).bind(objectKey, generation, uploadId),
        ...publishStatements(objectKey, byteSize, chunkCount, generation, etag, metadata),
        db.prepare(
          "DELETE FROM d1_blob_multipart_parts WHERE upload_id = ?",
        ).bind(uploadId),
        db.prepare(
          "DELETE FROM d1_blob_multipart_uploads WHERE upload_id = ?",
        ).bind(uploadId),
      ]);
      return { etag };
    },
    async abort() {
      await db.batch([
        db.prepare(
          "DELETE FROM d1_blob_multipart_parts WHERE upload_id = ?",
        ).bind(uploadId),
        db.prepare(
          "DELETE FROM d1_blob_multipart_uploads WHERE upload_id = ?",
        ).bind(uploadId),
      ]);
    },
  });

  return {
    async get(objectKey, getOptions): Promise<BlobObjectAdapter | null> {
      assertObjectKey(objectKey);
      const row = await db.prepare(
        `SELECT object_key, byte_size, chunk_count, generation, etag, content_type,
                cache_control, content_disposition, custom_metadata_json, created_at, updated_at
         FROM d1_blob_objects WHERE object_key = ?`,
      ).bind(objectKey).first<BlobObjectRow>();
      if (!row) return null;

      const totalSize = row.byte_size;
      const requested = getOptions?.range;
      const offset = requested ? Math.min(Math.max(0, requested.offset), totalSize) : 0;
      const length = requested
        ? Math.max(0, Math.min(requested.length, totalSize - offset))
        : totalSize;
      const end = offset + length;
      // Chunk boundaries are not uniform: multipart completion moves each
      // part's chunks verbatim, so a part that is not a multiple of the chunk
      // size leaves a short chunk mid-object. Resolve byte offsets from the
      // stored chunk lengths instead of assuming fixed strides. The lookup
      // runs lazily on first pull: a corrupt object fails when its body is
      // read, not when it is opened.
      let chunkStarts: number[] = [];
      let firstChunk = 0;
      let lastChunk = -1;
      let nextChunk = 0;
      let layoutLoaded = false;
      const loadLayout = async () => {
        if (layoutLoaded) return;
        layoutLoaded = true;
        const chunkLengths = await db.prepare(
          `SELECT chunk_index, length(data) AS size FROM d1_blob_chunks
           WHERE object_key = ? AND generation = ? ORDER BY chunk_index ASC`,
        ).bind(objectKey, row.generation).all<{ chunk_index: number; size: number }>();
        let storedBytes = 0;
        for (const [position, chunkRow] of chunkLengths.results.entries()) {
          if (chunkRow.chunk_index !== position) {
            throw new Error(`D1 blob object "${objectKey}" is incomplete: missing chunk ${position}.`);
          }
          chunkStarts.push(storedBytes);
          storedBytes += chunkRow.size;
        }
        if (storedBytes !== totalSize) {
          throw new Error(
            `D1 blob object "${objectKey}" is incomplete: expected ${totalSize} bytes, found ${storedBytes}.`,
          );
        }
        for (let index = 0; index < chunkStarts.length; index += 1) {
          if (chunkStarts[index] <= offset) firstChunk = index;
          if (chunkStarts[index] < end) lastChunk = index;
        }
        nextChunk = firstChunk;
      };
      let buffered: Array<{ index: number; bytes: Uint8Array }> = [];
      let finished = false;

      const body = new ReadableStream<Uint8Array>({
        async pull(controller) {
          if (length === 0) {
            controller.close();
            finished = true;
            return;
          }
          await loadLayout();
          if (buffered.length === 0) {
            if (finished || nextChunk > lastChunk) {
              controller.close();
              finished = true;
              return;
            }
            const windowEnd = Math.min(nextChunk + resolved.readChunksPerQuery - 1, lastChunk);
            const rows = await db.prepare(
              `SELECT chunk_index, data FROM d1_blob_chunks
               WHERE object_key = ? AND generation = ? AND chunk_index BETWEEN ? AND ?
               ORDER BY chunk_index ASC`,
            ).bind(objectKey, row.generation, nextChunk, windowEnd).all<BlobChunkRow>();
            const expected = windowEnd - nextChunk + 1;
            if (rows.results.length !== expected) {
              throw new Error(
                `D1 blob object "${objectKey}" is incomplete: expected ${expected} chunks, found ${rows.results.length}.`,
              );
            }
            buffered = rows.results.map((chunkRow, position) => {
              if (chunkRow.chunk_index !== nextChunk + position) {
                throw new Error(`D1 blob object "${objectKey}" is incomplete: missing chunk ${nextChunk + position}.`);
              }
              return { index: chunkRow.chunk_index, bytes: decodeBlobValue(chunkRow.data, "chunk") };
            });
            nextChunk = windowEnd + 1;
          }
          const next = buffered.shift();
          if (!next) {
            controller.close();
            finished = true;
            return;
          }
          const chunkStart = chunkStarts[next.index];
          const sliceStart = Math.max(0, offset - chunkStart);
          const sliceEnd = Math.min(next.bytes.byteLength, end - chunkStart);
          controller.enqueue(next.bytes.slice(sliceStart, sliceEnd));
        },
        cancel() {
          finished = true;
        },
      });

      const lastModified = new Date(row.updated_at);
      return {
        body,
        size: totalSize,
        ...(requested ? { range: { offset, length } } : {}),
        writeHttpMetadata: (headers) => {
          if (row.content_type) headers.set("Content-Type", row.content_type);
          if (row.cache_control) headers.set("Cache-Control", row.cache_control);
          if (row.content_disposition) headers.set("Content-Disposition", row.content_disposition);
          headers.set("ETag", `"${row.etag}"`);
          if (!Number.isNaN(lastModified.getTime())) {
            headers.set("Last-Modified", lastModified.toUTCString());
          }
        },
      };
    },

    async put(objectKey, value, putOptions) {
      assertObjectKey(objectKey);
      const metadata = parseBlobMetadata(putOptions);
      const normalized = normalizeValue(value);
      if (normalized.knownSize !== null && normalized.knownSize > resolved.maxObjectBytes) {
        throw d1ObjectTooLarge(resolved.maxObjectBytes);
      }
      const written = await writeObject(objectKey, normalized.pieces, metadata);
      return { etag: written.etag };
    },

    async createMultipartUpload(objectKey, createOptions) {
      assertObjectKey(objectKey);
      const metadata = parseBlobMetadata(createOptions);
      const uploadId = crypto.randomUUID();
      await sweepStaleMultipartUploads();
      await db.prepare(
        `INSERT INTO d1_blob_multipart_uploads
          (upload_id, object_key, content_type, cache_control, content_disposition, custom_metadata_json)
         VALUES (?, ?, ?, ?, ?, ?)`,
      ).bind(
        uploadId,
        objectKey,
        metadata.contentType,
        metadata.cacheControl,
        metadata.contentDisposition,
        metadata.customMetadataJson,
      ).run();
      return multipartAdapter(objectKey, uploadId);
    },

    resumeMultipartUpload(objectKey, uploadId) {
      assertObjectKey(objectKey);
      return multipartAdapter(objectKey, uploadId);
    },

    async delete(keys) {
      const unique = [...new Set(
        (Array.isArray(keys) ? keys : [keys])
          .filter((key): key is string => typeof key === "string" && key.length > 0),
      )];
      for (let index = 0; index < unique.length; index += D1_BLOB_DELETE_KEYS_PER_BATCH) {
        const batchKeys = unique.slice(index, index + D1_BLOB_DELETE_KEYS_PER_BATCH);
        const placeholders = batchKeys.map(() => "?").join(", ");
        await db.batch([
          db.prepare(
            `DELETE FROM d1_blob_chunks WHERE object_key IN (${placeholders})`,
          ).bind(...batchKeys),
          db.prepare(
            `DELETE FROM d1_blob_objects WHERE object_key IN (${placeholders})`,
          ).bind(...batchKeys),
        ]);
      }
    },
  };
};
