-- D1-backed object storage for Cloudflare deployments without an R2 binding.
-- Objects are split into chunks of at most 1 MiB: D1 rejects any row larger
-- than 2 MiB, and a chunked layout keeps every read and write well under the
-- per-query result and per-invocation limits. The object row points at the
-- live chunk generation, so a failed write leaves only invisible orphan chunks
-- that the next write of the same key removes. Multipart sessions keep their
-- parts chunked the same way; the service-level resource_uploads tables remain
-- the source of truth for which parts a client has sent.
CREATE TABLE IF NOT EXISTS d1_blob_objects (
  object_key TEXT PRIMARY KEY,
  byte_size INTEGER NOT NULL CHECK (byte_size >= 0),
  chunk_count INTEGER NOT NULL CHECK (chunk_count >= 0),
  generation TEXT NOT NULL CHECK (length(generation) > 0),
  etag TEXT NOT NULL CHECK (length(etag) > 0),
  content_type TEXT,
  cache_control TEXT,
  content_disposition TEXT,
  custom_metadata_json TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(custom_metadata_json)),
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS d1_blob_chunks (
  object_key TEXT NOT NULL,
  generation TEXT NOT NULL,
  chunk_index INTEGER NOT NULL CHECK (chunk_index >= 0),
  data BLOB NOT NULL,
  PRIMARY KEY (object_key, generation, chunk_index)
);

CREATE TABLE IF NOT EXISTS d1_blob_multipart_uploads (
  upload_id TEXT PRIMARY KEY,
  object_key TEXT NOT NULL,
  content_type TEXT,
  cache_control TEXT,
  content_disposition TEXT,
  custom_metadata_json TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(custom_metadata_json)),
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_d1_blob_multipart_uploads_created
  ON d1_blob_multipart_uploads (created_at);

CREATE TABLE IF NOT EXISTS d1_blob_multipart_parts (
  upload_id TEXT NOT NULL,
  part_number INTEGER NOT NULL CHECK (part_number > 0),
  chunk_index INTEGER NOT NULL CHECK (chunk_index >= 0),
  byte_size INTEGER NOT NULL CHECK (byte_size > 0),
  etag TEXT NOT NULL CHECK (length(etag) > 0),
  data BLOB NOT NULL,
  PRIMARY KEY (upload_id, part_number, chunk_index),
  FOREIGN KEY (upload_id) REFERENCES d1_blob_multipart_uploads (upload_id) ON DELETE CASCADE
);
