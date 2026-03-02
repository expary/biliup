CREATE TABLE IF NOT EXISTS youtube_jobs
(
    id                    INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    name                  VARCHAR                           NOT NULL UNIQUE,
    source_url            TEXT                              NOT NULL,
    source_type           TEXT                              NOT NULL,
    upload_streamer_id    INTEGER                           NOT NULL
        REFERENCES uploadstreamers (id)
            ON DELETE CASCADE,
    enabled               INTEGER                           NOT NULL DEFAULT 1,
    sync_interval_seconds INTEGER                           NOT NULL DEFAULT 1800,
    auto_publish          INTEGER                           NOT NULL DEFAULT 1,
    backfill_mode         TEXT                              NOT NULL DEFAULT 'all',
    status                TEXT                              NOT NULL DEFAULT 'idle',
    last_sync_at          INTEGER,
    next_sync_at          INTEGER,
    last_error            TEXT,
    created_at            INTEGER                           NOT NULL DEFAULT (strftime('%s', 'now')),
    updated_at            INTEGER                           NOT NULL DEFAULT (strftime('%s', 'now'))
);

CREATE TABLE IF NOT EXISTS youtube_items
(
    id                    INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    job_id                INTEGER                           NOT NULL
        REFERENCES youtube_jobs (id)
            ON DELETE CASCADE,
    video_id              VARCHAR                           NOT NULL,
    video_url             TEXT                              NOT NULL,
    channel_id            VARCHAR,
    source_title          TEXT,
    source_description    TEXT,
    source_tags           JSON,
    thumbnail_url         TEXT,
    upload_date           VARCHAR,
    duration_sec          INTEGER,
    raw_metadata          JSON,
    generated_title       TEXT,
    generated_description TEXT,
    generated_tags        JSON,
    local_file_path       TEXT,
    transcoded_file_path  TEXT,
    status                TEXT                              NOT NULL DEFAULT 'discovered',
    retry_count           INTEGER                           NOT NULL DEFAULT 0,
    last_error            TEXT,
    bili_aid              INTEGER,
    bili_bvid             VARCHAR,
    created_at            INTEGER                           NOT NULL DEFAULT (strftime('%s', 'now')),
    updated_at            INTEGER                           NOT NULL DEFAULT (strftime('%s', 'now')),
    uploaded_at           INTEGER,
    CONSTRAINT uq_youtube_items_job_video UNIQUE (job_id, video_id)
);

CREATE TABLE IF NOT EXISTS youtube_uploaded_videos
(
    video_id         VARCHAR PRIMARY KEY NOT NULL,
    youtube_item_id  INTEGER
        REFERENCES youtube_items (id)
            ON DELETE SET NULL,
    bili_aid         INTEGER,
    bili_bvid        VARCHAR,
    uploaded_at      INTEGER             NOT NULL DEFAULT (strftime('%s', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_youtube_items_job_status
    ON youtube_items (job_id, status);

CREATE INDEX IF NOT EXISTS idx_youtube_items_video_id
    ON youtube_items (video_id);

CREATE INDEX IF NOT EXISTS idx_youtube_jobs_enabled_next_sync
    ON youtube_jobs (enabled, next_sync_at);
