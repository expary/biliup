CREATE TABLE IF NOT EXISTS youtube_job_logs
(
    id         INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    job_id     INTEGER                           NOT NULL
        REFERENCES youtube_jobs (id)
            ON DELETE CASCADE,
    message    TEXT                              NOT NULL,
    created_at INTEGER                           NOT NULL DEFAULT (strftime('%s', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_youtube_job_logs_job_id_created_at
    ON youtube_job_logs (job_id, created_at, id);

ALTER TABLE uploadstreamers
    ADD COLUMN youtube_mark_source_link INTEGER DEFAULT 0;

ALTER TABLE uploadstreamers
    ADD COLUMN youtube_mark_source_channel INTEGER DEFAULT 0;
