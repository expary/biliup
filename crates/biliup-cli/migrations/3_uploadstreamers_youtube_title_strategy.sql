ALTER TABLE uploadstreamers
    ADD COLUMN youtube_title_strategy VARCHAR;

ALTER TABLE uploadstreamers
    ADD COLUMN youtube_title_strategy_prompt TEXT;
