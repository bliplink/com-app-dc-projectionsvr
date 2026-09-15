CREATE TABLE IF NOT EXISTS dc_trade_projection_watermark (
    partition_id VARCHAR(128) NOT NULL,
    source_epoch BIGINT NOT NULL DEFAULT 0,
    journal_seq BIGINT NOT NULL DEFAULT 0,
    event_id VARCHAR(256) NULL,
    update_time DATETIME(3) NOT NULL,
    PRIMARY KEY (partition_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dc_trade_projection_event (
    event_id VARCHAR(256) NOT NULL,
    partition_id VARCHAR(128) NOT NULL,
    source_epoch BIGINT NOT NULL,
    journal_seq BIGINT NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    source_node VARCHAR(128) NULL,
    event_time BIGINT NOT NULL,
    payload LONGTEXT NOT NULL,
    create_time DATETIME(3) NOT NULL,
    PRIMARY KEY (event_id),
    UNIQUE KEY uk_trade_projection_partition_seq (partition_id, source_epoch, journal_seq)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dc_trade_projection_mutation (
    event_id VARCHAR(256) NOT NULL,
    mutation_index INT NOT NULL,
    partition_id VARCHAR(128) NOT NULL,
    source_epoch BIGINT NOT NULL,
    journal_seq BIGINT NOT NULL,
    location VARCHAR(128) NOT NULL,
    source_type VARCHAR(64) NULL,
    request_id VARCHAR(256) NULL,
    entity_type VARCHAR(64) NOT NULL,
    operation_type VARCHAR(32) NOT NULL,
    entity_key VARCHAR(512) NULL,
    payload LONGTEXT NULL,
    create_time DATETIME(3) NOT NULL,
    PRIMARY KEY (event_id, mutation_index),
    KEY idx_trade_projection_mutation_entity (entity_type, entity_key),
    KEY idx_trade_projection_mutation_location (location, journal_seq)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
