DROP TABLE IF EXISTS query_metrics CASCADE;
DROP TABLE IF EXISTS q3_hourly_errors CASCADE;
DROP TABLE IF EXISTS q2_top_resources CASCADE;
DROP TABLE IF EXISTS q1_daily_traffic CASCADE;
DROP TABLE IF EXISTS execution_metadata CASCADE;

CREATE TABLE execution_metadata (
    run_id VARCHAR(50) PRIMARY KEY,
    pipeline_name VARCHAR(20),
    total_batches INTEGER,
    batch_size INTEGER,
    avg_batch_size DOUBLE PRECISION,
    malformed_record_count BIGINT DEFAULT 0,
    runtime_ms BIGINT,
    execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- batch_id kept for ingestion traceability but not populated by query aggregations
CREATE TABLE q1_daily_traffic (
    run_id VARCHAR(50),
    log_date VARCHAR(15),
    status_code INTEGER,
    request_count BIGINT,
    total_bytes BIGINT,
    CONSTRAINT fk_q1_run FOREIGN KEY (run_id)
        REFERENCES execution_metadata(run_id)
        ON DELETE CASCADE
);

CREATE INDEX idx_q1_run ON q1_daily_traffic(run_id);

CREATE TABLE q2_top_resources (
    run_id VARCHAR(50),
    resource_path TEXT,
    request_count BIGINT,
    total_bytes BIGINT,
    distinct_host_count BIGINT,
    CONSTRAINT fk_q2_run FOREIGN KEY (run_id)
        REFERENCES execution_metadata(run_id)
        ON DELETE CASCADE
);

CREATE INDEX idx_q2_run ON q2_top_resources(run_id);

CREATE TABLE q3_hourly_errors (
    run_id VARCHAR(50),
    log_date VARCHAR(20),
    log_hour VARCHAR(5),
    error_request_count BIGINT,
    total_request_count BIGINT,
    error_rate DOUBLE PRECISION,
    distinct_error_hosts BIGINT,
    CONSTRAINT fk_q3_run FOREIGN KEY (run_id)
        REFERENCES execution_metadata(run_id)
        ON DELETE CASCADE
);

CREATE INDEX idx_q3_run ON q3_hourly_errors(run_id);

CREATE TABLE query_metrics (
    run_id VARCHAR(50),
    query_name VARCHAR(100),
    runtime_ms BIGINT,
    CONSTRAINT fk_metrics_run FOREIGN KEY (run_id)
        REFERENCES execution_metadata(run_id)
        ON DELETE CASCADE
);

CREATE INDEX idx_metrics_run ON query_metrics(run_id);