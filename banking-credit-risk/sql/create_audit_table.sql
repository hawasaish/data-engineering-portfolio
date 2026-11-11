CREATE TABLE IF NOT EXISTS risk.pipeline_audit (

    audit_id BIGSERIAL PRIMARY KEY,

    run_id VARCHAR(100) NOT NULL,

    pipeline_name VARCHAR(200) NOT NULL,

    task_name VARCHAR(200) NOT NULL,

    start_time TIMESTAMP NOT NULL,

    end_time TIMESTAMP,

    status VARCHAR(30) NOT NULL,

    records_processed BIGINT,

    error_message TEXT
);