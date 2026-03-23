ALTER TABLE shm_logs ADD COLUMN status VARCHAR(20) DEFAULT 'pending';
ALTER TABLE shm_logs ADD CONSTRAINT unique_log_session UNIQUE (device_id, start_time, axis); 

SELECT create_hypertable('shm_raw', 'time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

ALTER TABLE shm_raw SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'log_id',
  timescaledb.compress_orderby = 'time DESC'
);

SELECT add_compression_policy('shm_raw', INTERVAL '7 days');
SELECT add_retention_policy('shm_raw', INTERVAL '6 months');