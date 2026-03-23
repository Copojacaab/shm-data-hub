CREATE TABLE gateways_auth(
    id               SERIAL PRIMARY KEY,
    client_id        TEXT UNIQUE NOT NULL,   
    secret_hash      TEXT NOT NULL,          
    is_active        BOOLEAN DEFAULT TRUE,
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

-- Inserisci qui l'hash generato con il tuo script porcodio.py
INSERT INTO gateways_auth (client_id, secret_hash)
VALUES ('gw01', '$argon2id$v=19$m=65536,t=3,p=4$wagCPXjifgvUFBzq4hqe3w$CYaIb8sB+wtD+Vu/P4uod1+Qof8h+1g7bbDlBID48Rc');

CREATE TABLE projects (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL,
    location        TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO projects (id, name) VALUES (1, 'Default');

CREATE TABLE devices (
    id              SERIAL PRIMARY KEY,
    project_id      INT REFERENCES projects(id),
    mac             CHAR(64) UNIQUE NOT NULL,
    logical_name    TEXT,
    is_active       BOOLEAN DEFAULT TRUE,
    last_sync       TIMESTAMPTZ
);

CREATE TABLE shm_logs(
    id              SERIAL PRIMARY KEY,
    device_id       INT REFERENCES devices(id),
    start_time      TIMESTAMPTZ NOT NULL,       
    axis            CHAR(1) NOT NULL,
    fs              DOUBLE PRECISION, 
    sensitivity     TEXT,
    og_filename     TEXT,                       
    b2_url          TEXT                        
);

CREATE TABLE shm_metrics(
    time            TIMESTAMPTZ NOT NULL,
    log_id          INT REFERENCES shm_logs(id),
    temperature     DOUBLE PRECISION,
    humidity        DOUBLE PRECISION,
    tilt_phi        DOUBLE PRECISION,
    tilt_theta      DOUBLE PRECISION,
    rms_value       DOUBLE PRECISION,
    peak_freqs      DOUBLE PRECISION[],
    peak_mags       DOUBLE PRECISION[]
);

CREATE TABLE shm_raw(
    time            TIMESTAMPTZ NOT NULL,
    log_id          INT REFERENCES shm_logs(id),
    acc_val         DOUBLE PRECISION NOT NULL
);

SELECT create_hypertable('shm_metrics', 'time');
SELECT create_hypertable('shm_raw', 'time');
CREATE INDEX ON shm_raw (log_id, time DESC);