import os
import jwt 
import psycopg2
import secrets
from datetime import datetime, timedelta, timezone
from typing import List, Annotated
from psycopg2 import pool
from psycopg2.extras import execute_values
from fastapi import FastAPI, HTTPException, status, Depends, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.security import OAuth2PasswordBearer
from fastapi.exceptions import RequestValidationError
from pwdlib import PasswordHash
from pydantic import BaseModel

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config
from io import BytesIO

# ===========================================
#       CONFIGURAZIONI E ENV VAR
# ===========================================

SECRET_KEY = os.getenv("API_SECRET_KEY", "b57ce0e0cac515ec6f7af0cf4aef1911ec1f66a270523f2f5335eba73740995f")
MASTER_REGISTRATION_KEY = os.getenv("MASTER_REGISTRATION_KEY", "master_key")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTE = 60

password_hash = PasswordHash.recommended()
auth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

DB_HOST = os.getenv("DB_HOST", "timescaledb")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "wisepower2026")

B2_ENDPOINT = os.getenv("B2_ENDPOINT")
B2_KEY_ID = os.getenv("B2_KEY_ID")
B2_APP_KEY = os.getenv("B2_APP_KEY")
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")

DEFAULT_PROJECT_ID = 1

timescale_pool = psycopg2.pool.ThreadedConnectionPool(
    1, 20,
    host=DB_HOST, database=DB_NAME,
    user=DB_USER, password=DB_PASS
)

s3_client = boto3.client(
    's3',
    endpoint_url=B2_ENDPOINT,
    aws_access_key_id=B2_KEY_ID,
    aws_secret_access_key=B2_APP_KEY,
    config=Config(signature_version='s3v4', max_pool_connections=20)
)

# =============================================
#       MODELLI DEI DATI (Pydantic)
# =============================================

# Modello per la prima registrazione (generazione secret)
class GatewayRegister(BaseModel):
    client_id: str                      #mac
    registration_token: str             #master key
# Modello per la risposta (restituisce la secret generata) forse uso GatewayAuth
# class GatewayRegisterResponse(BaseModel):
#     client_id: str
#     client_secret: str
# Modello per l'autenticazione (dopo register)
class GatewayAuth(BaseModel):
    client_id: str
    client_secret: str

class Token(BaseModel):
    access_token: str
    token_type: str

class ShmMetrics(BaseModel):
    temp: float
    humidity: float
    phi: float                      
    theta: float                    
    rms_asse: float
    fft_freqs: List[float]          
    fft_mags: List[float]           

class ShmPayload(BaseModel):
    mac: str
    timestamp: datetime             
    asse: str
    fs: float
    sensitivity: str
    metriche: ShmMetrics
    samples: List[float] 

# ====================================
#               HELPER
# ===================================

def _get_timescale_connection():
    return timescale_pool.getconn()

def _release_timescale_connection(conn):
    timescale_pool.putconn(conn)
    
def get_gateway_from_db(client_id: str):
    conn = _get_timescale_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT secret_hash, is_active FROM gateways_auth WHERE client_id = %s",
                (client_id,)
            )
            return cursor.fetchone()
    finally:
        _release_timescale_connection(conn)

# ===========================================
#           SUPPORTO (Backup e Storage)
# ===========================================

def upload_to_backblaze_parquet(payload_dict: dict, project_id: int):
    try:
        ts_start = datetime.fromisoformat(payload_dict['timestamp'])
        interval = 1.0 / payload_dict['fs']
        times = [ts_start + timedelta(seconds=i * interval) for i in range(len(payload_dict['samples']))]
        
        df = pd.DataFrame({'time': times,'acc_val': payload_dict['samples']})
        table = pa.Table.from_pandas(df)

        out_buffer = BytesIO()
        pq.write_table(table, out_buffer, compression='SNAPPY')
        out_buffer.seek(0)
        
        ts_str = ts_start.strftime("%Y-%m-%dT%H-%M-%S")
        remote_path = f"progetto_{project_id}/{payload_dict['mac']}/{ts_str}_{payload_dict['asse']}.parquet"
        
        s3_client.upload_fileobj(out_buffer, os.getenv("B2_BUCKET_NAME"), remote_path)          
        print(f"Salvato file Parquet in cold storage: {remote_path}")
    except Exception as e:
        print(f"[B2 ERROR] Impossibile completare il backup sul cloud: {str(e)}")

def process_heavy_data_async(payload_dict: dict, log_id: int, project_id: int):
    connection = None
    try:
        connection = _get_timescale_connection()
        cursor = connection.cursor()
        ts_start = datetime.fromisoformat(payload_dict['timestamp'])
        interval = 1.0 / payload_dict['fs']

        raw_data = [
            (ts_start + timedelta(seconds=i * interval), log_id, value)
            for i, value in enumerate(payload_dict['samples'])
        ]

        sql_insert_raw = "INSERT INTO shm_raw (time, log_id, acc_val) VALUES %s;"
        execute_values(cursor, sql_insert_raw, raw_data)
        
        sql_update_status = "UPDATE shm_logs SET status = 'completed' WHERE id=%s"
        cursor.execute(sql_update_status, (log_id,))
        
        connection.commit()
        upload_to_backblaze_parquet(payload_dict, project_id)
    except Exception as e:
        print(f"[ERRORE BACK API] Errore nel processamento asincrono (LogID: {log_id}: {str(e)})")
    finally:
        if connection: _release_timescale_connection(connection)

# ==============================
#       UTILITY SICUREZZA
# ==============================

def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTE)
    to_encode.update({"exp": int(expire.timestamp())})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

async def verify_gw(token: Annotated[str, Depends(auth2_scheme)]):
    credential_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Token non valido o scaduto",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        client_id: str = payload.get("sub")
        
        res = get_gateway_from_db(client_id)
        if res is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Gateway non presente nella lista degli utenti",
                headers={"WWW-Authenticate": "Bearer"},
            )
        if client_id is None or not res[1]:
            raise credential_exception
        return client_id
    except jwt.PyJWTError:
        raise credential_exception

# ==============================
#           ROUTES
# ==============================

app = FastAPI(title="SHM Data Ingestor")

@app.post("/register", response_model=GatewayAuth)
async def register_new_gateway(reg: GatewayRegister):
    # 1. Verifica masterkey
    if reg.registration_token != MASTER_REGISTRATION_KEY:
        raise HTTPException(status_code=403, detail="MasterKey non valida")
    
    # 2. Check per evitare doppioni nel db
    is_registered = get_gateway_from_db(reg.client_id)
    if is_registered:
        raise HTTPException(status_code=400, detail="Gateway gia' registrato")
    
    # 3. Genero client_secret univoca (mac)
    new_secret = secrets.token_urlsafe(32)
    
    # 4. Hashing per salvataggio in db
    hashed_secret = password_hash.hash(new_secret)
    
    # 5. Inserimento nel db
    conn = _get_timescale_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO gateways_auth (client_id, secret_hash) VALUES (%s, %s)",
                (reg.client_id, hashed_secret)
            )
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Errore database: {str(e)}")
    finally:
        _release_timescale_connection(conn)
    
    # 6. Risposta al gw
    return {
        "client_id": reg.client_id,
        "client_secret": new_secret
    }

@app.post("/token", response_model=Token)
async def generate_gw_token(auth: GatewayAuth):
    gw = get_gateway_from_db(auth.client_id)
    if not gw:
        raise HTTPException(status_code=401, detail="Credenziali Gateway Errate")
    db_secret_hash, is_active = gw
    if not password_hash.verify(auth.client_secret, db_secret_hash):
        raise HTTPException(status_code=401, detail="Credenziali Gateway Errate")
    if not is_active:
        raise HTTPException(status_code=400, detail="Gateway disabilitato")
    access_token = create_access_token(data={"sub": auth.client_id})
    return {"access_token": access_token, "token_type": "bearer"}

@app.post('/ingest')
def ingest_sensor_data(payload: ShmPayload, background_tasks: BackgroundTasks, gw_id : Annotated[str, Depends(verify_gw)]):
    conn = None
    try:
        conn = _get_timescale_connection()
        cursor = conn.cursor()
        
        sql_device = """
            INSERT INTO devices (mac, project_id, logical_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (mac) DO UPDATE SET last_sync = NOW()
            RETURNING id, project_id;
        """
        cursor.execute(sql_device, (payload.mac, DEFAULT_PROJECT_ID, f"Sensor {payload.mac[-5:]}"))
        res = cursor.fetchone()
        device_id, current_project_id = res

        sql_logs = """
            INSERT INTO shm_logs (device_id, start_time, axis, fs, sensitivity)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT ON CONSTRAINT unique_log_session DO UPDATE SET fs = EXCLUDED.fs
            RETURNING id;
        """
        cursor.execute(sql_logs, (device_id, payload.timestamp, payload.asse, payload.fs, payload.sensitivity))
        log_id = cursor.fetchone()[0]

        sql_metrics = """
            INSERT INTO shm_metrics (time, log_id, temperature, humidity, tilt_phi, tilt_theta, rms_value, peak_freqs, peak_mags)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        cursor.execute(sql_metrics, (payload.timestamp, log_id, payload.metriche.temp, payload.metriche.humidity, payload.metriche.phi, payload.metriche.theta, payload.metriche.rms_asse, payload.metriche.fft_freqs, payload.metriche.fft_mags))

        conn.commit()
        print(f"Ricevuti dati autenticati da {gw_id}")
        background_tasks.add_task(process_heavy_data_async, payload.model_dump(mode='json'), log_id, current_project_id)

        return {"status": "success", "log_id": log_id, "device_id": device_id, "samples": len(payload.samples)}
    except Exception as e:
        if conn: conn.rollback()
        raise HTTPException(status_code=500, detail=f"Errore interno: {str(e)}")
    finally:
        if conn: conn.close()

@app.exception_handler(RequestValidationError)
def validation_exception_handler(request, exc):
    error_details = [{"campo": error['loc'], "messaggio": error['msg'], "tipo": error["type"]} for error in exc.errors()]
    return JSONResponse(status_code=422, content={"detail": error_details})