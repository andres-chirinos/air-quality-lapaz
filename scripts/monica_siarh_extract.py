#!/usr/bin/env python3
import pandas as pd
import argparse
import datetime
import hashlib
import requests
import json
import os
import time
import logging
import random
import urllib3
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.filterwarnings('ignore')
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

COLUMNS_FILTERS = [
    "valor", "valor_ica",
    "fecha_hora_registro", "fecha_hora_calculo", "observaciones",
    "latitude", "longitude", "lugar_nombre"
]

METADATA_COLUMNS = [
    "_metadata_source", "_metadata_request_status",
    "_metadata_timestamp", "_metadata_unix_timestamp",
    "_metadata_hash"
]

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "es,en;q=0.9",
    "Connection": "keep-alive",
    "DNT": "1",
    "Priority": "u=0, i",
    "Sec-GPC": "1",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:149.0) Gecko/20100101 Firefox/149.0"
}

BASE_URL = "https://monica.siarh.gob.bo/v1/api"


def fetch_data(url, method='GET', retries=5, base_delay=2.0):
    """Fetch JSON data and generate metadata for a given URL, with exponential backoff and jitter."""
    for attempt in range(1, retries + 1):
        try:
            if method.upper() == 'POST':
                response = requests.post(
                    url, timeout=30, headers=HEADERS, verify=False)
            else:
                response = requests.get(
                    url, timeout=30, headers=HEADERS, verify=False)

            if response.status_code in [200, 201]:
                data = response.json()
                metadata = {
                    "_metadata_source": url,
                    "_metadata_request_status": response.status_code,
                    "_metadata_timestamp": datetime.datetime.now().isoformat(),
                    "_metadata_unix_timestamp": int(datetime.datetime.now().timestamp()),
                    "_metadata_hash": hashlib.md5(json.dumps(data).encode("utf-8")).hexdigest() if data else ""
                }
                return data, metadata
            elif response.status_code == 500:
                logging.warning(
                    f"Intento {attempt}/{retries}: Error 500 devuelto por {url}")
            else:
                logging.warning(
                    f"Intento {attempt}/{retries}: Código HTTP {response.status_code} para {url}")
        except Exception as e:
            logging.warning(
                f"Intento {attempt}/{retries}: Error de conexión en {url}: {e}")

        if attempt < retries:
            sleep_time = (base_delay ** attempt) + random.uniform(0.1, 1.0)
            time.sleep(sleep_time)

    logging.error(f"Se agotaron los {retries} intentos para {url}.")
    return None, None


def save_dataframe(df, base_filename, data_dir, execution_id, output_formats):
    """Save concatenated DataFrame to Disk in requested formats."""
    if df.empty:
        logging.info(f"No data to save for {base_filename}")
        return

    for fmt in output_formats:
        fmt = fmt.strip().lower()
        filename = f"{base_filename}_{execution_id}.{fmt}" if execution_id else f"{base_filename}.{fmt}"
        filepath = os.path.join(data_dir, filename)

        if fmt == 'csv':
            df.to_csv(filepath, index=False, encoding='utf-8')
        elif fmt == 'parquet':
            df.columns = df.columns.astype(str)
            df.to_parquet(filepath, index=False)
        else:
            logging.warning(f"Unsupported format: {fmt}")
            continue

        logging.info(
            f"Guardado exitosamente {filepath} con {len(df)} registros.")


def process_historical_endpoints(base_filename, data_dir, execution_id, output_formats, add_metadata):
    """Fase paralela para descargar todo el histórico desde v1/api."""
    logging.info(
        "Iniciando extracción de Monica (Fase 1: Mapeo de días disponibles)...")
    data_mun, _ = fetch_data(f"{BASE_URL}/municipality/")
    if not data_mun or "results" not in data_mun:
        logging.error("No se pudo obtener la lista de municipios.")
        return

    municipalities = data_mun["results"]
    tasks_to_run = []

    for municipality in municipalities:
        muni_id = municipality.get("id")
        if not muni_id:
            continue

        for year in range(2002, 2026 + 1):
            year_str = str(year)
            url_yearly = f"{BASE_URL}/ica_yearly/?year={year_str}&municipality_id={muni_id}"
            data_yearly, _ = fetch_data(url_yearly)

            if not data_yearly:
                continue

            dates = [reg["reading_date"]
                     for reg in data_yearly if "reading_date" in reg]
            for date in dates:
                tasks_to_run.append((municipality, year_str, date))

    logging.info(
        f"Fase 1 completada. {len(tasks_to_run)} días encontrados en total. Iniciando Fase 2: Descarga paralela...")

    registry = []

    def fetch_single_day(task):
        municipality, year_str, date = task
        muni_id = municipality["id"]
        url_day = f"{BASE_URL}/ica_by_day/?date={date}&municipality_id={muni_id}"

        data_day, meta_day = fetch_data(url_day)
        records = []
        if data_day:
            for record in data_day:
                record["municipality_id"] = muni_id
                record["lugar_nombre"] = record.get(
                    "name") or municipality.get("name")
                record["latitude"] = record.get(
                    "lat") or municipality.get("latitude")
                record["longitude"] = record.get(
                    "lng") or municipality.get("longitude")
                record["valor_ica"] = record.get("ica")
                record["fecha_hora_registro"] = date
                record["year"] = year_str
                record["reading_date"] = date
                if add_metadata and meta_day:
                    for k, v in meta_day.items():
                        record[k] = v
                records.append(record)
        return records

    MAX_WORKERS = 5

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_single_day, task)
                   for task in tasks_to_run]

        try:
            from tqdm import tqdm
            for future in tqdm(as_completed(futures), total=len(tasks_to_run), desc="Descargando histórico de Monica"):
                try:
                    records = future.result()
                    registry.extend(records)
                except Exception:
                    pass
        except ImportError:
            completed = 0
            for future in as_completed(futures):
                try:
                    records = future.result()
                    registry.extend(records)
                    completed += 1
                    if completed % 100 == 0:
                        logging.info(
                            f"Descargados {completed}/{len(tasks_to_run)} días...")
                except Exception:
                    pass

    if not registry:
        logging.warning("No se extrajeron datos históricos.")
        return

    df_raw = pd.DataFrame(registry)

    # Limpieza del histórico
    for date_col in ["fecha_hora_registro", "fecha_hora_calculo", "reading_date"]:
        if date_col in df_raw.columns:
            df_raw[date_col] = pd.to_datetime(
                df_raw[date_col], errors='coerce')
            df_raw[date_col] = df_raw[date_col].apply(
                lambda x: x.isoformat() if pd.notnull(x) else None
            )

    if "valor_original" in df_raw.columns:
        df_raw = df_raw.rename(columns={"valor_original": "valor"})
    elif "valor_medido" in df_raw.columns:
        df_raw = df_raw.rename(columns={"valor_medido": "valor"})

    for col in ["valor", "valor_ica"]:
        if col in df_raw.columns:
            df_raw[col] = pd.to_numeric(df_raw[col], errors="coerce")

    df_raw = df_raw.assign(
        fuente="Sistema de Información Ambienta y de Recursos Hídricos - Ministerio de Medio Ambiente y Agua ")

    cols_to_keep = COLUMNS_FILTERS + \
        ["municipality_id", "municipality_name", "year", "reading_date", "fuente"]
    if add_metadata:
        cols_to_keep += METADATA_COLUMNS

    existing_cols = [c for c in cols_to_keep if c in df_raw.columns]
    df_cleaned = df_raw[existing_cols]

    save_dataframe(df_cleaned, base_filename, data_dir,
                   execution_id, output_formats)


def main():
    parser = argparse.ArgumentParser(
        description="Extract La Paz air quality data exclusively from the Monica system, normalize it, and save it."
    )
    parser.add_argument("--data-dir", type=str, default=None,
                        help="Directory to save the extracted data. Defaults to 'data' in project root.")
    parser.add_argument("--append-timestamp", action="store_true",
                        help="Append a timestamp to the saved files.")
    parser.add_argument("--format", type=str, default="parquet",
                        help="Output format (csv, parquet). Defaults to parquet.")
    parser.add_argument("--addmetadata", action="store_true",
                        help="Include request metadata columns in the output files.")
    args = parser.parse_args()

    if args.data_dir:
        data_dir = args.data_dir
    else:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(script_dir)
        data_dir = os.path.join(project_root, "data")

    os.makedirs(data_dir, exist_ok=True)

    execution_id = datetime.datetime.now().strftime(
        "%Y%m%d_%H%M%S") if args.append_timestamp else ""
    output_formats = [f.strip().lower() for f in args.format.split(",")]

    logging.info(f"Iniciando extracción de Monica. Guardando en {data_dir}")

    process_historical_endpoints(
        base_filename="monica_api_historical",
        data_dir=data_dir, execution_id=execution_id,
        output_formats=output_formats, add_metadata=args.addmetadata
    )

    logging.info("Ejecución finalizada exitosamente.")


if __name__ == "__main__":
    main()
