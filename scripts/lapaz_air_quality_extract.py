#!/usr/bin/env python3
import os
import json
import requests
import hashlib
import datetime
import subprocess
import argparse
import pandas as pd

COLUMNS_FILTERS = [
    "id_parametro", "valor", "valor_ica", 
    "fecha_hora_registro", "fecha_hora_calculo", "observaciones"
]

METADATA_COLUMNS = [
    "_metadata_source", "_metadata_request_status", 
    "_metadata_timestamp", "_metadata_unix_timestamp", 
    "_metadata_hash"
]


def check_connection(url="http://131.0.1.19:3002/"):
    """Check if the base URL is reachable."""
    try:
        result = subprocess.run(["curl", "-I", url],
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                timeout=5)
        if result.returncode != 0:
            print(f"No se pudo alcanzar {url}. Saltando todo.")
            return False
        else:
            print(f"{url} respondió correctamente.")
            return True
    except Exception as e:
        print(f"Error al hacer curl a {url}: {e}. Saltando todo.")
        return False


def fetch_data(url, method='GET'):
    """Fetch JSON data and generate metadata for a given URL."""
    try:
        if method.upper() == 'POST':
            response = requests.post(url, timeout=100)
        else:
            response = requests.get(url, timeout=100)

        if response.status_code in [200, 201]:
            data = response.json()
            metadata = {
                "_metadata_source": url,
                "_metadata_request_status": response.status_code,
                "_metadata_timestamp": datetime.datetime.now().isoformat(),
                "_metadata_unix_timestamp": int(datetime.datetime.now().timestamp()),
                "_metadata_hash": hashlib.md5(json.dumps(data).encode("utf-8")).hexdigest()
            }
            return data, metadata
        else:
            print(f"Failed to fetch {url}. Status code: {response.status_code}")
            return None, None
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None, None


def transform_dataset(data, metadata, fuente, add_metadata=False):
    """Normalize JSON, clean columns, apply data typing, and filter."""
    if not data:
        return pd.DataFrame()

    df = pd.json_normalize(data)
    if df.empty:
        return df

    # Add metadata if flag is enabled
    if add_metadata:
        for key, value in metadata.items():
            df[key] = value

    # Add dataset source label
    df = df.assign(fuente=fuente)

    # Cast dates to datetime where possible
    for date_col in ["fecha_hora_registro", "fecha_hora_calculo", "_metadata_timestamp"]:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col])

    # Rename specific columns and drop unnecessary ones
    if fuente == "antiguo":
        if "valor_original" in df.columns:
            df = df.rename(columns={"valor_original": "valor"})
        if "promedio_24hrs" in df.columns:
            df = df.drop(columns=["promedio_24hrs"])
            
    elif fuente == "nuevo":
        if "valor_medido" in df.columns:
            df = df.rename(columns={"valor_medido": "valor"})
        if "valor_calculado_x2" in df.columns:
            df = df.drop(columns=["valor_calculado_x2"])

    # Enforce column filtering and ordering
    if add_metadata:
        final_cols = COLUMNS_FILTERS + METADATA_COLUMNS + ["fuente"]
    else:
        final_cols = COLUMNS_FILTERS + ["fuente"]
        
    # Ensure numeric types for values
    for numeric_col in ["valor", "valor_ica"]:
        if numeric_col in df.columns:
            df[numeric_col] = pd.to_numeric(df[numeric_col], errors="coerce")

    existing_cols = [c for c in final_cols if c in df.columns]
    
    return df[existing_cols]


def save_dataframe(df, base_filename, data_dir, execution_id, output_formats):
    """Save concatenated DataFrame to Disk in requested formats."""
    if df.empty:
        print(f"No data to save for {base_filename}")
        return

    for fmt in output_formats:
        fmt = fmt.strip().lower()
        if execution_id:
            filename = f"{base_filename}_{execution_id}.{fmt}"
        else:
            filename = f"{base_filename}.{fmt}"

        filepath = os.path.join(data_dir, filename)

        if fmt == 'csv':
            df.to_csv(filepath, index=False, encoding='utf-8')
        elif fmt == 'parquet':
            df.columns = df.columns.astype(str)
            df.to_parquet(filepath, index=False)
        else:
            print(f"Unsupported format: {fmt}")
            continue

        print(f"Saved {filepath} with {len(df)} rows.")


def process_grouped_endpoints(url_nuevo, url_antiguo, base_filename, method, data_dir, execution_id, output_formats, add_metadata):
    """Fetch, transform, and concatenate grouped Nuevo and Antiguo endpoints."""
    print(f"Processing {base_filename}...")
    data_nuevo, meta_nuevo = fetch_data(url_nuevo, method)
    data_antiguo, meta_antiguo = fetch_data(url_antiguo, method)

    df_nuevo = transform_dataset(data_nuevo, meta_nuevo, "nuevo", add_metadata)
    df_antiguo = transform_dataset(data_antiguo, meta_antiguo, "antiguo", add_metadata)

    dfs_to_concat = []
    if not df_antiguo.empty:
        dfs_to_concat.append(df_antiguo)
    if not df_nuevo.empty:
        dfs_to_concat.append(df_nuevo)

    if dfs_to_concat:
        df_total = pd.concat(dfs_to_concat, ignore_index=True)
        save_dataframe(df_total, base_filename, data_dir, execution_id, output_formats)
    else:
        print(f"No valid data retrieved for {base_filename}")


def process_datos_endpoint(url, base_filename, data_dir, execution_id, output_formats, add_metadata):
    """Specific processing for the single /datos endpoint that contains both."""
    print(f"Processing {base_filename}...")
    data, meta = fetch_data(url, "GET")
    if not data:
        return

    # Extract dicts inside the returned JSON payload
    data_nuevo = [data.get("nuevo")] if data.get("nuevo") else []
    data_antiguo = [data.get("antiguo")] if data.get("antiguo") else []

    df_nuevo = transform_dataset(data_nuevo, meta, "nuevo", add_metadata)
    df_antiguo = transform_dataset(data_antiguo, meta, "antiguo", add_metadata)

    dfs_to_concat = []
    if not df_antiguo.empty:
        dfs_to_concat.append(df_antiguo)
    if not df_nuevo.empty:
        dfs_to_concat.append(df_nuevo)

    if dfs_to_concat:
        df_total = pd.concat(dfs_to_concat, ignore_index=True)
        save_dataframe(df_total, base_filename, data_dir, execution_id, output_formats)
    else:
        print(f"No valid data retrieved for {base_filename}")


def main():
    parser = argparse.ArgumentParser(
        description="Extract La Paz air quality data, normalize, and save as CSV and/or Parquet."
    )
    parser.add_argument("--data-dir", type=str, default=None,
                        help="Directory to save the extracted data. Defaults to 'data' in project root.")
    parser.add_argument("--append-timestamp", action="store_true",
                        help="Append a timestamp to the saved files (useful for weekly runs to avoid overwriting).")
    parser.add_argument("--format", type=str, default="parquet",
                        help="Output format (csv, parquet, or both separated by comma). Defaults to parquet.")
    parser.add_argument("--addmetadata", action="store_true",
                        help="Include request metadata columns in the output files.")
    parser.add_argument("--extract-from", type=str, default="hours",
                        help="Endpoints to extract from: 'now', 'hours', 'days', or comma-separated list. Defaults to 'hours'.")
    args = parser.parse_args()

    if not check_connection("http://131.0.1.19:3002/"):
        return

    # Determine data directory
    if args.data_dir:
        data_dir = args.data_dir
    else:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(script_dir)
        data_dir = os.path.join(project_root, "data")

    os.makedirs(data_dir, exist_ok=True)

    execution_id = ""
    if args.append_timestamp:
        execution_id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        print(f"Starting extraction with execution ID: {execution_id}")

    output_formats = [f.strip().lower() for f in args.format.split(",")]
    extracts = [e.strip().lower() for e in args.extract_from.split(",")]
    
    print(f"Saving data to {data_dir} in formats: {output_formats}")
    print(f"Including metadata: {args.addmetadata}")
    print(f"Extracting from: {extracts}")

    # 1. Process ultimas-x-horas ("hours")
    if "hours" in extracts:
        process_grouped_endpoints(
            url_nuevo="http://131.0.1.19:3002/postgres2/ultimas-x-horas-nuevo/99999",
            url_antiguo="http://131.0.1.19:3002/postgres2/ultimas-x-horas-antiguo/99999",
            base_filename="postgres2_ultimas-x-horas",
            method="GET",
            data_dir=data_dir, execution_id=execution_id, 
            output_formats=output_formats, add_metadata=args.addmetadata
        )

    # 2. Process datos-diarios ("days")
    if "days" in extracts:
        process_grouped_endpoints(
            url_nuevo="http://131.0.1.19:3002/postgres2/datos-diarios-nuevo-2024/999999999",
            url_antiguo="http://131.0.1.19:3002/postgres2/datos-diarios-antiguo/999999999",
            base_filename="postgres2_datos-diarios",
            method="POST",
            data_dir=data_dir, execution_id=execution_id, 
            output_formats=output_formats, add_metadata=args.addmetadata
        )

    # 3. Process datos ("now")
    if "now" in extracts:
        process_datos_endpoint(
            url="http://131.0.1.19:3002/postgres2/datos",
            base_filename="postgres2_datos",
            data_dir=data_dir, execution_id=execution_id, 
            output_formats=output_formats, add_metadata=args.addmetadata
        )

    print("Extraction and transformation completed successfully.")

if __name__ == "__main__":
    main()
