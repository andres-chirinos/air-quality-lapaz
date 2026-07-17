import os
import argparse
import logging
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_URL = "https://onsc.senamhi.gob.bo/senamhiback/api"

def get_active_stations():
    url = f"{BASE_URL}/stations/datatable"
    headers = {"Content-Type": "application/json"}
    
    logging.info("Fetching list of all stations...")
    
    all_stations = []
    limit = 200
    offset = 0

    while True:
        payload = {
            "offset": offset, 
            "limit": limit, 
            "order": None, 
            "query": {}
        }
        
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=60)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            logging.error(f"Error fetching stations: {e}")
            break
            
        stations = data.get("data", [])
        if not stations:
            break
            
        all_stations.extend(stations)
        
        if len(stations) < limit:
            break
            
        offset += limit

    # Filter actively operating ones
    active = [s for s in all_stations if s.get("status") == "Activo"]
    if not active:
        active = all_stations # Fallback if status tracking changed
        
    logging.info(f"Total active stations found: {len(active)} (Out of {len(all_stations)})")
    return active

def check_and_extract(station, days=30):
    station_id = station["id"]
    headers = {"Content-Type": "application/json"}
    
    # 1. Verification of records (filterstations)
    var_url = f"{BASE_URL}/variables/filterstations"
    try:
        res = requests.post(var_url, headers=headers, json={"idStation": [station_id]}, timeout=30)
        res_data = res.json()
        variables = res_data.get("data", {}).get("variables", [])
    except Exception:
        return []
        
    if not variables:
        return []
        
    var_ids = [v["id"] for v in variables]
    
    # Check last date available for this station on its first variable
    last_date_url = f"{BASE_URL}/datalastdatestation"
    has_data = False
    for var_id in var_ids[:3]: # Limit checks
        try:
            res = requests.post(last_date_url, headers=headers, json={"data": "diarios", "idStation": station_id, "idVariable": [var_id]}, timeout=30)
            res_data = res.json()
            if res_data.get("succes") and res_data.get("data"):
                has_data = True
                break
        except Exception:
            pass
            
    if not has_data:
        return []

    # 2. Extract data (exportxlsx)
    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=days)
    
    export_url = f"{BASE_URL}/stations/exportxlsx"
    payload = {
        "data": "diarios", 
        "idStation": [station_id], 
        "idVariable": var_ids, 
        "date": [start_date.strftime("%Y-%m-%dT00:00:00.000Z"), end_date.strftime("%Y-%m-%dT00:00:00.000Z")], 
        "multiDim": False
    }
    
    try:
        res = requests.post(export_url, headers=headers, json=payload, timeout=60)
        res_data = res.json()
        if res_data.get("succes"):
            return res_data.get("data", [])
    except Exception as e:
        logging.warning(f"Failed to export data for station {station_id}: {e}")
        
    return []

def extract_hidro_stations(data_dir, output_formats, days, max_workers):
    stations = get_active_stations()
    
    if not stations:
        logging.error("No stations could be retrieved.")
        return
        
    all_data = []
    
    # Process stations concurrently to save time
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(check_and_extract, s, days): s for s in stations}
        
        completed = 0
        for future in as_completed(futures):
            station = futures[future]
            try:
                data = future.result()
                if data:
                    all_data.extend(data)
            except Exception as e:
                logging.error(f"Error processing station {station['id']}: {e}")
                
            completed += 1
            if completed % 25 == 0:
                logging.info(f"Processed {completed}/{len(stations)} stations...")

    if not all_data:
        logging.warning("No data retrieved for any station in the specified time period.")
        return
        
    df = pd.DataFrame(all_data)
    
    # Strip quotes from column names
    df.columns = df.columns.str.replace('"', '').str.strip()
    
    logging.info(f"Total records extracted: {len(df)}")
    
    # Estandarización de Fechas y Columnas
    if all(c in df.columns for c in ['gestion', 'mes', 'dia']):
        df['fecha'] = pd.to_datetime(dict(year=df['gestion'], month=df['mes'], day=df['dia']), errors='coerce')
        df['fecha_hora_registro'] = df['fecha'].apply(lambda x: x.isoformat() if pd.notnull(x) else None)
        df.drop(columns=['fecha', 'gestion', 'mes', 'dia'], inplace=True, errors='ignore')

    # Convert numeric variables that are objects
    for col in df.columns:
        if df[col].dtype == 'object' and col not in ['estacion', 'fecha_hora_registro']:
            df[col] = pd.to_numeric(df[col], errors='ignore')

    base_filename = "senamhi_hidro_stations"

    if "csv" in output_formats:
        csv_path = os.path.join(data_dir, f"{base_filename}.csv")
        df.to_csv(csv_path, index=False)
        logging.info(f"Saved CSV: {csv_path} ({len(df)} records)")

    if "parquet" in output_formats:
        parquet_path = os.path.join(data_dir, f"{base_filename}.parquet")
        # For pyarrow, we need string columns to be uniform
        df.to_parquet(parquet_path, index=False)
        logging.info(f"Saved Parquet: {parquet_path} ({len(df)} records)")

def main():
    parser = argparse.ArgumentParser(description="Extract Senamhi Hidro/Met stations data.")
    parser.add_argument("--format", type=str, default="csv,parquet", help="Output formats")
    parser.add_argument("--days", type=int, default=30, help="Number of historical days to fetch")
    parser.add_argument("--workers", type=int, default=15, help="Max concurrent workers for API requests")
    args = parser.parse_args()

    output_formats = [f.strip().lower() for f in args.format.split(",")]

    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    data_dir = os.path.join(project_root, "data")
    os.makedirs(data_dir, exist_ok=True)

    extract_hidro_stations(data_dir, output_formats, args.days, args.workers)

if __name__ == "__main__":
    main()
