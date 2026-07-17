import os
import argparse
import logging
import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import shape

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def extract_senamhi_alerts(data_dir, output_formats):
    url = "https://onsc.senamhi.gob.bo/senamhiback/api/forecastweather/datatable"
    logging.info(f"Extracting Senamhi alerts from {url}")

    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    }

    all_alerts = []
    limit = 10
    offset = 0

    while True:
        payload = {
            "limit": limit,
            "offset": offset,
            "order": None,
            "query": {},
            "type": "Aprobado"
        }
        
        logging.info(f"Fetching records with offset {offset}...")
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=30)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            logging.error(f"Error fetching data from {url}: {e}")
            break

        if not data or "data" not in data or not data["data"]:
            logging.info("No more alerts returned from API.")
            break

        alerts = data["data"]
        all_alerts.extend(alerts)
        
        if len(alerts) < limit:
            break
            
        offset += limit

    if not all_alerts:
        logging.warning("No alerts found across all pages.")
        return

    logging.info(f"Retrieved a total of {len(all_alerts)} alert records from API.")

    records = []
    for alert in all_alerts:
        try:
            # Extract standard fields
            record = {
                "id_alerta": alert.get("id"),
                "codigo": alert.get("code"),
                "fecha_inicio": alert.get("date", [None, None])[0] if alert.get("date") else None,
                "fecha_fin": alert.get("date", [None, None])[1] if alert.get("date") and len(alert.get("date")) > 1 else None,
                "nivel_alerta": alert.get("ForecastTypeAlert", {}).get("alertType") if alert.get("ForecastTypeAlert") else None,
                "color_alerta": alert.get("ForecastTypeAlert", {}).get("color") if alert.get("ForecastTypeAlert") else None,
                "tipo_evento": alert.get("ForecastTypeEvent", {}).get("eventType") if alert.get("ForecastTypeEvent") else None,
                "descripcion": alert.get("description"),
                "fecha_publicacion": alert.get("publicationDate"),
                "estado": alert.get("status")
            }
            
            # Geometry
            drop_area = alert.get("dropArea")
            if drop_area and drop_area.get("type"):
                record["geometry"] = shape(drop_area)
            else:
                record["geometry"] = None
                
            records.append(record)
        except Exception as e:
            logging.error(f"Error processing alert {alert.get('id')}: {e}")
            
    if not records:
        logging.warning("No records were parsed successfully.")
        return
        
    gdf = gpd.GeoDataFrame(records, geometry="geometry", crs="EPSG:4326")
    
    # Estandarizacion de Fechas
    # Convert 'fecha_publicacion' to standard ISO string if possible
    if 'fecha_publicacion' in gdf.columns:
        gdf['fecha_publicacion'] = pd.to_datetime(gdf['fecha_publicacion'], errors='coerce').apply(lambda x: x.isoformat() if pd.notnull(x) else None)
        
    # Guardar los archivos
    base_filename = "senamhi_alerts"

    if "parquet" in output_formats:
        parquet_path = os.path.join(data_dir, f"{base_filename}.parquet")
        try:
            gdf.to_parquet(parquet_path, index=False)
            logging.info(f"Saved Parquet: {parquet_path} ({len(gdf)} records)")
        except Exception as e:
            logging.error(f"Failed to save Parquet (geometry columns might need conversion): {e}")
            
    if "geojson" in output_formats:
        geojson_path = os.path.join(data_dir, f"{base_filename}.geojson")
        try:
            # For GeoJSON, drop rows with null geometries to avoid errors
            gdf_valid_geom = gdf[gdf.geometry.notnull()]
            gdf_valid_geom.to_file(geojson_path, driver="GeoJSON")
            logging.info(f"Saved GeoJSON: {geojson_path} ({len(gdf_valid_geom)} records)")
        except Exception as e:
            logging.error(f"Failed to save GeoJSON: {e}")

def main():
    parser = argparse.ArgumentParser(description="Extract Senamhi weather alerts.")
    parser.add_argument("--format", type=str, default="geojson,parquet", help="Output formats")
    args = parser.parse_args()

    output_formats = [f.strip().lower() for f in args.format.split(",")]

    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    data_dir = os.path.join(project_root, "data")
    os.makedirs(data_dir, exist_ok=True)

    extract_senamhi_alerts(data_dir, output_formats)

if __name__ == "__main__":
    main()
