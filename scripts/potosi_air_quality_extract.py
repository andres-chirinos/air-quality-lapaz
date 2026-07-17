import os
import argparse
import logging
import requests
import pandas as pd
import datetime

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

ZONE_MAPPING = {
    1: {"lugar_nombre": "POTOSI - PLAZA AMARRILLA", "latitude": "-19.570804191581686", "longitude": "-65.76772320164257"},
    2: {"lugar_nombre": "POTOSI - NUEVA TERMINAL", "latitude": "-19.55803872082381", "longitude": "-65.76170899579182"},
    3: {"lugar_nombre": "POTOSI - UNIVERSIDAD AUTONOMA TOMAS FRIAS", "latitude": "-19.58502446927686", "longitude": "-65.75739556469925"},
    4: {"lugar_nombre": "POTOSI - NORMAL EDUARDO AVAROA", "latitude": "-19.571800624731402", "longitude": "-65.75225395806997"},
    5: {"lugar_nombre": "POTOSI - ESCUELA MUNICIPAL DE PLATERIA", "latitude": "-19.58661649949563", "longitude": "-65.76904873340257"},
    6: {"lugar_nombre": "POTOSI - PARQUE RECREACIONAL Y CULTURAL POTOSI", "latitude": "-19.569618586275315", "longitude": "-65.77073723432272"},
    7: {"lugar_nombre": "POTOSI - MERCADO UYUNI", "latitude": "-19.57824096110273", "longitude": "-65.75314240764938"},
    8: {"lugar_nombre": "POTOSI - EX TRANSITO", "latitude": "-19.582366030372796", "longitude": "-65.75715241133447"},
    9: {"lugar_nombre": "POTOSI - AVENIDA UNIVERSITARIA", "latitude": "-19.579635954635204", "longitude": "-65.76360193634714"},
    10: {"lugar_nombre": "POTOSI - BLANCO DE CAMPO", "latitude": "-19.557865766316215", "longitude": "-65.76149333701625"},
    11: {"lugar_nombre": "POTOSI - PLAZA SAN PEDRO", "latitude": "-19.594644106968346", "longitude": "-65.75134988490373"},
    12: {"lugar_nombre": "POTOSI - PLAZA TUMUSLA", "latitude": "-19.592645640700677", "longitude": "-65.74118824824475"},
}


def extract_potosi_data(data_dir, output_formats):
    url = "http://redmonica.potosi.bo/page/simulation/run?zone=all&month="
    logging.info(f"Extracting data from {url}")

    headers = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    }

    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        logging.error(f"Error fetching data from {url}: {e}")
        return

    if not data:
        logging.warning("No data returned from API.")
        return

    df = pd.DataFrame(data)

    if df.empty:
        logging.warning("Extracted data is empty.")
        return

    logging.info(f"Retrieved {len(df)} records from API.")

    # Estandarización de Columnas
    if "date" in df.columns:
        # Potosí API devuelve "2014-03-19". Parseamos a UTC.
        df["fecha_hora_registro"] = pd.to_datetime(
            df["date"], errors="coerce").dt.tz_localize('UTC')
    else:
        logging.error("No 'date' column found in response.")
        return

    if "ica50" in df.columns:
        # El usuario pidió explícitamente guardar el ica50 como el valor ICA estandarizado
        df["valor_ica"] = pd.to_numeric(df["ica50"], errors="coerce")
    else:
        logging.error("No 'ica50' column found in response.")
        return

    # Asignar nombre de lugar y coordenadas basado en zone_id
    def get_zone_info(row):
        zone_id = row.get("zone_id")
        try:
            zone_id = int(zone_id)
        except (ValueError, TypeError):
            zone_id = None

        info = ZONE_MAPPING.get(zone_id, {
                                "lugar_nombre": f"POTOSÍ - ZONA {zone_id}", "latitude": None, "longitude": None})
        return pd.Series([info["lugar_nombre"], info["latitude"], info["longitude"]])

    df[["lugar_nombre", "latitude", "longitude"]
       ] = df.apply(get_zone_info, axis=1)

    # Añadir campos estandarizados restantes
    df["fuente"] = "Red MoniCA - Potosí"
    df["fecha_hora_calculo"] = df["fecha_hora_registro"]  # Fallback

    # Asegurarse de que están en formato ISO 8601 (mismo formato de La Paz)
    df["fecha_hora_registro"] = df["fecha_hora_registro"].apply(
        lambda x: x.isoformat() if pd.notnull(x) else None)
    df["fecha_hora_calculo"] = df["fecha_hora_calculo"].apply(
        lambda x: x.isoformat() if pd.notnull(x) else None)

    # Seleccionar columnas finales
    expected_cols = [
        "valor_ica", "fecha_hora_registro", "fecha_hora_calculo",
        "latitude", "longitude", "lugar_nombre", "fuente"
    ]

    final_df = df[[c for c in expected_cols if c in df.columns]].copy()

    # Guardar los archivos
    base_filename = "potosi_api_historical"

    if "csv" in output_formats:
        csv_path = os.path.join(data_dir, f"{base_filename}.csv")
        final_df.to_csv(csv_path, index=False)
        logging.info(f"Saved CSV: {csv_path} ({len(final_df)} records)")

    if "parquet" in output_formats:
        parquet_path = os.path.join(data_dir, f"{base_filename}.parquet")
        final_df['valor_ica'] = pd.to_numeric(
            final_df['valor_ica'], errors='coerce')
        final_df['latitude'] = pd.to_numeric(
            final_df['latitude'], errors='coerce')
        final_df['longitude'] = pd.to_numeric(
            final_df['longitude'], errors='coerce')
        final_df.to_parquet(parquet_path, index=False)
        logging.info(
            f"Saved Parquet: {parquet_path} ({len(final_df)} records)")


def main():
    parser = argparse.ArgumentParser(
        description="Extract air quality data for Potosí.")
    parser.add_argument("--format", type=str,
                        default="csv,parquet", help="Output formats")
    args = parser.parse_args()

    output_formats = [f.strip().lower() for f in args.format.split(",")]

    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    data_dir = os.path.join(project_root, "data")
    os.makedirs(data_dir, exist_ok=True)

    extract_potosi_data(data_dir, output_formats)


if __name__ == "__main__":
    main()
