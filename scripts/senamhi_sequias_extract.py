import os
import argparse
import logging
import requests
import pandas as pd
import io

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

URLS = {
    "Bolivia": "https://bolivia.mydewetra.cimafoundation.org/drought_public/MSBI/MSBI_Bolivia_last.csv",
    "Altiplano": "https://bolivia.mydewetra.cimafoundation.org/drought_public/MSBI/MSBI_Altiplano_last.csv",
    "Amazonia": "https://bolivia.mydewetra.cimafoundation.org/drought_public/MSBI/MSBI_Amazonia_last.csv",
    "Chaco": "https://bolivia.mydewetra.cimafoundation.org/drought_public/MSBI/MSBI_Chaco_last.csv",
    "Chiquitania": "https://bolivia.mydewetra.cimafoundation.org/drought_public/MSBI/MSBI_Chiquitania_last.csv",
    "Llanuras_Sabanas": "https://bolivia.mydewetra.cimafoundation.org/drought_public/MSBI/MSBI_Llanuras_Sabanas_last.csv",
    "Yungas_Chapare": "https://bolivia.mydewetra.cimafoundation.org/drought_public/MSBI/MSBI_Yungas_Chapare_last.csv",
    "Valles": "https://bolivia.mydewetra.cimafoundation.org/drought_public/MSBI/MSBI_Valles_last.csv"
}

def extract_sequias(data_dir, output_formats):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "*/*"
    }

    all_dfs = []

    for region, url in URLS.items():
        logging.info(f"Extracting data for region: {region} from {url}")
        try:
            response = requests.get(url, headers=headers, timeout=60)
            response.raise_for_status()
            
            csv_content = response.text
            
            # Pasa skipinitialspace=True para limpiar los espacios después de las comas en los CSVs
            df = pd.read_csv(io.StringIO(csv_content), skipinitialspace=True)
            df['region'] = region
            all_dfs.append(df)
            
        except Exception as e:
            logging.error(f"Failed to extract data from {url}: {e}")

    if not all_dfs:
        logging.error("No drought data could be extracted.")
        return

    # Combine all DataFrames
    combined_df = pd.concat(all_dfs, ignore_index=True)
    
    # Estandarización de nombres de columnas y limpieza
    combined_df.columns = [col.strip().lower() for col in combined_df.columns]
    
    # El archivo CSV original de senamhi tiene 'date' como '2007-02'.
    if 'date' in combined_df.columns:
        combined_df['fecha'] = pd.to_datetime(combined_df['date'], format='%Y-%m', errors='coerce')
        combined_df['fecha_hora_registro'] = combined_df['fecha'].apply(lambda x: x.isoformat() if pd.notnull(x) else None)
        combined_df.drop(columns=['fecha'], inplace=True)
        
    logging.info(f"Total extracted records: {len(combined_df)}")

    base_filename = "senamhi_sequias"

    if "csv" in output_formats:
        csv_path = os.path.join(data_dir, f"{base_filename}.csv")
        combined_df.to_csv(csv_path, index=False)
        logging.info(f"Saved CSV: {csv_path} ({len(combined_df)} records)")

    if "parquet" in output_formats:
        parquet_path = os.path.join(data_dir, f"{base_filename}.parquet")
        combined_df.to_parquet(parquet_path, index=False)
        logging.info(f"Saved Parquet: {parquet_path} ({len(combined_df)} records)")

def main():
    parser = argparse.ArgumentParser(description="Extract Senamhi Drought (Sequías) data.")
    parser.add_argument("--format", type=str, default="csv,parquet", help="Output formats")
    args = parser.parse_args()

    output_formats = [f.strip().lower() for f in args.format.split(",")]

    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    data_dir = os.path.join(project_root, "data")
    os.makedirs(data_dir, exist_ok=True)

    extract_sequias(data_dir, output_formats)

if __name__ == "__main__":
    main()
