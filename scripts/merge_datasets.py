#!/usr/bin/env python3
import pandas as pd
import os
import glob
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)


def merge_datasets():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    data_dir = os.path.join(project_root, "data")

    # Archivos a buscar (excluimos archivos que ya sean el resultado del merge)
    parquet_files = glob.glob(os.path.join(data_dir, "*.parquet"))
    csv_files = glob.glob(os.path.join(data_dir, "*.csv"))

    # Excluir los archivos consolidados si ya existen
    parquet_files = [f for f in parquet_files if "consolidated" not in f]
    csv_files = [f for f in csv_files if "consolidated" not in f]

    dataframes = []

    # Leemos todos los parquets
    for f in parquet_files:
        try:
            df = pd.read_parquet(f)
            dataframes.append(df)
            logging.info(
                f"Cargado {os.path.basename(f)} ({len(df)} registros)")
        except Exception as e:
            logging.error(f"Error cargando {f}: {e}")

    # Leemos todos los csvs (solo si no hay un parquet equivalente para no duplicar data innecesaria)
    parquet_basenames = [os.path.splitext(os.path.basename(f))[
        0] for f in parquet_files]
    for f in csv_files:
        basename = os.path.splitext(os.path.basename(f))[0]
        if basename not in parquet_basenames:
            try:
                df = pd.read_csv(f)
                dataframes.append(df)
                logging.info(
                    f"Cargado {os.path.basename(f)} ({len(df)} registros)")
            except Exception as e:
                logging.error(f"Error cargando {f}: {e}")

    if not dataframes:
        logging.warning("No se encontraron archivos de datos para concatenar.")
        return

    logging.info("Concatenando datos...")
    consolidated_df = pd.concat(dataframes, ignore_index=True)

    # Aseguramos el orden de las columnas si existen
    ordered_columns = [
        "fecha_hora_registro", "lugar_nombre", "latitude", "longitude",
        "valor_ica", "observaciones", "fuente"
    ]

    # Filtramos a las que existen y agregamos cualquier otra columna residual al final
    existing_ordered = [
        c for c in ordered_columns if c in consolidated_df.columns]
    residual_columns = []  # [c for c in consolidated_df.columns if c not in existing_ordered]
    consolidated_df = consolidated_df[existing_ordered + residual_columns]

    # Opcional: eliminar duplicados si los hubiera
    # consolidated_df = consolidated_df.drop_duplicates()

    consolidated_dir = os.path.join(data_dir, "consolidated")
    os.makedirs(consolidated_dir, exist_ok=True)
    
    out_parquet = os.path.join(consolidated_dir, "air_quality_consolidated.parquet")
    out_csv = os.path.join(consolidated_dir, "air_quality_consolidated.csv")

    consolidated_df.to_parquet(out_parquet, index=False)
    logging.info(
        f"Guardado consolidado Parquet: {out_parquet} ({len(consolidated_df)} registros totales)")

    consolidated_df.to_csv(out_csv, index=False, encoding="utf-8")
    logging.info(
        f"Guardado consolidado CSV: {out_csv} ({len(consolidated_df)} registros totales)")


if __name__ == "__main__":
    merge_datasets()
