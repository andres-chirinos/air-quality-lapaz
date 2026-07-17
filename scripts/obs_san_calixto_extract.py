#!/usr/bin/env python3
import pandas as pd
import argparse
import datetime
import hashlib
import requests
import json
import os
import sys
import time
import logging
from bs4 import BeautifulSoup
from io import StringIO
from tqdm.auto import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
import re

warnings.filterwarnings('ignore')

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

HEADERS = {
    "Host": "www.osc.org.bo",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:149.0) Gecko/20100101 Firefox/149.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
}

BASE_URL = "https://www.osc.org.bo/index.php/es/"

def fetch_html(url, retries=3, delay_seconds=2):
    for attempt in range(1, retries + 1):
        try:
            res = requests.get(url, timeout=30, headers=HEADERS, verify=False)
            if res.status_code == 200:
                return res.content
        except Exception as e:
            pass
        time.sleep(delay_seconds)
    return None

def fetch_detail_page(url):
    """Extrae la información extra de la página de detalle."""
    if not url: return {}
    html_content = fetch_html(url)
    if not html_content: return {}
    
    soup = BeautifulSoup(html_content, 'html.parser')
    tables = soup.find_all('table')
    if not tables: return {}
    
    target_table = next((t for t in tables if 'Magnitud' in t.text or 'Región' in t.text), tables[0])
    df_list = pd.read_html(StringIO(str(target_table)))
    
    if not df_list: return {}
    df = df_list[0].dropna(subset=[0, 1])
    
    keys = df.iloc[:, 0].astype(str).tolist()
    values = df.iloc[:, 1].astype(str).tolist()
    
    detail_dict = {'enlace_detalle': url}
    for k, v in zip(keys, values):
        if k != v and 'M - ' not in k:
            clean_key = k.strip().lower().replace(" ", "_").replace(":", "")
            detail_dict[clean_key] = v.strip()
            
    return detail_dict

def get_max_id():
    """Obtiene el ID máximo desde la primera página del listado."""
    url = f"{BASE_URL}?_pagi_pg=1"
    html_content = fetch_html(url)
    if not html_content: return 0
    soup = BeautifulSoup(html_content, 'html.parser')
    tables = soup.find_all('table')
    if not tables: return 0
    target_table = next((t for t in tables if 'Magnitud (M)' in t.text or 'Fecha' in t.text), tables[-1])
    rows = target_table.find_all('tr')
    for row in rows[1:]:
        link = row.get('data-href', '')
        if 'ID=' in link:
            try:
                max_id = int(link.split('ID=')[1].split('&')[0])
                return max_id
            except ValueError:
                continue
    return 0

def fetch_page_data(page_num):
    """Obtiene los datos de una página específica del listado de sismos."""
    url = f"{BASE_URL}?_pagi_pg={page_num}"
    html_content = fetch_html(url, retries=3, delay_seconds=5)
    
    if not html_content:
        logging.error(f"Fallo al obtener la página {page_num}")
        return None, url, 500
        
    soup = BeautifulSoup(html_content, 'html.parser')
    tables = soup.find_all('table')
    
    if not tables:
        return [], url, 200
    
    target_table = next((t for t in tables if 'Magnitud (M)' in t.text or 'Fecha' in t.text), tables[-1])
    rows = target_table.find_all('tr')
    
    if len(rows) <= 1:
        return [], url, 200
        
    headers = [th.text.strip() for th in rows[0].find_all(['th', 'td'])]
    
    data = []
    for row in rows[1:]:
        cols = row.find_all('td')
        if not cols: continue
        row_data = {headers[i]: col.text.strip() for i, col in enumerate(cols) if i < len(headers)}
        
        link = row.get('data-href', '')
        if link and link.startswith('http'):
            row_data['enlace_detalle'] = link
        elif link:
            row_data['enlace_detalle'] = "https://www.osc.org.bo" + (link if link.startswith('/') else '/' + link)
        else:
            row_data['enlace_detalle'] = None
            
        data.append(row_data)
        
    return data, url, 200

def enrich_with_details(data, max_workers=5):
    """Para cada registro, descarga el enlace_detalle en paralelo y une la info."""
    enriched_data = []
    
    def process_row(row):
        link = row.get('enlace_detalle')
        if link:
            details = fetch_detail_page(link)
            exclude_keys = ['latitud', 'longitud', 'profundidad', 'magnitud', 'fecha', 'hora', 'fecha_y_hora', 'localización', 'enlace_detalle']
            filtered_details = {k: v for k, v in details.items() if k not in exclude_keys}
            return {**row, **filtered_details}
        return row
        
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_row, row): row for row in data}
        for future in as_completed(futures):
            try:
                enriched_data.append(future.result())
            except Exception as e:
                enriched_data.append(futures[future])
                
    return enriched_data

def brute_force_extraction(max_id, max_workers=10):
    """Extrae datos haciendo requests por fuerza bruta a todos los IDs posibles."""
    all_details = []
    urls_to_fetch = [f"https://www.osc.org.bo/index.php?option=com_content&view=article&id=50&ID={i}" for i in range(1, max_id + 1)]
    
    def process_url(url):
        return fetch_detail_page(url)
        
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_url, url): url for url in urls_to_fetch}
        for future in tqdm(as_completed(futures), total=len(urls_to_fetch), desc="Fuerza bruta detalles"):
            try:
                res = future.result()
                if res and 'magnitud' in res and any(char.isdigit() for char in res['magnitud']): 
                    all_details.append(res)
            except Exception:
                pass
                
    return all_details

def clean_brute_force_data(df):
    """Limpia los campos extraídos directamente por fuerza bruta para asemejarse al listado."""
    if df.empty: return df
    
    if 'localización' in df.columns:
        # Ejemplo: "-17.443 ; -69.309"
        locs = df['localización'].str.split(';', expand=True)
        if locs.shape[1] >= 2:
            df['latitud'] = locs[0]
            df['longitud'] = locs[1]
            
    if 'fecha_y_hora' in df.columns:
        # Ejemplo: "2026-06-24 18:59:34 (Hora local)"
        df['fecha_hora_registro'] = df['fecha_y_hora'].str.replace(r'\(.*\)', '', regex=True).str.strip()
        df['fecha_hora_registro'] = pd.to_datetime(df['fecha_hora_registro'], errors='coerce')
        
    return df

def transform_dataset(data, url_source, status_code, add_metadata=False, is_bruteforce=False):
    """Limpia y estructura los datos extraídos."""
    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)
    
    if is_bruteforce:
        df = clean_brute_force_data(df)
    
    # Renombrar columnas base del listado y de fuerza bruta
    rename_map = {
        'Fecha': 'fecha',
        'Hora local': 'hora',
        'Latitud': 'latitud',
        'Longitud': 'longitud',
        'Profundidad (Km)': 'profundidad',
        'Magnitud (M)': 'magnitud',
        'Región': 'region_base',
        'región': 'region_base'
    }
    
    existing_rename = {k: v for k, v in rename_map.items() if k in df.columns}
    df = df.rename(columns=existing_rename)
    
    if 'fecha' in df.columns and 'hora' in df.columns:
        try:
            df['fecha_hora_registro'] = pd.to_datetime(df['fecha'] + ' ' + df['hora'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
        except Exception:
            df['fecha_hora_registro'] = pd.to_datetime(df['fecha'] + ' ' + df['hora'], errors='coerce')
            
    if 'profundidad' in df.columns:
        df['profundidad_raw'] = df['profundidad'].astype(str).copy()

    numeric_cols = ['latitud', 'longitud', 'profundidad', 'magnitud']
    for col in numeric_cols:
        if col in df.columns:
            # Extraer números sin importar el tipo actual para evitar fallos por dtypes como 'string'
            df[col] = df[col].astype(str).str.extract(r'([-\d\.]+)')[0]
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
    df['fuente'] = "observatorio_san_calixto"
    
    if 'observaciones' not in df.columns:
        df['observaciones'] = df.get('region_base', '')
    else:
        df['observaciones'] = df['observaciones'].fillna(df.get('region_base', ''))
        
    # Preservar el texto en bruto de profundidad si contenía información útil (ej: "Intermedia")
    if 'profundidad_raw' in df.columns:
        # Añadir a observaciones si la profundidad extraída fue nula pero había texto, o si contiene palabras clave
        mask = (df['profundidad'].isna() & (df['profundidad_raw'] != 'nan') & (df['profundidad_raw'] != 'None') & (df['profundidad_raw'].str.strip() != '')) | df['profundidad_raw'].str.contains('intermedia|superficial|profund', case=False, na=False)
        df.loc[mask, 'observaciones'] = df.loc[mask, 'observaciones'].astype(str) + " | Profundidad: " + df.loc[mask, 'profundidad_raw'].astype(str)
        df = df.drop(columns=['profundidad_raw'])
        
    if add_metadata:
        data_str = json.dumps(data, ensure_ascii=False)
        df['_metadata_source'] = url_source
        df['_metadata_request_status'] = status_code
        df['_metadata_timestamp'] = datetime.datetime.now().isoformat()
        df['_metadata_unix_timestamp'] = int(datetime.datetime.now().timestamp())
        df['_metadata_hash'] = hashlib.md5(data_str.encode("utf-8")).hexdigest()
        
    cols_to_drop = [c for c in ['fecha', 'hora', 'fecha_y_hora', 'localización', 'profundidad_detalle', 'magnitud_detalle'] if c in df.columns]
    df = df.drop(columns=cols_to_drop, errors='ignore')
        
    return df

def save_dataframe(df, base_filename, data_dir, execution_id, output_formats):
    if df.empty: return
    for fmt in output_formats:
        fmt = fmt.strip().lower()
        filename = f"{base_filename}_{execution_id}.{fmt}" if execution_id else f"{base_filename}.{fmt}"
        filepath = os.path.join(data_dir, filename)

        if fmt == 'csv':
            df.to_csv(filepath, index=False, encoding='utf-8')
        elif fmt == 'parquet':
            df.columns = df.columns.astype(str)
            df.to_parquet(filepath, index=False)
        logging.info(f"Guardado exitoso: {filepath} ({len(df)} filas)")

def main():
    parser = argparse.ArgumentParser(description="Extract earthquake data from Observatorio San Calixto.")
    parser.add_argument("--data-dir", type=str, default=None)
    parser.add_argument("--append-timestamp", action="store_true")
    parser.add_argument("--format", type=str, default="csv,parquet")
    parser.add_argument("--addmetadata", action="store_true",
                        help="Incluir columnas de metadatos del request.")
    parser.add_argument("--max-pages", type=int, default=0)
    parser.add_argument("--extract-type", type=str, choices=['listado', 'brute-force'], default='listado',
                        help="Método de extracción: 'listado' recorre la paginación, 'brute-force' extrae IDs de detalle 1 a N")
    args = parser.parse_args()

    data_dir = args.data_dir or os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
    os.makedirs(data_dir, exist_ok=True)
    execution_id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S") if args.append_timestamp else ""

    logging.info(f"Iniciando extracción (Modo: {args.extract_type})...")
    
    if args.extract_type == 'brute-force':
        max_id = get_max_id()
        if max_id == 0:
            logging.error("No se pudo detectar el ID máximo para fuerza bruta.")
            return
            
        logging.info(f"Se detectó ID máximo: {max_id}. Iniciando fuerza bruta...")
        # Limitar max_id para pruebas si max_pages fue provisto (usando max_pages como limite de records)
        if args.max_pages > 0:
            max_id = min(max_id, args.max_pages)
            logging.info(f"Limitando fuerza bruta a los primeros {max_id} IDs por --max-pages.")
            
        brute_data = brute_force_extraction(max_id, max_workers=10)
        
        if brute_data:
            final_df = transform_dataset(brute_data, "brute-force-multiple", 200, args.addmetadata, is_bruteforce=True)
            if 'fecha_hora_registro' in final_df.columns:
                final_df = final_df.sort_values(by='fecha_hora_registro', ascending=False)
            save_dataframe(final_df, "observatorio_san_calixto_sismos", data_dir, execution_id, args.format.split(","))
        else:
            logging.warning("Fuerza bruta no extrajo datos válidos.")
            
    else: # listado
        all_dataframes = []
        page_num = 1
        pbar = tqdm(desc="Scrapeando páginas")
        
        while True:
            data, url, status_code = fetch_page_data(page_num)
            
            if data is None:
                break
            if not data:
                logging.info(f"Fin de registros en página {page_num}.")
                break
                
            data_enriched = enrich_with_details(data, max_workers=8)
            df_page = transform_dataset(data_enriched, url, status_code, args.addmetadata, is_bruteforce=False)
            
            if not df_page.empty:
                all_dataframes.append(df_page)
                
            pbar.update(1)
            pbar.set_postfix({"pág": page_num, "registros": len(data)})
            
            if args.max_pages > 0 and page_num >= args.max_pages:
                break
            page_num += 1

        pbar.close()

        if all_dataframes:
            final_df = pd.concat(all_dataframes, ignore_index=True)
            if 'fecha_hora_registro' in final_df.columns:
                final_df = final_df.sort_values(by='fecha_hora_registro', ascending=False)
            save_dataframe(final_df, "observatorio_san_calixto_sismos", data_dir, execution_id, args.format.split(","))
        else:
            logging.warning("No se extrajo ningún dato válido.")

if __name__ == "__main__":
    main()
