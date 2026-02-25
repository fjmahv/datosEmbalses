import os
import sys
import json
import hashlib
import zipfile
import subprocess
import io
import shutil  # <-- ¡AQUÍ ESTÁ LA LIBRERÍA QUE FALTA!
from datetime import datetime
from dateutil.relativedelta import relativedelta

import requests
import pandas as pd
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn

# --- CONFIGURACIÓN ---
# ... (el resto de tu código sigue igual)

# --- CONFIGURACIÓN ---
URL_ZIPPED_DB = "https://www.miteco.gob.es/content/dam/miteco/es/agua/temas/evaluacion-de-los-recursos-hidricos/boletin-hidrologico/Historico-de-embalses/BD-Embalses.zip"
ZIP_FILE = "temp_embalses.zip"
MDB_FILE = "BD-Embalses.mdb"
HASH_FILE = "last_mdb_hash.txt"
JSON_OUTPUT = "datos_embalses_optimizado.json"
TABLE_NAME = "T_Datos Embalses 1988-2026"

console = Console()

# --- METADATOS Y MAPEO DE CLAVES CORTAS ---
METADATA = {
    "fuente": "MITECO",
    "mapeo": {
        "an": "AMBITO_NOMBRE",
        "en": "EMBALSE_NOMBRE",
        "at": "AGUA_TOTAL",
        "aa": "AGUA_ACTUAL",
        "f": "FECHA_ULTIMO_DATO",
        "m1s": "Media_Ultima_Semana",
        "m2s": "Media_Ultimas_2_Semanas",
        "m1m": "Media_Ultimo_Mes",
        "ma1": "Media_Mismo_Mes_Anio_Anterior",
        "h3a": "Media_Historica_Mes_3_Anios",
        "h5a": "Media_Historica_Mes_5_Anios",
        "h10a": "Media_Historica_Mes_10_Anios",
        "h20a": "Media_Historica_Mes_20_Anios",
        "ht": "Media_Historica_Mes_Total"
    }
}

def get_file_hash(filepath):
    """Calcula el hash SHA-256 de un archivo."""
    hasher = hashlib.sha256()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hasher.update(chunk)
    return hasher.hexdigest()

def descargar_y_extraer():
    """Descarga el ZIP, extrae dinámicamente el MDB y limpia temporales."""
    try:
        console.print(f"[cyan]Descargando datos desde:[/cyan] {URL_ZIPPED_DB}")
        response = requests.get(URL_ZIPPED_DB, stream=True, timeout=30)
        response.raise_for_status()
        
        with open(ZIP_FILE, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        with zipfile.ZipFile(ZIP_FILE, 'r') as zip_ref:
            # Buscamos el primer archivo que termine en .mdb (ignorando mayúsculas/minúsculas)
            mdb_interno = next((nombre for nombre in zip_ref.namelist() if nombre.lower().endswith('.mdb')), None)
            
            if not mdb_interno:
                raise FileNotFoundError("No se encontró ningún archivo .mdb dentro del ZIP de MITECO.")
            
            # Lo leemos directamente del ZIP y lo escribimos con nuestro nombre estándar
            # Esto evita problemas si el archivo original venía metido en subcarpetas
            with zip_ref.open(mdb_interno) as source, open(MDB_FILE, "wb") as target:
                shutil.copyfileobj(source, target)
            
        # Limpieza inmediata para ahorrar espacio
        os.remove(ZIP_FILE)
        console.print(f"[green]✔ Descarga completada. MDB extraído dinámicamente ({mdb_interno}). ZIP eliminado.[/green]")
        
    except Exception as e:
        console.print(f"[bold red]✖ Error en la descarga/extracción: {e}[/bold red]")
        sys.exit(1)

def calcular_estadisticas_embalse(df_embalse):
    """Calcula indicadores estadísticos para un embalse individual."""
    if df_embalse.empty:
        return pd.Series()

    df_embalse = df_embalse.sort_values('FECHA', ascending=False)
    fecha_referencia = df_embalse['FECHA'].iloc[0] # El dato más reciente de este embalse
    mes_ref = fecha_referencia.month
    
    # Filtros temporales
    df_1s = df_embalse[df_embalse['FECHA'] >= fecha_referencia - pd.Timedelta(days=7)]
    df_2s = df_embalse[df_embalse['FECHA'] >= fecha_referencia - pd.Timedelta(days=14)]
    df_1m = df_embalse[df_embalse['FECHA'] >= fecha_referencia - pd.Timedelta(days=30)]
    
    # Filtros históricos del mismo mes
    df_hist_mes = df_embalse[df_embalse['FECHA'].dt.month == mes_ref]
    
    def media_historica(anios):
        fecha_limite = fecha_referencia - relativedelta(years=anios)
        return df_hist_mes[df_hist_mes['FECHA'] >= fecha_limite]['AGUA_ACTUAL'].mean()

    # Mismo mes año anterior
    fecha_anio_ant_inicio = fecha_referencia - relativedelta(years=1, months=1)
    fecha_anio_ant_fin = fecha_referencia - relativedelta(years=1) + relativedelta(months=1)
    df_anio_ant = df_embalse[(df_embalse['FECHA'] > fecha_anio_ant_inicio) & 
                             (df_embalse['FECHA'] <= fecha_anio_ant_fin) & 
                             (df_embalse['FECHA'].dt.month == mes_ref)]

    stats = {
        'aa': round(df_embalse['AGUA_ACTUAL'].iloc[0], 2),
        'at': round(df_embalse['AGUA_TOTAL'].iloc[0], 2),
        'f': fecha_referencia.strftime('%Y-%m-%d'),
        'm1s': round(df_1s['AGUA_ACTUAL'].mean(), 2),
        'm2s': round(df_2s['AGUA_ACTUAL'].mean(), 2),
        'm1m': round(df_1m['AGUA_ACTUAL'].mean(), 2),
        'ma1': round(df_anio_ant['AGUA_ACTUAL'].mean(), 2) if not df_anio_ant.empty else None,
        'h3a': round(media_historica(3), 2),
        'h5a': round(media_historica(5), 2),
        'h10a': round(media_historica(10), 2),
        'h20a': round(media_historica(20), 2),
        'ht': round(df_hist_mes['AGUA_ACTUAL'].mean(), 2)
    }
    return pd.Series(stats)

def procesar_datos():
    with Progress(
        SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
        BarColumn(), TimeElapsedColumn(), console=console
    ) as progress:
        
        # 1. Extracción con mdb-export
        task1 = progress.add_task("[yellow]Extrayendo datos MDB...", total=None)
        try:
            proceso = subprocess.Popen(['mdb-export', MDB_FILE, TABLE_NAME], 
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = proceso.communicate()
            if proceso.returncode != 0:
                raise Exception(f"mdb-export falló: {stderr.decode('utf-8')}")
            
            df = pd.read_csv(io.BytesIO(stdout), dtype=str)
            progress.update(task1, completed=100)
        except Exception as e:
            console.print(f"[bold red]✖ Error leyendo MDB: {e}[/bold red]")
            sys.exit(1)

        # 2. Limpieza
        task2 = progress.add_task("[yellow]Limpiando y tipando datos...", total=None)
        df.columns = [c.split('"')[0].strip().replace(' ', '_') for c in df.columns]
        for col in ['AGUA_TOTAL', 'AGUA_ACTUAL']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].str.replace(',', '.', regex=False), errors='coerce')
        
        df['FECHA'] = pd.to_datetime(df['FECHA'], dayfirst=True, errors='coerce')
        df = df.dropna(subset=['AGUA_TOTAL', 'AGUA_ACTUAL', 'AMBITO_NOMBRE', 'EMBALSE_NOMBRE', 'FECHA'])
        progress.update(task2, completed=100)

        # 3. Cálculos Estadísticos
        task3 = progress.add_task("[yellow]Calculando estadísticas (puede tardar unos segundos)...", total=None)
        # Agrupamos y aplicamos la función estadística a cada embalse
        df_agrupado = df.groupby(['AMBITO_NOMBRE', 'EMBALSE_NOMBRE']).apply(calcular_estadisticas_embalse).reset_index()
        
        # Renombrar columnas de agrupación a formato corto
        df_agrupado.rename(columns={'AMBITO_NOMBRE': 'an', 'EMBALSE_NOMBRE': 'en'}, inplace=True)
        progress.update(task3, completed=100)

# 4. Exportación JSON compacta e híbrida (Legible por humanos)
        task4 = progress.add_task("[yellow]Generando JSON optimizado y legible...", total=None)
        
        # Manejo de NaNs (reemplazando por None para que JSON lo serialice como null)
        df_agrupado = df_agrupado.where(pd.notnull(df_agrupado), None)
        
        export_data = {
            "metadatos": METADATA,
            "datos": df_agrupado.to_dict(orient='records')
        }
        
        # --- ESCRITURA HÍBRIDA ---
        with open(JSON_OUTPUT, 'w', encoding='utf-8') as f:
            # 1. Escribimos los metadatos con indentación normal
            f.write('{\n  "metadatos": ')
            json.dump(export_data["metadatos"], f, ensure_ascii=False, indent=2)
            
            # 2. Abrimos la lista de datos
            f.write(',\n  "datos": [\n')
            
            # 3. Escribimos cada registro en una única línea
            records = export_data["datos"]
            for i, record in enumerate(records):
                # separators=(',', ':') elimina espacios innecesarios dentro de la misma línea
                json_str = json.dumps(record, ensure_ascii=False, separators=(',', ':'))
                f.write(f'    {json_str}')
                
                # Añadimos coma y salto de línea salvo en el último registro
                if i < len(records) - 1:
                    f.write(',\n')
                else:
                    f.write('\n')
                    
            # 4. Cerramos el JSON
            f.write('  ]\n}\n')
        
        progress.update(task4, completed=100)

def main():
    console.rule("[bold blue]Iniciando ETL Hidrológico MITECO")
    
    descargar_y_extraer()
    
    # Control de Estado (Hash)
    current_hash = get_file_hash(MDB_FILE)
    if os.path.exists(HASH_FILE):
        with open(HASH_FILE, 'r') as f:
            last_hash = f.read().strip()
            
        if current_hash == last_hash:
            console.print("[green]✔ El archivo MDB no ha cambiado (Hash idéntico). Proceso finalizado para ahorrar recursos.[/green]")
            os.remove(MDB_FILE) # Limpiamos el MDB
            sys.exit(0)
    
    console.print("[yellow]Nuevos datos detectados. Iniciando procesamiento...[/yellow]")
    procesar_datos()
    
    # Guardar nuevo hash y limpiar
    with open(HASH_FILE, 'w') as f:
        f.write(current_hash)
    os.remove(MDB_FILE)
    
    tamaño_kb = os.path.getsize(JSON_OUTPUT) / 1024
    console.print(f"[bold green]✔ Pipeline completado con éxito.[/bold green]")
    console.print(f"[green]Archivo generado: {JSON_OUTPUT} ({tamaño_kb:.1f} KB)[/green]")
    console.rule("[bold blue]Fin del Proceso")

if __name__ == "__main__":
    main()