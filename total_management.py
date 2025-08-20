
import os 
import sys 
import shutil
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException
from datetime import datetime, date
import yaml
from glob import glob
import pandas as pd
from collections import defaultdict
import subprocess
import re
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe
from modulos_git.business_management import business_management
import numpy as np
import pickle

# Variables globales
folder_root = None
chrome_driver_load = None
ACTIONS = None
process_closed_credit_accounts = None
export_pickle = None

def initialize_globals():
    """Inicializa las variables globales necesarias para el funcionamiento del programa."""
    global folder_root, chrome_driver_load, ACTIONS, process_closed_credit_accounts, export_pickle
    
    folder_root = os.getcwd()
    
    # Añade al path la carpeta donde están las librerías
    libs_dir = os.path.join(folder_root, "Librería")
    sys.path.insert(0, libs_dir)

    # Importa las funciones necesarias
    from chrome_driver_load import load_chrome
    from credit_closed import process_closed_credit_accounts as pcc, export_pickle as ep
    
    chrome_driver_load = load_chrome
    process_closed_credit_accounts = pcc
    export_pickle = ep

    ACTIONS = {
        "https://www.banorte.com/wps/portal/ixe/Home/inicio": [
            {"type": "send_keys", "by": By.XPATH, "locator": '//*[@id="userid"]',    "value": "Abre el YAML y escriibe tu usuario"},
            {"type": "click",     "by": By.XPATH, "locator": '//*[@id="btn_lgn_entrar"]'},
            {"type": "send_keys", "by": By.XPATH, "locator": '//*[@id="passwordLogin"]',    "value": "Abre el YAML y escriibe tu contraseña"},
            {"type": "wait_user", "value": "Por favor ingresa tu token y presiona enter en la terminal"},
            {"type": "click",     "by": By.XPATH, "locator":'//*[@id="btnAceptarloginPasswordAsync"]'}
        ],
    }

def open_folder(os_path):
    """Opens a folder in the appropriate file explorer depending on the OS."""
    message_open = "A explorer windows was open, please move the files here"
    print(message_print(message_open))
    try:
        if os.name == 'nt':  # Windows
            os.startfile(os_path)
        elif os.name == 'posix':  # macOS or Linux
            if "darwin" in os.uname().sysname.lower():  # macOS
                subprocess.run(["open", os_path])
            else:  # Linux
                subprocess.run(["xdg-open", os_path])
        else:
            print(f"Unsupported OS: {os.name}")
    except Exception as e:
        print(f"Error opening folder: {e}")

def create_directory_if_not_exists(path_or_paths):
    """Creates a directory if it does not exist and prints in Jupyter."""
    message_create_directory_if_not_exists = 'Confirmando que los folders necesarios existen'
    print(message_print(message_create_directory_if_not_exists))
    if isinstance(path_or_paths, str):
        paths = [path_or_paths]
    elif isinstance(path_or_paths, list):
        paths = path_or_paths
    else:
        raise TypeError("El argumento debe ser un string o una lista de strings.")

    for path in paths:
        if not os.path.exists(path):
            print(f"\n\tNo se localizó el folder {os.path.basename(path)}, creando.", flush=True)
            os.makedirs(path)
            print(f"\tFolder {os.path.basename(path)} creado.", flush=True)
        else:
            print(f"\tFolder {os.path.basename(path)} encontrado.", flush=True)


def yaml_creation(download_folder): 
    output_yaml = os.path.join(download_folder, "passwords.yaml")
    yaml_exists = os.path.exists(output_yaml)

    if yaml_exists:
        # Abrir y cargar el contenido YAML en un diccionario
        with open(output_yaml, 'r', encoding='utf-8') as f:
            data_access = yaml.safe_load(f)
        print(f"Archivo cargado correctamente: {os.path.basename(output_yaml)}")
        return data_access

    else: 
        print(message_print("No se localizó un yaml válido, vamos a crear uno con: "))
        platforms = ["BANORTE"] # Los bancos
        fields    = ["url", "user", "password", "month_free_headers", "credit_headers", "debit_headers"] # Cada variable de los bancos
        
        lines = []
        for platform in platforms:
            for field in fields:
                # clave = valor vacío
                lines.append(f"{platform}_{field}: ")
            lines.append("")  # línea en blanco entre bloques
        
        # Escribe el archivo YAML (aunque use "=" tal como en tu ejemplo)
        with open(output_yaml, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))

def message_print(message): 
    message_highlights= '*' * len(message)
    message = f'\n{message_highlights}\n{message}\n{message_highlights}\n'
    return message

def add_to_gitignore(root_directory, path_to_add):
    gitignore_path = os.path.join(root_directory, ".gitignore")
    
    # La ruta que queremos ignorar, relativa al root
    
    #relative_output = "Output/"
    #relative_output = f"{os.path.basename(path_to_add)}\\"
    relative_output = f"{os.path.basename(path_to_add)}/"
    #print(relative_output)

    # Verifica si ya está en .gitignore, si no, lo agrega
    if os.path.exists(gitignore_path):
        with open(gitignore_path, 'r') as f:
            lines = f.read().splitlines()
    else:
        lines = []

    if relative_output not in lines:
        with open(gitignore_path, 'a') as f:
            f.write(f"\n{relative_output}\n")
        print(f"'{relative_output}' agregado a .gitignore.")
    else:
        print(f"'{relative_output}' ya está listado en .gitignore.")

def site_operation(ACTIONS, driver, timeout,download_folder, data_access, path_al_corte = None, al_corte = False):

    # Navegación y automatización 
    
    for url, steps in ACTIONS.items():
        print(f"\n🔗 Navegando a {message_print(url)}")
        driver.get(url)

        try:
            for idx, step in enumerate(steps, start=1):
                typ = step["type"]
                print(f"  → Paso {idx}: {typ}", end="")
                if typ == "wait_user":
                    msg = step.get("value", "Presiona enter para continuar...")
                    print(f"\n    ⏸ {msg}")
                    input()  # Aquí espera al usuario antes de continuar
                    continue  # Saltamos cualquier búsqueda de elementos
                else:
                    by   = step["by"]
                    loc  = step["locator"]
                    typ  = step["type"]
                    print(f"  → Paso {idx}: {typ} en {loc}")

                    # use element_to_be_clickable for both click and send_keys
                    elem = WebDriverWait(driver, timeout).until(
                        EC.element_to_be_clickable((by, loc))
                    )

                    if typ == "click":
                        elem.click()
                        print(f"    ✓ Clicked {loc}")

                    elif typ == "send_keys":
                        # click once to focus, then clear and send
                        elem.click()
                        elem.clear()
                        elem.send_keys(step["value"])
                        print(f"    ✓ Sent keys (‘{step['value']}’) to {loc}")          
                    else:
                        raise ValueError(f"Tipo desconocido: {typ}")
                    

        except TimeoutException as e:
            print(f"    ✗ Timeout en paso {idx} ({typ} @ {loc}): {e}")           
    input(message_print("Presina enter para cerrar el navegador"))
    driver.quit()

    #                                      #
    # Preparación y lectura de encabezados #
    #                                      #

    print(message_print("Renombrando y fusionando los archivos descargados para mover a su carpeta"))    

    working_folder = os.path.abspath(os.path.join(download_folder, '..'))

    headers_credit = data_access['BANORTE_credit_headers']
    headers_debit  = data_access['BANORTE_debit_headers']
    headers_MFI    = data_access['BANORTE_month_free_headers']

    dict_files = {}

    csv_files = glob(os.path.join(download_folder, "*.csv"))

    for file in csv_files:
        file_info = {}

        # 🔍 Fecha de creación (última modificación, por compatibilidad multiplataforma)
        timestamp = os.path.getmtime(file)  # getctime para creación, getmtime para modificación
        dt = datetime.fromtimestamp(timestamp)
        file_info['year'] = dt.year
        file_info['month'] = dt.month
        file_info['day'] = dt.day

        try:
            try:
                df = pd.read_csv(file, nrows=1, encoding='utf-8')
            except UnicodeDecodeError:
                df = pd.read_csv(file, nrows=1, encoding='latin1')  # fallback
            
            file_headers = list(df.columns)

            if file_headers == headers_credit:
                file_info['type'] = 'credit'
            elif file_headers == headers_debit:
                file_info['type'] = 'debit'
            elif file_headers == headers_MFI:
                file_info['type'] = 'MFI'
            else:
                print(f"⚠️ Archivo '{os.path.basename(file)}' no coincide con ninguna categoría.")
                continue  # saltar este archivo si no coincide
        except Exception as e:
            print(f"❌ Error al leer '{os.path.basename(file)}': {e}")
            continue

        dict_files[file] = file_info  # ✅ Guardar todo en el diccionario principal

    print("\n📁 Archivos categorizados:")
    for f, info in dict_files.items():
        print(f"  - {os.path.basename(f)} → {info['type']} ({info['day']:02d}/{info['month']:02d}/{info['year']})")
    #print(dict_files)
    print(message_print("Procesando y agrupando archivos por fecha y tipo"))

    grouped_files = defaultdict(list)

    for file_path, info in dict_files.items():
        key = (info['year'], info['month'], info['day'], info['type'])
        grouped_files[key].append(file_path)
    # ─────────────────────────────────────────────────────────────────────────
    # MODO NORMAL (al_corte = False): igual que antes, guardar por día/tipo
    # ─────────────────────────────────────────────────────────────────────────
    if not al_corte:
        for (year, month, day, typ), file_list in grouped_files.items():
            print("Vamos a fusionar archivos del mismo mes, día y tipo (Debit, Credit, MFI) diferente cuenta")
            dataframes = []
            loaded_hashes = set()

            for each_file in file_list:
                try:
                    try:
                        df = pd.read_csv(each_file, encoding='utf-8')
                    except UnicodeDecodeError:
                        df = pd.read_csv(each_file, encoding='latin1')
                    df_hash = pd.util.hash_pandas_object(df, index=True).sum()

                    if df_hash in loaded_hashes:
                        print(f"⚠️ Archivo duplicado: {typ} {os.path.basename(each_file)} — será ignorado en la fusión.")
                        continue

                    loaded_hashes.add(df_hash)
                    dataframes.append(df)
                except Exception as e:
                    print(f"❌ Error al procesar '{os.path.basename(each_file)}': {e}")

            if not dataframes:
                print(f"⚠️ No se cargaron archivos válidos para {year}-{month}-{day} (Tipo: {typ})")
                continue

            final_df = pd.concat(dataframes, ignore_index=True)

            csv_folder = os.path.join(working_folder, f"{year}-{month:02d}")
            not os.path.exists(csv_folder) and create_directory_if_not_exists(csv_folder)

            csv_path = os.path.join(csv_folder, f"{year}-{month:02d}-{day:02d}_{typ}.csv")
            final_df.to_csv(csv_path, index=False)
            print(f"✅ Archivo movido: {os.path.basename(csv_path)}")

        # Limpiar descargas
        for f, _ in dict_files.items():
            print(f"🗑️ Eliminando {os.path.basename(f)} debido a que ya se procesó")
            os.remove(f)

        print(message_print("Fin de la descarga y movimiento de archivos a sus carpetas, continúa procesando los CSV en cada carpeta (Opción 2)"))
        return

    # ─────────────────────────────────────────────────────────────────────────
    # MODO AL CORTE (al_corte = True): un solo CSV mensual YYYY-MM.csv
    # ─────────────────────────────────────────────────────────────────────────
    # Validaciones mínimas
    if al_corte and not path_al_corte:
        raise ValueError("path_al_corte es requerido cuando al_corte=True")

    now = datetime.now()
    year = now.year
    month = now.month
    final_name = f"{year}-{month:02d}.csv"

    # 1) Fusionar TODOS los CSV del download_folder (evitando duplicados exactos)
    print(message_print("Fusionando todos los CSV descargados en un único archivo mensual"))
    merged_frames = []
    seen_hashes = set()
    for file_path in dict_files.keys():
        try:
            try:
                df = pd.read_csv(file_path, encoding='utf-8')
            except UnicodeDecodeError:
                df = pd.read_csv(file_path, encoding='latin1')
            df_hash = pd.util.hash_pandas_object(df, index=True).sum()
            if df_hash in seen_hashes:
                print(f"⚠️ Duplicado exacto detectado y omitido: {os.path.basename(file_path)}")
                continue
            seen_hashes.add(df_hash)
            merged_frames.append(df)
        except Exception as e:
            print(f"❌ Error al procesar '{os.path.basename(file_path)}': {e}")

    if not merged_frames:
        print("⚠️ No se encontraron CSV válidos para fusionar en modo al_corte.")
        return

    merged_df = pd.concat(merged_frames, ignore_index=True)

    # 2) Asegurar que en download_folder quede SOLO el archivo final
    #    Guardamos primero como temporal, luego borramos los demás y renombramos.
    temp_path = os.path.join(download_folder, "__merged_temp.csv")
    merged_df.to_csv(temp_path, index=False)
    print(f"📝 Archivo temporal creado: {os.path.basename(temp_path)}")

    for f in glob(os.path.join(download_folder, "*.csv")):
        if os.path.abspath(f) != os.path.abspath(temp_path):
            print(f"🗑️ Eliminando original: {os.path.basename(f)}")
            os.remove(f)

    final_download_path = os.path.join(download_folder, final_name)
    # Si por algún motivo existe, lo quitamos para renombrar limpio
    if os.path.exists(final_download_path):
        os.remove(final_download_path)
    os.rename(temp_path, final_download_path)
    print(f"✅ Archivo final en descargas: {os.path.basename(final_download_path)}")

    # 3) Mover a path_al_corte con el nombre YYYY-MM.csv
    create_directory_if_not_exists(path_al_corte)
    dest_path = os.path.join(path_al_corte, final_name)
    # Si existe en destino, lo sustituimos
    if os.path.exists(dest_path):
        os.remove(dest_path)
    shutil.move(final_download_path, dest_path)
    print(f"📦 El archivo {os.path.basename(final_name)} se movió a {os.path.basename(path_al_corte)}")


def processing_csv_post_cut(working_folder, data_access):
    """
    Busca en working_folder la subcarpeta del mes actual YYYY-MM, 
    selecciona el archivo más reciente de cada grupo (_credit.csv, _debit.csv, _stdMFI.csv),
    avisa si hay retraso, y carga tres DataFrames: df_credit, df_debit y df_mfi.
    """
    print(message_print("Procesando CSVs posteriores al corte"))
    # Fecha de hoy
    today: date = datetime.now().date()

    # Carpeta del mes actual
    yyyy = f"{today.year:04d}"
    mm   = f"{today.month:02d}"
    monthly_folder = os.path.join(working_folder, f"{yyyy}-{mm}")

    if not os.path.isdir(monthly_folder):
        print(f"La carpeta correspondiente al mes {mm} de {yyyy} no se encontró, descarga archivos y regresa.")
        return None, None, None

    # Definimos sufijos y claves
    groups = {
        "_credit.csv": "credit",
        "_debit.csv":  "debit",
        "_stdMFI.csv": "mfi"
    }
    latest_paths = {}

    def _parse_date_from_name(fn: str) -> date:
        # fn ejemplo: "2025-07-25_credit.csv" -> extrae "2025-07-25"
        basename = os.path.basename(fn)
        date_str = basename.split("_")[0]  # "2025-07-25"
        try:
            return datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return date.min  # para que quede al final si no coincide

    # Iterar por cada grupo, buscar y seleccionar el más reciente
    for suffix, key in groups.items():
        files = [f for f in os.listdir(monthly_folder) if f.endswith(suffix)]
        if not files:
            print(f"No existe archivo para la categoría {suffix}.")
            continue

        # Ordenar por fecha extraída del nombre, descendente
        files.sort(key=lambda fn: _parse_date_from_name(fn), reverse=True)
        newest = files[0]
        file_date = _parse_date_from_name(newest)
        delay = (today - file_date).days
        if delay != 0:
            print(f"El archivo de {suffix} tiene {delay} día(s) de retraso con respecto a hoy ({today}).")
        latest_paths[key] = os.path.join(monthly_folder, newest)

    # Helper para leer CSV
    def _read_csv(p):
        if data_access and hasattr(data_access, "read_csv"):
            return data_access.read_csv(p)
        else:
            return pd.read_csv(p)

    # Cargar DataFrames (o None si no había archivo)
    df_credit = _read_csv(latest_paths["credit"]) if "credit" in latest_paths else None
    df_debit  = _read_csv(latest_paths["debit"])  if "debit"  in latest_paths else None
    df_mfi    = _read_csv(latest_paths["mfi"])    if "mfi"    in latest_paths else None    
    
    #print(latest_paths["credit"])
    #print(df_credit.head(20))

    for key, df in (("credit", df_credit),
                    ("debit",  df_debit),
                    ("mfi",    df_mfi)):
        if df is not None and key in latest_paths:
            df["filename"] = os.path.basename(latest_paths[key])

    ## CARGAR EN GOOGLE SHEET

    # Define the scope
    scope = ['https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive']
    # Add your service account file
    json_path = os.path.join(working_folder, 'armjorgeSheets.json')
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_path, scope)  # Ensure the correct path
    # Authorize the client sheet
    client = gspread.authorize(creds)
    url = data_access['url_google_sheet']
    spreadsheet = client.open_by_url(url)

    def clean_for_sheets(df):
        # convert any +Inf/−Inf to NaN, then replace all NaNs
        df = df.replace([np.inf, -np.inf], np.nan)
        return df.fillna("")   # or .fillna(0) if you want zeros

    def update_google_sheet(sheet_name, df):
        ws = spreadsheet.worksheet(sheet_name)
        ws.clear()

        # 1) If you want literal strings "11/14/2024" in M/D/YYYY...
        print(f"\tCambiando el tipo de la columna Fecha a date de la hoja {sheet_name}\n")
        if 'Fecha' in df.columns:
            df['Fecha'] = (
                pd.to_datetime(df['Fecha'], errors='coerce')
                .dt.strftime('%m/%d/%Y')      # → "11/14/2024"
            )
        df_clean = clean_for_sheets(df)

        values = [df_clean.columns.tolist()] + df_clean.values.tolist()

        # 3) Use RAW so Sheets never re-parse them as ISO
        spreadsheet.values_update(
            f"{sheet_name}!A1",
            params={'valueInputOption': 'RAW'},
            body={'values': values}
        )

    # Update the sheets with dataframes from df_informacion_actualizada
    print("\nCargando a google sheet los archivos encontrados\n")
    #df_credit['filename'] = 
    #print('latest paths', latest_paths)
    update_google_sheet('Credit_current', df_credit)
    update_google_sheet('Debit_current', df_debit)    
    return df_credit, df_debit, df_mfi



def credit_closed_by_month(path_TC_closed, headers_credit, check_only=True, debit = False):
    global process_closed_credit_accounts, export_pickle
    
    pickle_file = os.path.join(path_TC_closed, 'pickle_database.pkl')
    # Obtener año y mes actuales
    now = datetime.now()
    year = now.year
    month = now.month

    if check_only:
        if not os.path.exists(pickle_file):
            print(f"⚠️ No se encontró el archivo: {pickle_file}")
            return
        
        df = pd.read_pickle(pickle_file)
        #print('\n📂 Columnas del DataFrame:\n', df.columns, '\n')

        unique_files_raw = df['file_name'].unique()

        # Extraer solo la parte antes del __HASH__
        cleaned_files = [x.split('__HASH__')[0] for x in unique_files_raw]

        #print("🧾 Archivos únicos encontrados:")
        #print(cleaned_files)


        # Crear lista de nombres esperados
        expected = [f"{year}-{m:02d}.csv" for m in range(month, 0, -1)]

        #print("\n🔎 Esperando encontrar:")
        #print(expected)

        # Comparar
        missing = [f for f in expected if f not in cleaned_files]

        if missing:
            print(message_print("⚠️ Cortes faltantes:"))
            for f in missing:
                print(f"  - {f}")
        else:
            print("\n✅ Todos los archivos mensuales esperados están presentes.")

    else:
        process_closed_credit_accounts(path_TC_closed, headers_credit, open_folder, debit)
        print("Exportando información post-corte a tu carpeta de descargas")
        output_tc_al_corte = os.path.expanduser(f"~/Downloads/Info al corte.xlsx")
        export_pickle(pickle_file, output_tc_al_corte)
        print(f"✅ Archivo exportado a: {output_tc_al_corte}")


def upload_data(working_folder, data_access):
    """
    1) Carga pickle_database.pkl de 'TC al corte'
    2) Detecta la carpeta más reciente YYYY-MM dentro de working_folder
    3) Busca y carga:
       - {YYYY-MM}_MFI.xlsx  → df_mfi
       - *_credit.csv         → df_post_corte (el más reciente)
       - *_debit.csv          → df_debito     (el más reciente)
    4) Devuelve cuatro DataFrames: al_corte, mfi, post_corte y debito.
    """
    # 1) Data al corte
    pickle_file = os.path.join(working_folder, 'TC al corte', 'pickle_database.pkl')
    df_al_corte = pd.read_pickle(pickle_file)
    df_al_corte['file_name'] = df_al_corte['file_name'].str.replace(r'__HASH__.*$', '', regex=True)
    debit_pickle = os.path.join(working_folder, 'Débito al mes', 'pickle_database.pkl')
    df_debito_corte = pd.read_pickle(debit_pickle)
    df_debito_corte['file_name'] = df_debito_corte['file_name'].str.replace(r'__HASH__.*$', '', regex=True)

    print(f"Archivos al corte cargados: {os.path.basename(pickle_file)}")
    # 2) Folder más reciente YYYY-MM
    candidates = [
        d for d in os.listdir(working_folder)
        if os.path.isdir(os.path.join(working_folder, d)) and re.match(r'^\d{4}-\d{2}$', d)
    ]
    if not candidates:
        raise FileNotFoundError(f"No se encontraron carpetas YYYY-MM en {working_folder}")
    newest_folder = sorted(candidates)[-1]
    newest_folder_path = os.path.join(working_folder, newest_folder)

    # 3a) MFI (.xlsx)
    mfi_pattern = os.path.join(newest_folder_path, f"{newest_folder}_MFI.xlsx")
    mfi_files = glob(mfi_pattern)
    newest_mfi = mfi_files[0] if mfi_files else None

    # 3b) Crédito posterior al corte (último *_credit.csv)
    credit_pattern = os.path.join(newest_folder_path, "*_credit.csv")
    credit_files = sorted(glob(credit_pattern))
    newest_post_corte = credit_files[-1] if credit_files else None

    # 3c) Débito (último *_debit.csv)
    debit_pattern = os.path.join(newest_folder_path, "*_debit.csv")
    debit_files = sorted(glob(debit_pattern))
    newest_debit = debit_files[-1] if debit_files else None

    # 3d) Cargar cada uno (o dataframe vacío)
    if newest_mfi:
        df_mfi = pd.read_excel(newest_mfi)
        print(f"Archivo MSI cargado: {os.path.basename(newest_mfi)}")
    else:
        print(f"⚠️ No se encontró {newest_folder}_MFI.xlsx en {os.path.basename(newest_folder_path)}")
        df_mfi = pd.DataFrame()

    if newest_post_corte:
        df_post_corte = pd.read_csv(newest_post_corte)
        df_post_corte['file_name'] = os.path.basename(newest_post_corte)
        print(f"Archivo post corte: {os.path.basename(newest_post_corte)}")
    else:
        print(f"⚠️ No se encontró ningún *_credit.csv en {newest_folder_path}")
        df_post_corte = pd.DataFrame()

    if newest_debit:
        df_debito = pd.read_csv(newest_debit)
        df_debito['file_name'] = os.path.basename(newest_debit)

        print(f"Archivo débito: {os.path.basename(newest_debit)}")
    else:
        print(f"⚠️ No se encontró ningún *_debit.csv en {newest_folder_path}")
        df_debito = pd.DataFrame()

    # Cambiando fechas. 
    # 1 Débito al corte             
    if not df_debito_corte.empty:
        df_debito_corte['Fecha'] = (
            pd.to_datetime(df_debito_corte['Fecha'], dayfirst=True, errors='coerce')
            .dt.date
        )
        print('df_debito_corte: ', df_debito_corte['Fecha'].head(10))
    # 2) Crédito después del corte post corte
    if not df_post_corte.empty: 
        df_post_corte['Fecha'] = (
            pd.to_datetime(df_post_corte['Fecha'], dayfirst=True, errors='coerce')
            .dt.date
        )
        print('df_post_corte: ', df_post_corte['Fecha'].head(10))
    # 3 Crédito cortes cerrados
    if not df_al_corte.empty:
        df_al_corte['Fecha'] = pd.to_datetime(
            df_al_corte['Fecha'],
            format='%Y-%m-%d',
            errors='coerce'
        ).dt.date   # ← yields pure datetime.date objects
        print('df_al_corte: ', df_al_corte['Fecha'].head(10))
    # 4 Débito abierto
    if not df_debito.empty:
        df_debito['Fecha'] = (
            pd.to_datetime(df_debito['Fecha'], dayfirst=True, errors='coerce')
            .dt.date
        )        
        print('df_debito: ', df_debito['Fecha'].head(10))
    # --- 5) Merge de MFI en post_corte si aplica ---
    basename_credit = os.path.basename(newest_post_corte) if newest_post_corte else 'N/A'
    if not df_mfi.empty and not df_post_corte.empty:
        fecha_series    = df_post_corte['Fecha'].dt.date
        concepto_series = df_post_corte['Concepto']
        cargo_series    = df_post_corte['Cargo']
        basename_mfi    = os.path.basename(newest_mfi)

        for _, row in df_mfi.iterrows():
            fecha_op = row['Fecha de operación'].date()
            concepto = row['Concepto']
            cargo    = row['Mensualidad']

            existe = (
                (fecha_series == fecha_op) &
                (concepto_series == concepto) &
                (cargo_series    == cargo)
            ).any()

            if not existe:
                nueva = {
                    'Fecha':    fecha_op,
                    'Concepto': concepto,
                    'Cargo':    cargo,
                    'filename': basename_mfi
                }
                df_post_corte = pd.concat(
                    [df_post_corte, pd.DataFrame([nueva])],
                    ignore_index=True
                )

        print(f"Se generó un DataFrame uniendo {basename_credit} y {basename_mfi}")
    else:
        print(f"Se generó un DataFrame solamente con ({basename_credit}) por no encontrar información MSI")

    ## CARGAR EN GOOGLE SHEET

    # Define the scope
    scope = ['https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive']
    # Add your service account file
    json_path = os.path.join(working_folder, 'armjorgeSheets.json')
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_path, scope)  # Ensure the correct path
    # Authorize the client sheet
    client = gspread.authorize(creds)
    url = data_access['url_google_sheet']
    spreadsheet = client.open_by_url(url)


    def update_google_sheet(sheet_name, df):
        ws = spreadsheet.worksheet(sheet_name)
        ws.clear()

        # 1) If you want literal strings "11/14/2024" in M/D/YYYY...
        if 'Fecha' in df.columns:
            df['Fecha'] = (
                pd.to_datetime(df['Fecha'], errors='coerce')
                .dt.strftime('%m/%d/%Y')      # → "11/14/2024"
            )

        values = [df.columns.tolist()] + df.values.tolist()

        # 3) Use RAW so Sheets never re-parse them as ISO
        spreadsheet.values_update(
            f"{sheet_name}!A1",
            params={'valueInputOption': 'RAW'},
            body={'values': values}
        )

    # Update the sheets with dataframes from df_informacion_actualizada
    update_google_sheet('Credit_closed', df_al_corte)
    update_google_sheet('Credit_current', df_post_corte)
    update_google_sheet('Debit_closed', df_debito_corte)    
    update_google_sheet('Debit_current', df_debito)    
    

def generador_conceptos(folder): 
    print(message_print("Iniciando el módulo para generar los conceptos y categorías presupuestales"))
    # 1. Generar hoja de conceptos para un folder dado: 
    #Necesitamos: a cada folder con la nomenclatura: buscar los _deb, _cred y _mfi.  
    #Acomodar por orden del nombre: más nuevo al principio
    #Cargar el actual si es que tiene datos llenos
    #Agregar columnas estandarizadas si es que aún no tiene
    #Generar un nuevo df si es que noe existe. 
    #Ir agregando nuevos renglones y la fecha en la que aparecieron
    #Agregar la info previa si es que el renglón ya estaba
    #Guardar como {yyyy}-{mm} xlsx con las hojas _credit _debit _mfi 
    # 2. Generar una copia del archivo de cortes débito y cortes crédito
    # Buscar el renglón excepto la columna de fecha para _debit: cortes débito y _credit: cortes crédito. 
    # Para el correspondiente {yyyy}-{mm}.csv : buscarn en su carpeta correspondiente, agregar columnas faltantes si es que no las tiene. 
    # Llenar con info previa 
    # No sustituir la info del renglón si el archivo trae un nan 
    # Guardar como {año} - {corte} {_debit} o _credit
    # Cargar a google sheet. 


# --- Helpers ---------------------------------------------------------------

def parse_fecha_ddmmyyyy(text):
    try:
        return datetime.strptime(text.strip(), "%d/%m/%Y").date()
    except ValueError:
        return None

def fechas_corte_tarjeta(working_folder):
    """
    Retorna (fecha_corte: date, estatus_fecha_corte: bool)
    estatus_fecha_corte es True si ya existía o se guardó una fecha válida.
    """
    
    today = date.today()
    year = today.year
    month = today.month  # 1..12
    index_row = month - 1  # para índice 0-based

    date_file = os.path.join(working_folder, f"{year} df_fechas_corte.pickle")
    months_name = ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
                   'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre']

    # Si no existe, crear dataframe base
    if not os.path.exists(date_file):
        df_fechas = pd.DataFrame({
            'Fecha corte dd-mm-yyyy': [None] * 12,
            'Mes': months_name
        })
        # Asegurar orden correcto
        df_fechas = df_fechas.set_index('Mes').loc[months_name].reset_index()
        with open(date_file, "wb") as f:
            pickle.dump(df_fechas, f)
        print(f"{date_file} creado con meses predefinidos.")

    # Cargar
    with open(date_file, "rb") as f:
        df_fechas = pickle.load(f)

    # Validar orden de meses; reordenar si hace falta
    if list(df_fechas['Mes']) != months_name:
        df_fechas = df_fechas.set_index('Mes').reindex(months_name).reset_index()

    # Obtener fecha existente
    raw_fecha = df_fechas.at[index_row, 'Fecha corte dd-mm-yyyy']
    fecha_corte = None
    estatus_fecha_corte = False

    if raw_fecha:
        if isinstance(raw_fecha, str):
            fecha_corte = parse_fecha_ddmmyyyy(raw_fecha)
        elif isinstance(raw_fecha, date):
            fecha_corte = raw_fecha
        elif isinstance(raw_fecha, datetime):
            fecha_corte = raw_fecha.date()

    if not fecha_corte:
        # No hay fecha válida: pedirla
        while True:
            entrada = input(f"Escribe la fecha de corte para {months_name[index_row]} en formato dd/mm/YYYY: ").strip()
            parsed = parse_fecha_ddmmyyyy(entrada)
            if parsed:
                fecha_corte = parsed
                df_fechas.at[index_row, 'Fecha corte dd-mm-yyyy'] = fecha_corte.strftime("%d/%m/%Y")
                with open(date_file, "wb") as f:
                    pickle.dump(df_fechas, f)
                print(f"Fecha de corte para {months_name[index_row]} guardada: {fecha_corte.strftime('%d/%m/%Y')}")
                estatus_fecha_corte = True
                break
            else:
                print("Formato inválido. Intenta de nuevo (ejemplo: 15/08/2025).")
    else:
        # Ya había fecha válida
        estatus_fecha_corte = True
        print(f"Fecha registrada existente para {months_name[index_row]}: {fecha_corte.strftime('%d/%m/%Y')}")
        # Preguntar si se quiere corregir, con timeout de 3 segundos si está disponible
        choice = input("¿Quieres corregir la fecha de corte? (Si o No): ").strip().lower()
        if choice in ("si", "sí", "s"):
            while True:
                nueva = input(f"Escribe la nueva fecha de corte para {months_name[index_row]} en formato dd/mm/YYYY: ").strip()
                parsed = parse_fecha_ddmmyyyy(nueva)
                if parsed:
                    fecha_corte = parsed
                    df_fechas.at[index_row, 'Fecha corte dd-mm-yyyy'] = fecha_corte.strftime("%d/%m/%Y")
                    with open(date_file, "wb") as f:
                        pickle.dump(df_fechas, f)
                    print(f"Fecha de corte actualizada: {fecha_corte.strftime('%d/%m/%Y')}")
                    break
                else:
                    print("Formato inválido. Intenta de nuevo.")
        else:
            # No quiere corregir; se mantiene la existente
            pass

    return fecha_corte, estatus_fecha_corte

def total_management(): 
    global folder_root, chrome_driver_load, ACTIONS, process_closed_credit_accounts, export_pickle
    
    working_folder= os.path.join(folder_root, "Implementación", "Info Bancaria")
    not os.path.exists(working_folder) and create_directory_if_not_exists(working_folder) 
    data_access = yaml_creation(working_folder)
    downloads = "Temporal Downloads"
    download_folder = os.path.join(working_folder, downloads)
    path_TC_closed = os.path.join(working_folder, 'TC al corte')
    path_DEBIT_closed = os.path.join(working_folder, 'Débito al mes')
    create_directory_if_not_exists([working_folder, download_folder, path_DEBIT_closed])
    add_to_gitignore(folder_root, working_folder)
    headers_credit = data_access['BANORTE_credit_headers']
    headers_debit  = data_access['BANORTE_debit_headers']
    credit_closed_by_month(path_TC_closed, headers_credit)
    
    ACTIONS["https://www.banorte.com/wps/portal/ixe/Home/inicio"][0]["value"] = data_access["BANORTE_user"]
    ACTIONS["https://www.banorte.com/wps/portal/ixe/Home/inicio"][2]["value"] = data_access["BANORTE_password"]

    print("Cargando el usuario y contraseña registrados en el YAML")
    timeout = 20

    while True:
        choice = input(f"""{message_print('¿Qué deseas hacer?')}
    1. Descargar, renombrar y mover archivos CSV corrientes (no cortes, no cerrados)
    2. Procesar archivos CSV posteriores al corte
    3. Procesar archivos CSV al mes corte
    0. Salir
    Elige una opción (1, 2 o 3): """)

        if choice == "1":
            driver = chrome_driver_load(download_folder)
            site_operation(ACTIONS, driver, timeout, download_folder, data_access)            
        elif choice == "2":
            print("📦 Procesando archivos CSV después del corte...")
            processing_csv_post_cut(working_folder, data_access)
        elif choice == "3":
            # Crédito 
            temporal_tc_files = os.path.join(path_TC_closed, 'Descargas temporales')
            not os.path.exists(working_folder) and create_directory_if_not_exists(temporal_tc_files) 
            print("Confirmando si hay un archivo de crédito para el último corte")
            fecha_corte, estatus_fecha_corte = fechas_corte_tarjeta(path_TC_closed)
            # Declaramos la fecha de hoy en el mismo formato que fecha_corte
            today = date.today()
            if today <= fecha_corte :
                print("💰 Esperando fecha de corte")
            elif today > fecha_corte :
                print("Nos falta el archivo al último corte, se va a abrir una ventana, por favor descarga: ")
                driver = chrome_driver_load(temporal_tc_files)
                site_operation(ACTIONS, driver, timeout, path_TC_closed, data_access, path_TC_closed, al_corte=True)
                input("Presiona enter si ya descargaste el archivo correspondiente al último corte")
                credit_closed_by_month(path_TC_closed, headers_credit, check_only=False, debit = False)
            # Débito
            print(message_print("Confirmando si hay un archivo de débito para el mes cerrado anterior"))
            credit_closed_by_month(path_DEBIT_closed, headers_debit, check_only=False, debit = True)
         
        elif choice == "0":
            print("👋 ¡Hasta luego!")
            break            
        else:
            print("⚠️ Opción no válida. Por favor elige 1, 2, 3, 4 o 5 .\n")


if __name__ == "__main__":
    # Inicializar variables globales
    initialize_globals()

    while True:
        choice = input(message_print("Elige: \n\t1) para la información bancaria  o \n\t2) para el módulo de gastos y presupuestos\n")).strip()

        if choice == "1":
            total_management()
            break
        elif choice == "2":
            business_management(folder_root)
            break
        else:
            print("\n⚠️ Elige una opción válida (1 o 2). Inténtalo de nuevo.\n")

