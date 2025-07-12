
import os 
import sys 
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException
from datetime import datetime
import yaml
from glob import glob
import pandas as pd
from collections import defaultdict
import subprocess
import re
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe



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

def site_operation(ACTIONS, driver, timeout):
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


def processing_csv(download_folder, data_access):
    print(message_print("Iniciando la función para procesar CSVs"))

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
    print(dict_files)
    print(message_print("Procesando y agrupando archivos por fecha y tipo"))

    grouped_files = defaultdict(list)

    for file_path, info in dict_files.items():
        key = (info['year'], info['month'], info['day'], info['type'])
        grouped_files[key].append(file_path)

    for (year, month, day, typ), file_list in grouped_files.items():
        dataframes = []
        loaded_hashes = set()

        for each_file in file_list:
            try:
                try:
                    df = pd.read_csv(each_file, encoding='utf-8')
                except UnicodeDecodeError:
                    df = pd.read_csv(each_file, encoding='latin1')  # fallback
                df_hash = pd.util.hash_pandas_object(df, index=True).sum()

                if df_hash in loaded_hashes:
                    print(f"⚠️ Archivo duplicado: {os.path.basename(each_file)} — será ignorado.")
                    continue

                loaded_hashes.add(df_hash)
                dataframes.append(df)

            except Exception as e:
                print(f"❌ Error al procesar '{os.path.basename(each_file)}': {e}")

        if not dataframes:
            print(f"⚠️ No se cargaron archivos válidos para {year}-{month}-{day} ({typ})")
            continue

        final_df = pd.concat(dataframes, ignore_index=True)

        csv_folder = os.path.join(working_folder, f"{year}-{month:02d}")
        create_directory_if_not_exists(csv_folder)

        csv_path = os.path.join(csv_folder, f"{year}-{month:02d}-{day:02d}_{typ}.csv")
        final_df.to_csv(csv_path, index=False)
        print(f"✅ Guardado: {csv_path}")

def credit_closed_by_month(path_TC_closed, process_closed_credit_accounts, export_pickle, headers_credit, check_only=True, debit = False):
    pickle_file = os.path.join(path_TC_closed, 'pickle_database.pkl')
    
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

        # Obtener año y mes actuales
        now = datetime.now()
        year = now.year
        month = now.month

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
        choice = input("¿Quieres exportar la información al corte a tu carpeta de descargas? (si/no): ").strip().lower()

        if choice == "si":
            if debit: 
                filename= 'Debito_al_corte'
            else: 
                filename = 'TC_al_corte'
            output_tc_al_corte = os.path.expanduser(f"~/Downloads/{filename}.xlsx")
            export_pickle(pickle_file, output_tc_al_corte)
            print(f"✅ Archivo exportado a: {output_tc_al_corte}")


def credit_current_month(working_folder, check_only=True):
    """
    Para cada subcarpeta de `working_folder` con nombre 'YYYY-MM':
      1) Lee todos los archivos '*_credit.csv' (nombres 'YYYY-MM-DD…_credit.csv').
      2) Carga o crea '{YYYY-MM}_credit_summary.xlsx'.
      3) Añade sólo los renglones nuevos (comparando todos los campos excepto 'filename').
      4) Guarda el resumen actualizado.
    """
    folder_pattern = re.compile(r'^\d{4}-\d{2}$')
    print(message_print('Iniciando el script de gastos después del corte'))
    for folder_name in os.listdir(working_folder):
        folder_path = os.path.join(working_folder, folder_name)
        if not os.path.isdir(folder_path) or not folder_pattern.match(folder_name):
            continue
        #print('Folder de trabajo', os.path.basename(working_folder))
        #print('Folder de trabajo', os.path.basename(folder_name))
        # ——— Sección MFI ———
        # 1) Agregar lista de archivos MFI dentro de la subcarpeta
        mfi_pattern = os.path.join(folder_path, '*_MFI.csv')
        MFI_files = sorted(glob(mfi_pattern))
        
        if MFI_files:
            # 2) Quedarse solo con el más reciente (último en el sort lexicográfico YYYY-MM-DD_MFI.csv)
            mfi_file = MFI_files[-1]
            basename = os.path.basename(mfi_file)
            file_date = datetime.strptime(basename.split('_')[0], '%Y-%m-%d')
            # 3) Definir ruta de resumen dentro de la misma subcarpeta
            MFI_summary_path = os.path.join(
                folder_path,
                f"{folder_name}_MFI.xlsx"
            )

            # 4) Leer todo el CSV
            try:
                df_brut_msi = pd.read_csv(
                    mfi_file,
                    encoding='utf-8-sig',
                    sep=',',
                    skipinitialspace=True  # drop spaces immediately after commas
                )
            except Exception:
                df_brut_msi = pd.read_csv(mfi_file, encoding='utf-8')
            print(repr(df_brut_msi.columns.tolist()))
            # 5) Normalizar fecha y fijar al mes actual
            #MFI_files_date = mfi_file has the format 2025-06-04_MFI.csv, we need to extract yyyy-mm-dd as date time 
            df_brut_msi['Fecha de operación'] = pd.to_datetime(
                df_brut_msi['Fecha de operación'],
                format='%d/%m/%Y'
            ).apply(lambda x: x.replace(month=file_date.month))

            # 6) Función para duplicar filas con offset de meses
            def duplicate_rows_with_incremented_months(df):
                new_rows = []
                for _, row in df.iterrows():
                    pagos, suffix = row['Pagos pendientes'].split('/')
                    count = int(pagos)
                    for i in range(count):
                        new_row = row.copy()
                        new_row['Fecha de operación'] = (
                            new_row['Fecha de operación']
                            + pd.DateOffset(months=i+1)
                        )
                        new_row['Pagos pendientes'] = f"{count-i-1:02d}/{suffix}"
                        new_rows.append(new_row)
                duplicated = pd.DataFrame(new_rows, columns=df.columns)
                return pd.concat([df, duplicated], ignore_index=True)\
                         .sort_values(by='Fecha de operación')
            print(df_brut_msi.head(10))
            # 7) Generar DataFrame final e ignorar duplicados entre offsets
            df_brut_msi = duplicate_rows_with_incremented_months(df_brut_msi)

            # 8) Guardar el resumen a Excel
            df_brut_msi.to_excel(MFI_summary_path, index=False)
            print(f"✅ Guardado MFI summary: {MFI_summary_path}")
        else:
            df_brut_msi = pd.DataFrame()
            print(f"⚠️ No se encontraron archivos *_MFI.csv en {folder_path}")
            

        # ——— Fin sección MFI ———
        # """"
        # """"        
        # ——— Inicia sección Crédito después del corte ———        
        print('\nInicia sección Crédito después del corte')
        csv_files = sorted(glob(os.path.join(folder_path, '*_credit.csv')))
        summary_path = os.path.join(working_folder, f"{folder_name}", f"{folder_name}_credit_summary.xlsx")

        # Carga o crea df_summary
        if os.path.exists(summary_path):
            df_summary = pd.read_excel(summary_path)
            df_summary['Fecha'] = pd.to_datetime(
                df_summary['Fecha'],
                dayfirst=True,               # 'dd/mm/YYYY'
                format='%d/%m/%Y',
                errors='coerce'              # bad parse → NaT
            )            
        else:
            df_summary = pd.DataFrame()

        for csv_file in csv_files:
            base = os.path.basename(csv_file)
            df_new = pd.read_csv(csv_file)
            df_new['filename'] = base

            # Columnas de datos (sin 'filename')
            data_cols = [c for c in df_new.columns if c != 'filename']

            if df_summary.empty:
                # Primera lectura: todo entra
                df_summary = df_new.copy()
            else:
                # Para cada renglón de df_new, sólo agregar si no existe ya en df_summary
                for _, row in df_new.iterrows():
                    iguales = (df_summary[data_cols] == row[data_cols]).all(axis=1)
                    if not iguales.any():
                        # concatena ese único renglón
                        df_summary = pd.concat(
                            [df_summary, pd.DataFrame([row])],
                            ignore_index=True
                        )
        # Guardar el resumen actualizado
        df_summary.to_excel(summary_path, index=False)

        #print(df_summary.head(10))
        if not df_brut_msi.empty:
            print(message_print('Uniendo información de MSI con información después del corte'))

            df_summary['Fecha'] = pd.to_datetime(
                df_summary['Fecha'],
                dayfirst=True,
                errors='coerce'
            )

            for _, row in df_brut_msi.iterrows():
                fecha_op = row['Fecha de operación'].date()   # datetime.date
                concepto = row['Concepto']
                cargo    = row['Mensualidad']

                # now compare against the plain-date series
                existe = (
                    (df_summary['Fecha'] == fecha_op) &
                    (df_summary['Concepto'] == concepto) &
                    (df_summary['Cargo'] == cargo)
                ).any()

                if not existe:
                    nueva = {
                        'Fecha':    fecha_op,
                        'Concepto': concepto,
                        'Cargo':    cargo,
                        'filename': basename
                    }
                    df_summary = pd.concat([df_summary, pd.DataFrame([nueva])], ignore_index=True)
                # Guardar el resumen actualizado
                df_summary.to_excel(summary_path, index=False)

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
    



def total_management(chrome_driver_load, folder_root, ACTIONS, process_closed_credit_accounts, export_pickle): 
    working_folder= os.path.join(folder_root, "Implementación")
    data_access = yaml_creation(working_folder)
    downloads = "Temporal Downloads"
    download_folder = os.path.join(working_folder, downloads)
    path_TC_closed = os.path.join(working_folder, 'TC al corte')
    path_DEBIT_closed = os.path.join(working_folder, 'Débito al mes')
    create_directory_if_not_exists([working_folder, download_folder, path_DEBIT_closed])
    add_to_gitignore(folder_root, working_folder)
    headers_credit = data_access['BANORTE_credit_headers']
    headers_debit  = data_access['BANORTE_debit_headers']
    credit_closed_by_month(path_TC_closed, process_closed_credit_accounts, export_pickle, headers_credit)
    
    ACTIONS["https://www.banorte.com/wps/portal/ixe/Home/inicio"][0]["value"] = data_access["BANORTE_user"]
    ACTIONS["https://www.banorte.com/wps/portal/ixe/Home/inicio"][2]["value"] = data_access["BANORTE_password"]

    print("Cargando el usuario y contraseña registrados en el YAML")


    while True:
        choice = input(f"""{message_print('¿Qué deseas hacer?')}
    1. Descargar archivos CSV
    2. Procesar archivos CSV
    3. Cargar mes de corte
    4. Cargar gastos posterior al corte
    5. Actualizar el google sheet
    0. Salir
    Elige una opción (1, 2 o 3): """)

        if choice == "1":
            driver = chrome_driver_load(download_folder)
            timeout = 20
            site_operation(ACTIONS, driver, timeout)            
        elif choice == "2":
            print("📦 Procesando archivos CSV...")
            processing_csv(download_folder, data_access)
        elif choice == "3":
            print("💰 Cargando corte de crédito cerrado")
            credit_closed_by_month(path_TC_closed, process_closed_credit_accounts, export_pickle, headers_credit, check_only=False, debit = False)
            print("💰 Cargando corte de débito al mes")
            credit_closed_by_month(path_DEBIT_closed, process_closed_credit_accounts, export_pickle, headers_debit, check_only=False, debit = True)
            # Aquí puedes llamar a la función correspondiente
            
        elif choice == "4":
            print("💰 Cargando gastos posterior al corte")
            credit_current_month(working_folder, check_only=True)
        
        elif choice == "5":
            print("💰 Cargando gastos posterior al corte")
            upload_data(working_folder, data_access)
        elif choice == "0":
            print("👋 ¡Hasta luego!")
            break            
        else:
            print("⚠️ Opción no válida. Por favor elige 1, 2, 3, 4 o 5 .\n")


if __name__ == "__main__":
    folder_root = os.getcwd()
    # 1) Añade al path la carpeta donde está df_multi_match.py
    libs_dir = os.path.join(folder_root, "Librería")
    print(libs_dir)
    sys.path.insert(0, libs_dir)

    # 2) Ahora importa la función directamente
    from chrome_driver_load import load_chrome
    from credit_closed import process_closed_credit_accounts, export_pickle

    ACTIONS = {
    "https://www.banorte.com/wps/portal/ixe/Home/inicio": [
        {"type": "send_keys", "by": By.XPATH, "locator": '//*[@id="userid"]',    "value": "Abre el YAML y escriibe tu usuario"},
        {"type": "click",     "by": By.XPATH, "locator": '//*[@id="btn_lgn_entrar"]'},
        {"type": "send_keys", "by": By.XPATH, "locator": '//*[@id="passwordLogin"]',    "value": "Abre el YAML y escriibe tu contraseña"},
        {"type": "wait_user", "value": "Por favor ingresa tu token y presiona enter en la terminal"},
        {"type": "click",     "by": By.XPATH, "locator":'//*[@id="btnAceptarloginPasswordAsync"]'}
    ],} 
    # 3) Llama a tu función pasándola como parámetro
    total_management(load_chrome, folder_root, ACTIONS, process_closed_credit_accounts, export_pickle)