
import os 
import sys 
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException
import time
from datetime import datetime
import yaml
from glob import glob 
import pandas as pd
from collections import defaultdict


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
                    df = pd.read_csv(file, nrows=1, encoding='utf-8')
                except UnicodeDecodeError:
                    df = pd.read_csv(file, nrows=1, encoding='latin1')  # fallback
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

def total_management(chrome_driver_load, folder_root, ACTIONS): 
    working_folder= os.path.join(folder_root, "Implementación")
    downloads = "Temporal Downloads"
    download_folder = os.path.join(working_folder, downloads)
    create_directory_if_not_exists([working_folder, download_folder])
    add_to_gitignore(folder_root, working_folder)
    data_access = yaml_creation(working_folder)
    ACTIONS["https://www.banorte.com/wps/portal/ixe/Home/inicio"][0]["value"] = data_access["BANORTE_user"]
    ACTIONS["https://www.banorte.com/wps/portal/ixe/Home/inicio"][2]["value"] = data_access["BANORTE_password"]

    print("Cargando el usuario y contraseña registrados en el YAML")


    while True:
        choice = input("""
    ¿Qué deseas hacer?
    1. Descargar archivos CSV
    2. Procesar archivos CSV
    3. Procesar ingresos
    Elige una opción (1, 2 o 3): """)

        if choice == "1":
            driver = chrome_driver_load(download_folder)
            timeout = 20
            site_operation(ACTIONS, driver, timeout)
            break
        elif choice == "2":
            print("📦 Procesando archivos CSV...")
            processing_csv(download_folder, data_access)
            break
        elif choice == "3":
            print("💰 Procesando ingresos...")
            # Aquí puedes llamar a la función correspondiente
            break
        else:
            print("⚠️ Opción no válida. Por favor elige 1, 2 o 3.\n")


if __name__ == "__main__":
    print(message_print('Iniciando el script de administración total'))
    if sys.platform == "darwin":
        folder_root = r"/Users/armjorge/Library/CloudStorage/GoogleDrive-armjorge@gmail.com/My Drive/Projects/administracion_total"
    elif sys.platform.startswith("win"):
        folder_root = r"C:\Users\arman\Documents\habit_starter"
    else:
        raise RuntimeError(f"Unsupported platform: {sys.platform}")
    # 1) Añade al path la carpeta donde está df_multi_match.py
    libs_dir = os.path.join(folder_root, "Librería")
    print(libs_dir)
    sys.path.insert(0, libs_dir)

    # 2) Ahora importa la función directamente
    from chrome_driver_load import load_chrome

    ACTIONS = {
    "https://www.banorte.com/wps/portal/ixe/Home/inicio": [
        {"type": "send_keys", "by": By.XPATH, "locator": '//*[@id="userid"]',    "value": "Abre el YAML y escriibe tu usuario"},
        {"type": "click",     "by": By.XPATH, "locator": '//*[@id="btn_lgn_entrar"]'},
        {"type": "send_keys", "by": By.XPATH, "locator": '//*[@id="passwordLogin"]',    "value": "Abre el YAML y escriibe tu contraseña"},
        {"type": "wait_user", "value": "Por favor ingresa tu token y presiona enter en la terminal"},
        {"type": "click",     "by": By.XPATH, "locator":'//*[@id="btnAceptarloginPasswordAsync"]'}
    ],} 
    # 3) Llama a tu función pasándola como parámetro
    total_management(load_chrome, folder_root, ACTIONS)