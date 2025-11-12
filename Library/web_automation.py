import os
import platform
from selenium import webdriver
from selenium.common.exceptions import (
    SessionNotCreatedException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from datetime import date

try:
    from Library.helpers import Helper
except ModuleNotFoundError:
    # fallback if running inside the Library folder
    from helpers import Helper

class WebAutomation:
    def __init__(self, working_folder, data_access):
        self.driver = None
        self.timeout = 20
        self.helper = Helper()        
        self.today = date.today()
        self.working_folder = working_folder
        self.data_access = data_access
        self.current_folder = os.path.join(self.working_folder,'Info Bancaria', f'{self.today.year}-{self.today.month:02d}')
        self.closed_folder = os.path.join(self.working_folder,'Info Bancaria', 'Meses cerrados', 'Repositorio por mes')
        self.temporal_downloads = os.path.join(self.working_folder, 'Info Bancaria', 'Descargas temporales')
        
    def chrome_driver_load(self, directory):
        """Launch Chrome with OS-specific paths and consistent configuration."""

        system = platform.system()
        home = os.path.expanduser("~")

        if system == "Windows":
            chrome_binary_path = os.path.join(home, "Documents", "chrome-win64", "chrome.exe")
            chromedriver_path = os.path.join(home, "Documents", "chromedriver-win64", "chromedriver.exe")
        elif system == "Darwin":
            machine = platform.machine().lower()
            arch_suffix = "arm64" if "arm" in machine else "x64"
            chrome_binary_path = os.path.join(
                home,
                "chrome_testing",
                f"chrome-mac-{arch_suffix}",
                "Google Chrome for Testing.app",
                "Contents",
                "MacOS",
                "Google Chrome for Testing",
            )
            chromedriver_path = os.path.join(
                home,
                "chrome_testing",
                f"chromedriver-mac-{arch_suffix}",
                "chromedriver",
            )
        else:
            print(f"❌ Unsupported OS: {system}")
            return None

        if not os.path.exists(chrome_binary_path) or not os.path.exists(chromedriver_path):
            print("⚠️ Chrome o Chromedriver no encontrados. Iniciando instalación guiada...")
            chrome_binary_path, chromedriver_path = self.helper.install_chromedriver()

        if not chrome_binary_path or not chromedriver_path:
            print("❌ No se obtuvieron rutas válidas para Chrome.")
            return None

        if not os.path.exists(chrome_binary_path) or not os.path.exists(chromedriver_path):
            print("❌ Las rutas configuradas para Chrome/Chromedriver no existen.")
            print(f"   Chrome: {chrome_binary_path}")
            print(f"   Chromedriver: {chromedriver_path}")
            return None

        download_dir = os.path.abspath(directory)
        os.makedirs(download_dir, exist_ok=True)

        print(f"Usando Chrome binario: {chrome_binary_path}")
        print(f"Usando Chromedriver: {chromedriver_path}")

        chrome_options = Options()
        chrome_options.binary_location = chrome_binary_path

        prefs = {
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "plugins.always_open_pdf_externally": True,
        }
        chrome_options.add_experimental_option("prefs", prefs)
        chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])

        chrome_options.add_argument("--disable-background-networking")
        chrome_options.add_argument("--disable-client-side-phishing-detection")
        chrome_options.add_argument("--disable-component-update")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_argument("--window-size=1920x1080")
        chrome_options.add_argument("--remote-allow-origins=*")

        if system == "Linux":
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--no-sandbox")
        elif system == "Windows":
            chrome_options.add_argument("--disable-gpu")

        try:
            service = Service(chromedriver_path)
            driver = webdriver.Chrome(service=service, options=chrome_options)
            print("🚀 ChromeDriver inicializado correctamente.")
            return driver
        except SessionNotCreatedException as exc:
            print("❌ ChromeDriver no pudo crear la sesión")
            print(exc)
        except WebDriverException as exc:
            print("❌ Error de WebDriver al iniciar ChromeDriver")
            print(exc)
        except Exception as exc:
            print("❌ Error inesperado al iniciar ChromeDriver")
            print(exc)

        return None
    
    def execute_download_session(self, final_files):
        print("Iniciando sesión de descarga web...")
        print(final_files)
        try:
            self.driver = self.chrome_driver_load(self.temporal_downloads)
            if not self.driver:
                print("❌ No se pudo iniciar ChromeDriver. Revisa los mensajes anteriores.")
                return False

            actions = self._build_actions(self.data_access)
            success = self._execute_navigation(actions)

            if success:
                print("✅ Navegación completada con éxito. Procediendo a la descarga manual guiada...")
                
                # 🧭 Iniciar guía manual fuera del flujo de navegación
                self.rename_downloads_guided(final_files)

                print("🏁 Todas las descargas y renombrados completados correctamente.")
            else:
                print("❌ No se pudieron procesar las acciones de login.")
                return False

        except Exception as e:
            print(f"❌ Error durante la automatización: {e}")
            return False

        finally:
            if self.driver:
                input(Helper.message_print("Presiona enter para cerrar el navegador"))
                self.driver.quit()
                
    def file_routing(self, download_folder, archivos_faltantes, periodo):
        print(f"Buscando archivos en el directorio: {download_folder}")

        """Gestiona la ruta de los archivos descargados"""
        expected_headers_credito = self.data_access['BANORTE_credit_headers']
        expected_headers_debito = self.data_access['BANORTE_debit_headers']

        # Determinar los headers descargados según los patrones en archivos_faltantes
        headers_descargados = []
        if 'credito_corriente' in archivos_faltantes or 'credito_cerrado' in archivos_faltantes:
            headers_descargados.append(expected_headers_credito)
        if 'debito_corriente' in archivos_faltantes or 'debito_cerrado' in archivos_faltantes:
            headers_descargados.append(expected_headers_debito)

        # Imprimir los headers seleccionados
        print("\nHeaders seleccionados dinámicamente:")
        for header in headers_descargados:
            print(f" - {header}")

        paths_destino = {}
        print("Archivos faltantes", archivos_faltantes, "\n")

        # Generar rutas dinámicas para los archivos faltantes
        if 'debito_corriente' in archivos_faltantes:
            suffix = '_debito.csv'
            partial_path = self.helper.archivo_corriente_reciente(self.today, suffix, 'corriente')
            paths_destino['debito_corriente'] = partial_path
        if 'credito_corriente' in archivos_faltantes:
            suffix = '_credito.csv'
            partial_path = self.helper.archivo_corriente_reciente(self.today, suffix, 'corriente')
            paths_destino['credito_corriente'] = partial_path
        if 'debito_cerrado' in archivos_faltantes:
            suffix = '_debito.csv'
            partial_path = self.helper.archivo_corriente_reciente(periodo, suffix, 'cerrado')
            paths_destino['debito_cerrado'] = partial_path
        if 'credito_cerrado' in archivos_faltantes:
            suffix = '_credito.csv'
            partial_path = self.helper.archivo_corriente_reciente(periodo, suffix, 'cerrado')
            paths_destino['credito_cerrado'] = partial_path

        # Loop para reintentar hasta que los archivos sean encontrados
        max_retries = 5
        retries = 0
        while retries < max_retries:
            print(f"🔄 Intento {retries + 1} de {max_retries} para encontrar archivos...")
            csv_files = self.helper.get_files_in_directory(download_folder)  # Asegúrate de usar download_folder aquí
            print(f"Archivos encontrados: {csv_files}")

            # Filtrar archivos por headers
            csv_files_credit = [f for f in csv_files if self.helper.get_file_headers(os.path.join(download_folder, f)) == expected_headers_credito]
            csv_files_debito = [f for f in csv_files if self.helper.get_file_headers(os.path.join(download_folder, f)) == expected_headers_debito]

            print(f"Archivos de crédito encontrados: {csv_files_credit}")
            print(f"Archivos de débito encontrados: {csv_files_debito}")

            if csv_files_credit or csv_files_debito:
                break  # Salir del loop si se encuentran archivos
            else:
                print("⚠️ No se encontraron archivos válidos. Esperando antes de reintentar...")
                retries += 1
                import time
                input("\nPresiona Enter para continuar buscar de nuevo...\n")

        if retries == max_retries:
            print("❌ No se encontraron archivos después de varios intentos.")
            return False

        # Procesar archivos encontrados
        for key, path in paths_destino.items():
            if key == 'debito_corriente' and csv_files_debito:
                print(f"🔄 Fusionando archivos de débito para {key}...")
                merged_file = self.helper.merge_files([os.path.join(download_folder, f) for f in csv_files_debito])
                if merged_file:
                    destination_path = os.path.join(self.working_folder, path)
                    self.helper.move_file(merged_file, destination_path)
                    print(f"✅ Archivo fusionado de débito movido a: {destination_path}")
                    for file in csv_files_debito:
                        file_path = os.path.join(download_folder, file)
                        if os.path.exists(file_path):  # Verificar si el archivo aún existe
                            os.remove(file_path)
                            print(f"🗑️ Archivo eliminado: {file}")
                        else:
                            print(f"⚠️ Archivo ya no existe y no se puede eliminar: {file}")

            elif key == 'credito_corriente' and csv_files_credit:
                print(f"🔄 Fusionando archivos de crédito para {key}...")
                merged_file = self.helper.merge_files([os.path.join(download_folder, f) for f in csv_files_credit])
                if merged_file:
                    destination_path = os.path.join(self.working_folder, path)
                    self.helper.move_file(merged_file, destination_path)
                    print(f"✅ Archivo fusionado de crédito movido a: {destination_path}")
                    for file in csv_files_credit:
                        file_path = os.path.join(download_folder, file)
                        if os.path.exists(file_path):  # Verificar si el archivo aún existe
                            os.remove(file_path)
                            print(f"🗑️ Archivo eliminado: {file}")
                        else:
                            print(f"⚠️ Archivo ya no existe y no se puede eliminar: {file}")
            elif key == 'debito_cerrado' and csv_files_debito:
                print(f"🔄 Fusionando archivos de débito para {key}...")
                merged_file = self.helper.merge_files([os.path.join(download_folder, f) for f in csv_files_debito])
                if merged_file:
                    destination_path = os.path.join(self.path_tc_closed, "Repositorio por mes")
                    self.helper.create_directory_if_not_exists(destination_path)
                    destination_file_path = os.path.join(destination_path, path)
                    self.helper.move_file(merged_file, destination_file_path)
                    print(f"✅ Archivo fusionado de débito movido a: {destination_file_path}")
                    for file in csv_files_debito:
                        file_path = os.path.join(download_folder, file)
                        if os.path.exists(file_path):  # Verificar si el archivo aún existe
                            os.remove(file_path)
                            print(f"🗑️ Archivo eliminado: {file}")
                        else:
                            print(f"⚠️ Archivo ya no existe y no se puede eliminar: {file}")
            elif key == 'credito_cerrado' and csv_files_credit:
                print(f"🔄 Fusionando archivos de crédito para {key}...")
                merged_file = self.helper.merge_files([os.path.join(download_folder, f) for f in csv_files_credit])
                if merged_file:
                    destination_path = os.path.join(self.path_tc_closed, "Repositorio por mes")
                    self.helper.create_directory_if_not_exists(destination_path)
                    destination_file_path = os.path.join(destination_path, path)
                    self.helper.move_file(merged_file, destination_file_path)
                    print(f"✅ Archivo fusionado de crédito movido a: {destination_file_path}")
                    for file in csv_files_credit:
                        file_path = os.path.join(download_folder, file)
                        if os.path.exists(file_path):  # Verificar si el archivo aún existe
                            os.remove(file_path)
                            print(f"🗑️ Archivo eliminado: {file}")
                        else:
                            print(f"⚠️ Archivo ya no existe y no se puede eliminar: {file}")
        return True

    def _build_actions(self, data_access):
        """Construye las acciones con las credenciales del usuario"""
        actions = {
            "https://www.banorte.com/wps/portal/ixe/Home/inicio": [
                {
                    "type": "send_keys",
                    "by": By.XPATH,
                    "locator": '//*[@id="userid"]',
                    "value": data_access.get("BANORTE_user", "")
                },
                {
                    "type": "click",
                    "by": By.XPATH,
                    "locator": '//*[@id="btn_lgn_entrar"]'
                },
                {
                    "type": "send_keys",
                    "by": By.XPATH,
                    "locator": '//*[@id="passwordLogin"]',
                    "value": data_access.get("BANORTE_password", "")
                },
                {
                    "type": "wait_user",
                    "value": "Por favor ingresa tu token y presiona enter en la terminal"
                },
                {
                    "type": "click",
                    "by": By.XPATH,
                    "locator": '//*[@id="btnAceptarloginPasswordAsync"]'
                }
            ]
        }
        return actions    
    def _execute_navigation(self, actions):
        """Ejecuta la navegación web paso a paso"""
        for url, steps in actions.items():
            print(f"\n🔗 Navegando a {url}")
            self.driver.get(url)
            try:
                for idx, step in enumerate(steps, start=1):
                    success = self._execute_step(step, idx)
                    if not success:
                        if step["type"] == "call_function":
                            print("⚠️ Reintentando la función personalizada...")
                            continue  # Reintentar la función personalizada
                        else:
                            return False
                            
            except TimeoutException as e:
                print(f"❌ Timeout durante la navegación: {e}")
                return False
            
        return True
    
    def _execute_step(self, step, step_number):
        """Ejecuta un paso individual de la automatización"""
        step_type = step["type"]
        print(f"  → Paso {step_number}: {step_type}")
        
        if step_type == "wait_user":
            msg = step.get("value", "Presiona enter para continuar...")
            print(f"\n    ⏸ {msg}")
            input()
            return True
        # Paso para llamar a la función. 
        elif step_type == "call_function":
            # Llamar a una función personalizada
            function = step.get("function")
            args = step.get("args", [])
            kwargs = step.get("kwargs", {})
            print(f"  → Llamando a la función: {function.__name__}")
            try:
                result = function(*args, **kwargs)
                if result:
                    print(f"    ✓ Función {function.__name__} ejecutada con éxito.")
                    return True
                else:
                    print(f"    ⚠️ Función {function.__name__} no completada. Reintentando...")
                    return False
            except Exception as e:
                print(f"    ❌ Error al ejecutar la función {function.__name__}: {e}")
                return False
        # Operación en la web

        try:
            # Localizar elemento
            by = step["by"]
            locator = step["locator"]
            
            element = WebDriverWait(self.driver, self.timeout).until(
                EC.element_to_be_clickable((by, locator))
            )
            
            # Ejecutar acción
            if step_type == "click":
                element.click()
                print(f"    ✓ Click ejecutado en {locator}")
                
            elif step_type == "send_keys":
                element.click()
                element.clear()
                element.send_keys(step["value"])
                print(f"    ✓ Texto enviado a {locator}")
                
            else:
                print(f"    ⚠️ Tipo de paso desconocido: {step_type}")
                return False
            
            return True
            
        except TimeoutException:
            print(f"    ❌ Timeout en paso {step_number}: {locator}")
            return False
        except Exception as e:
            print(f"    ❌ Error en paso {step_number}: {e}")
            return False

    def rename_downloads_guided(self, final_files):
        """Guía al usuario para descargar y renombrar archivos"""
        import time, glob
        print("\n🧹 Limpiando carpeta temporal...")
        for f in glob.glob(os.path.join(self.temporal_downloads, "*.csv")):
            try:
                os.remove(f)
            except Exception:
                pass

        # Agrupar por cuenta
        grouped = {}
        for item in final_files:
            grouped.setdefault(item["account"], []).append(item)

        for account, items in grouped.items():
            print(f"\n⚙️ Procesando cuenta {account}")
            input(f"➡️ Navega a la sección de la cuenta {account} y presiona Enter para continuar...")

            for item in items:
                status = item.get("status")
                period = item.get("period")
                print(f"\n⬇️ Descarga el archivo ({status.upper()} - {period}) y presiona Enter cuando termine...")
                before = set(glob.glob(os.path.join(self.temporal_downloads, "*.csv")))
                input()
                detected = None
                for _ in range(30):
                    time.sleep(1)
                    after = set(glob.glob(os.path.join(self.temporal_downloads, "*.csv")))
                    new_files = after - before
                    if new_files:
                        detected = list(new_files)[0]
                        break
                if not detected:
                    print("⚠️ No se detectó ningún archivo nuevo.")
                    continue
                base = os.path.basename(detected)
                ext = os.path.splitext(base)[1]
                if status == "closed":
                    new_name = f"{period} {account}_{base}"
                else:
                    new_name = f"{account}_{base}"
                try:
                    os.rename(detected, os.path.join(self.temporal_downloads, new_name))
                    print(f"✅ Archivo renombrado: {new_name}")
                except Exception as e:
                    print(f"⚠️ No se pudo renombrar el archivo {base}: {e}")

        print("🏁 Descargas completadas y renombradas.")
        return True


if __name__ == "__main__":
    # 1️⃣ Obtiene la ruta absoluta al archivo .env (un nivel arriba del archivo actual)
    env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '.env'))
    dot_env_name = "MAIN_PATH"

    # 2️⃣ Carga variables del .env si existe
    if os.path.exists(env_path):
        load_dotenv(dotenv_path=env_path)
        # 3️⃣ Obtiene la variable MAIN_PATH
        working_folder = os.getenv(dot_env_name)
        if not working_folder:
            raise ValueError(f"La variable {dot_env_name} no está definida en {env_path}")
        # 4️⃣ Construye la ruta absoluta hacia config.yaml dentro del MAIN_PATH
        yaml_path = os.path.join(working_folder, 'config.yaml')
        if not os.path.exists(yaml_path):
            raise FileNotFoundError(f"No se encontró config.yaml en {yaml_path}")
        # 5️⃣ Carga el archivo YAML
        with open(yaml_path, 'r') as file:
            data_access = yaml.safe_load(file)
        # 6️⃣ Ejecuta la aplicación principal
        app = DownloaderWorkflow(working_folder, data_access)
        app.download_missing_files()

    else:
        raise FileNotFoundError(f"No se encontró el archivo .env en {env_path}")