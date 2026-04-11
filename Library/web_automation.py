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
import time, glob
import shutil
import sys
import select
import tty
import termios
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
                # 2. If login worked (or you did it manually), we enter the core logic
                print("✅ Acceso listo. Iniciando el proceso de descarga...")
                self.rename_downloads_guided(final_files)
            else:
                print("❌ El acceso falló.")

        except Exception as e:
            print(f"❌ Error General: {e}")

        finally:
            # 3. ONLY close when everything is done
            if self.driver:
                input("\n🏁 Todo listo. Presiona ENTER para cerrar el navegador...")
                self.driver.quit()

                

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
    def _wait_for_manual_interrupt(self, timeout=3):
            """Espera cualquier tecla por 'timeout' segundos en macOS/Linux."""
            fd = sys.stdin.fileno()
            # Guardamos la configuración actual de la terminal
            old_settings = termios.tcgetattr(fd)
            try:
                # tty.setcbreak permite capturar la tecla sin esperar al ENTER
                tty.setcbreak(fd)
                print(f"Presiona CUALQUIER TECLA para modo MANUAL ({timeout}s): ", end="", flush=True)
                
                start_time = time.time()
                while time.time() - start_time < timeout:
                    # Revisa si hay algo en el buffer de entrada (stdin)
                    if select.select([sys.stdin], [], [], 0.1)[0]:
                        sys.stdin.read(1) # 'Limpiamos' la tecla presionada del buffer
                        return True
                    
                    # Opcional: imprimir puntos para ver progreso
                    # print(".", end="", flush=True)
                    
            except Exception as e:
                print(f"⚠️ Error en detector de teclado: {e}")
            finally:
                # IMPORTANTE: Devolvemos la terminal a su estado original
                termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
                
            return False

    def _execute_navigation(self, actions):
        """
        Challenge solved: If manual mode is triggered, it opens the URL, 
        waits for you, and returns True so the main script immediately 
        calls rename_downloads_guided.
        """
        for url, steps in actions.items():
            # Check for ANY KEY to go manual
            if self._wait_for_manual_interrupt(timeout=3):
                print("\n\n🛠 [MODO MANUAL ACTIVADO]")
                self.driver.get(url) 
                print("👉 Navega/Logueate manualmente en el navegador.")
                input("🚀 Una vez estés en la pantalla de inicio, presiona ENTER aquí para ir a las descargas...")
                
                # This is the key: we return True immediately. 
                # The calling function will then trigger rename_downloads_guided.
                return True 

            # --- Flujo Automático (Si no presionas nada) ---
            print(f"\n🔗 Navegando automáticamente a {url}")
            self.driver.get(url)
            try:
                for idx, step in enumerate(steps, start=1):
                    if not self._execute_step(step, idx):
                        return False
            except Exception as e:
                print(f"❌ Error: {e}")
                return False
                
        return True

    # def _execute_navigation(self, actions):
    #     """Ejecuta la navegación web paso a paso"""
    #     for url, steps in actions.items():
    #         print(f"\n🔗 Navegando a {url}")
    #         self.driver.get(url)
    #         try:
    #             for idx, step in enumerate(steps, start=1):
    #                 success = self._execute_step(step, idx)
    #                 if not success:
    #                     if step["type"] == "call_function":
    #                         print("⚠️ Reintentando la función personalizada...")
    #                         continue  # Reintentar la función personalizada
    #                     else:
    #                         return False
                            
    #         except TimeoutException as e:
    #             print(f"❌ Timeout durante la navegación: {e}")
    #             return False
            
    #     return True
    
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
        print("\n🧹 Limpiando carpeta temporal...")
        # Limpieza inicial (solo una vez al inicio)
        for f in glob.glob(os.path.join(self.temporal_downloads, "*.csv")):
            try:
                os.remove(f)
            except Exception as e:
                print(f"⚠️ No se pudo borrar {f}: {e}")

        # Agrupar por tipo: debit / credit
        grouped = {}
        for item in final_files:
            grouped.setdefault(item["type"], []).append(item)

        for ttype, items in grouped.items():
            print(f"\n📂 Procesando tipo: {ttype.upper()}")
            input(f"➡️ Navega a la sección de {ttype.upper()} en Banorte y presiona Enter...")

            for item in items:
                account = item["account"]
                period  = item["period"]
                status  = item["status"]  # 'open' o 'closed'

                print(f"\n⬇️ Descarga para cuenta {account} ({status.upper()} • {period})")
                print("   👉 Cuando YA descargaste el archivo, presiona Enter…")

                # 1) Tomamos un snapshot de los CSV que ya existen ANTES de descargar
                existing_files = set(glob.glob(os.path.join(self.temporal_downloads, "*.csv")))

                input()  # el usuario descarga aquí

                # 2) Esperar a que aparezca UN archivo nuevo
                detected = None
                for _ in range(60):  # hasta 60 segundos
                    time.sleep(1)
                    current_files = set(glob.glob(os.path.join(self.temporal_downloads, "*.csv")))
                    new_files = current_files - existing_files  # solo los nuevos

                    if new_files:
                        # Si por alguna razón hubiera más de uno, tomamos el más reciente
                        detected = max(new_files, key=os.path.getmtime)
                        break

                if not detected:
                    print("⚠️ No se detectó archivo descargado nuevo. Continuando con el siguiente…")
                    continue

                base = os.path.basename(detected)
                ext  = os.path.splitext(base)[1]

                # 3) Nombre final en la carpeta temporal
                if status == "closed":
                    new_name = f"{period} {account}{ext}"
                else:  # 'open' u otro
                    new_name = f"{account}_{base}"

                new_path = os.path.join(self.temporal_downloads, new_name)

                # Evitar sobrescritura en carpeta temporal
                counter = 1
                while os.path.exists(new_path):
                    name_noext, _ = os.path.splitext(new_name)
                    new_name = f"{name_noext}_{counter}{ext}"
                    new_path = os.path.join(self.temporal_downloads, new_name)
                    counter += 1

                try:
                    os.rename(detected, new_path)
                    print(f"✅ Archivo renombrado como: {new_name}")
                except Exception as e:
                    print(f"⚠️ No se pudo renombrar {base}: {e}")
                    continue

                # 4) Mover según status (open / closed)
                if status == "open":
                    target_root = self.current_folder   # asegúrate de definir esto en __init__
                else:  # 'closed'
                    target_root = self.closed_folder    # idem

                os.makedirs(target_root, exist_ok=True)

                target_path = os.path.join(target_root, new_name)

                # Evitar sobrescritura en la carpeta destino también
                move_counter = 1
                final_target = target_path
                name_noext, ext2 = os.path.splitext(new_name)
                while os.path.exists(final_target):
                    final_target = os.path.join(
                        target_root,
                        f"{name_noext}_{move_counter}{ext2}"
                    )
                    move_counter += 1

                try:
                    shutil.move(new_path, final_target)
                    print(f"📁 Movido a: {final_target}")
                except Exception as e:
                    print(f"⚠️ No se pudo mover {new_path} a {final_target}: {e}")

        print("\n🏁 Todos los archivos descargados, renombrados y movidos correctamente.")
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