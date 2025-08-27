import os
import sys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
#from utils.helpers import message_print
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import platform
import os
from utils.helpers import Helper  # Import the Helper class


class WebAutomation:
    def __init__(self, data_access):
        self.driver = None
        self.timeout = 20
        self.data_access = data_access
        # Cargar driver de Chrome
        
    
    def chrome_driver_load(self, directory):
        """Launch Chrome with OS-specific paths and consistent configuration."""

        # Detect OS
        system = platform.system()
        home = os.path.expanduser("~")
        # Set Chrome binary and ChromeDriver paths based on OS
        if system == "Windows":
            chrome_binary_path = os.path.join(home, "Documents", "chrome-win64", "chrome.exe")
            chromedriver_path = os.path.join(home, "Documents", "chromedriver-win64", "chromedriver.exe")
        elif system == "Darwin":  # macOS
            
            chrome_binary_path = os.path.join(home, "chrome_testing", "chrome-mac-arm64", "Google Chrome for Testing.app", "Contents", "MacOS", "Google Chrome for Testing")
            chromedriver_path = os.path.join(home, "chrome_testing", "chromedriver-mac-arm64", "chromedriver")
        else:
            print(f"Unsupported OS: {system}")
            return None

        # Set Chrome options
        chrome_options = Options()
        chrome_options.binary_location = chrome_binary_path

        prefs = {
            "download.default_directory": os.path.abspath(directory),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "plugins.always_open_pdf_externally": True
        }
        chrome_options.add_experimental_option("prefs", prefs)
        chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
        # (Optional) Further reduce noise:
        chrome_options.add_argument("--disable-background-networking")
        chrome_options.add_argument("--disable-client-side-phishing-detection")
        chrome_options.add_argument("--disable-component-update")

        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920x1080")

        try:
            # Initialize ChromeDriver with the correct service path
            service = Service(chromedriver_path)
            driver = webdriver.Chrome(service=service, options=chrome_options)
            return driver
        except Exception as e:
            print(f"Failed to initialize Chrome driver: {e}")
            return None
    
    def execute_download_session(self, download_folder):
        """Ejecuta una sesión completa de descarga"""
        if not self.chrome_driver_load:
            print("❌ Driver de Chrome no disponible")
            return False
        
        try:
            # Inicializar driver
            self.driver = self.chrome_driver_load(download_folder)
            
            # Configurar acciones según los datos de acceso
            actions = self._build_actions(self.data_access)
            
            # Ejecutar navegación
            success = self._execute_navigation(actions)
            
            return success
            
        except Exception as e:
            print(f"❌ Error durante la automatización: {e}")
            return False
        finally:
            if self.driver:
                input(Helper.message_print("Presiona enter para cerrar el navegador"))
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
    
    def _execute_navigation(self, actions):
        """Ejecuta la navegación web paso a paso"""
        for url, steps in actions.items():
            print(f"\n🔗 Navegando a {url}")
            self.driver.get(url)
            
            try:
                for idx, step in enumerate(steps, start=1):
                    success = self._execute_step(step, idx)
                    if not success:
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