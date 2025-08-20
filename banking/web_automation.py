import os
import sys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from utils.helpers import message_print

class WebAutomation:
    def __init__(self, download_folder, folder_root):
        self.download_folder = download_folder
        self.folder_root = folder_root
        self.driver = None
        self.timeout = 20
        
        # Cargar driver de Chrome
        self._load_chrome_driver()
    
    def _load_chrome_driver(self):
        """Carga el driver de Chrome desde el módulo externo"""
        libs_dir = os.path.join(self.folder_root, "Librería")
        sys.path.insert(0, libs_dir)
        
        try:
            from chrome_driver_load import load_chrome
            self.chrome_driver_load = load_chrome
        except ImportError:
            print("⚠️ No se pudo cargar el driver de Chrome")
            self.chrome_driver_load = None
    
    def execute_download_session(self, data_access, mode="normal"):
        """Ejecuta una sesión completa de descarga"""
        if not self.chrome_driver_load:
            print("❌ Driver de Chrome no disponible")
            return False
        
        try:
            # Inicializar driver
            self.driver = self.chrome_driver_load(self.download_folder)
            
            # Configurar acciones según los datos de acceso
            actions = self._build_actions(data_access)
            
            # Ejecutar navegación
            success = self._execute_navigation(actions)
            
            return success
            
        except Exception as e:
            print(f"❌ Error durante la automatización: {e}")
            return False
        finally:
            if self.driver:
                input(message_print("Presiona enter para cerrar el navegador"))
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