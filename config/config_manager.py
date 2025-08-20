import os
import yaml
from utils.helpers import message_print, create_directory_if_not_exists, add_to_gitignore

class ConfigManager:
    def __init__(self, folder_root):
        self.folder_root = folder_root
        self.actions = None
        self._setup_actions()
    
    def _setup_actions(self):
        """Define las acciones de automatización web"""
        from selenium.webdriver.common.by import By
        
        self.actions = {
            "https://www.banorte.com/wps/portal/ixe/Home/inicio": [
                {"type": "send_keys", "by": By.XPATH, "locator": '//*[@id="userid"]', "value": ""},
                {"type": "click", "by": By.XPATH, "locator": '//*[@id="btn_lgn_entrar"]'},
                {"type": "send_keys", "by": By.XPATH, "locator": '//*[@id="passwordLogin"]', "value": ""},
                {"type": "wait_user", "value": "Por favor ingresa tu token y presiona enter en la terminal"},
                {"type": "click", "by": By.XPATH, "locator": '//*[@id="btnAceptarloginPasswordAsync"]'}
            ]
        }
    
    def load_or_create_yaml(self, working_folder):
        """Carga o crea el archivo YAML de configuración"""
        create_directory_if_not_exists(working_folder)
        add_to_gitignore(self.folder_root, working_folder)
        
        output_yaml = os.path.join(working_folder, "passwords.yaml")
        
        if os.path.exists(output_yaml):
            with open(output_yaml, 'r', encoding='utf-8') as f:
                data_access = yaml.safe_load(f)
            print(f"Archivo cargado correctamente: {os.path.basename(output_yaml)}")
        else:
            data_access = self._create_yaml_template(output_yaml)
        
        # Actualizar credenciales en actions
        self._update_actions_credentials(data_access)
        return data_access
    
    def _create_yaml_template(self, output_yaml):
        """Crea un archivo YAML template"""
        print(message_print("No se localizó un yaml válido, vamos a crear uno"))
        platforms = ["BANORTE"]
        fields = ["url", "user", "password", "month_free_headers", "credit_headers", "debit_headers"]
        
        lines = []
        for platform in platforms:
            for field in fields:
                lines.append(f"{platform}_{field}: ")
            lines.append("")
        
        with open(output_yaml, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        
        return {}
    
    def _update_actions_credentials(self, data_access):
        """Actualiza las credenciales en las acciones"""
        if data_access and "BANORTE_user" in data_access:
            self.actions["https://www.banorte.com/wps/portal/ixe/Home/inicio"][0]["value"] = data_access["BANORTE_user"]
            self.actions["https://www.banorte.com/wps/portal/ixe/Home/inicio"][2]["value"] = data_access["BANORTE_password"]