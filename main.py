import os
from config.config_manager import ConfigManager
from banking.banking_manager import BankingManager
from business.business_manager import BusinessManager
from utils.helpers import message_print

class TotalManagementApp:
    def __init__(self):
        self.folder_root = os.getcwd()
        self.config_manager = ConfigManager(self.folder_root)
        self.banking_manager = None
        self.business_manager = None
    
    def initialize(self):
        """Inicializa los managers principales"""
        working_folder = os.path.join(self.folder_root, "Implementación", "Info Bancaria")
        
        # Inicializar configuración
        data_access = self.config_manager.load_or_create_yaml(working_folder)
        
        # Inicializar managers
        self.banking_manager = BankingManager(working_folder, data_access, self.folder_root)
        self.business_manager = BusinessManager(self.folder_root)
    
    def run(self):
        """Ejecuta el menú principal de la aplicación"""
        self.initialize()
        
        while True:
            choice = input(message_print(
                "Elige: \n\t1) para la información bancaria  o \n\t2) para el módulo de gastos y presupuestos\n\t 0 para salir"
            )).strip()

            if choice == "1":
                self.banking_manager.run_banking_menu()
                
            elif choice == "2":
                self.business_manager.run_business_menu()
                
            elif choice == "0":
                print("👋 ¡Hasta luego!")
                break                
            else:
                print("\n⚠️ Elige una opción válida (1 o 2). Inténtalo de nuevo.\n")

if __name__ == "__main__":
    app = TotalManagementApp()
    app.run()