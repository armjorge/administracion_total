import os
from banking.banking_manager_workflow import BankingManager
from datetime import date
import yaml
from datawarehouse.datawarehouse import DataWarehouse
from utils.helpers import Helper    
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from utils.helpers import Helper

class TotalManagementApp:
    def run(self):
        """Run the main application menu."""
        while True:
            choice = input(
                "Elige: \n\t1) para la información bancaria  o \n\t2) para el módulo de gastos y presupuestos\n\t\t3) Ejecutar SQLs\n\t0) para salir\n"
            ).strip()
            if choice == "1":
                print(self.helper.message_print("\n🚀 Iniciando la generación de información bancaria para su posterior minería..."))                     
                self.banking_manager.run_banking_menu()
            elif choice == "2":
                print(self.helper.message_print("\n🚀 Iniciando la generación de partidas presupuestarias..."))                
                self.business_manager.run_business_menu()

            elif choice == "3":
                print(self.helper.message_print("\n🚀 Ejecutando queries"))
                source_url = self.data_access['sql_url']
                try:
                    src_engine = create_engine(source_url, pool_pre_ping=True)
                    with src_engine.connect() as conn:
                        conn.execute(text("SELECT 1"))
                    print("✅ Conexión a la fuente exitosa.")
                    sql_files = [f for f in os.listdir(self.queries_folder) if f.endswith('.sql')]
                    for file in sql_files:
                        self.datawarehouse.print_query_results(src_engine, file)
                        #print(f"Encontramos consulta desde archivo {file}.")
                except Exception as e:
                    print(f"❌ Error conectando a la fuente: {e}")
                    return                   


            elif choice == "0":
                print("👋 ¡Hasta luego!")
                break
            else:
                print("\n⚠️ Elige una opción válida (1, 2, o 0). Inténtalo de nuevo.\n")

    def __init__(self):
        # Load .env for the root path (same as csv_to_sql.py)
        env_path = os.path.join(os.path.dirname(__file__), '.env')
        with open(env_path, 'r') as file:
            env_data = yaml.safe_load(file)  # Use yaml.safe_load if .env is YAML; otherwise, use dotenv.load_dotenv() and os.getenv()
        
        # Set working_folder as the root (do NOT append 'Info Bancaria' here)
        self.working_folder = env_data['Main_path']  # e.g., '/Users/armjorge/Documents/Repositorios/administracion_total'
        
        # Load config.yaml (same as csv_to_sql.py)
        yaml_path = os.path.join(self.working_folder, 'config.yaml')
        with open(yaml_path, 'r') as file:
            self.data_access = yaml.safe_load(file)
        self.helper = Helper()
        self.banking_manager = BankingManager(self.working_folder, self.data_access)

if __name__ == "__main__":
    app = TotalManagementApp()
    app.run()