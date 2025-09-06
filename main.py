import os
from banking.banking_manager_workflow import BankingManager
from business.business_manager import BusinessManager
from datetime import date
import yaml
from datawarehouse.datawarehouse import DataWarehouse
from utils.helpers import Helper    
from sqlalchemy import create_engine, text

class TotalManagementApp:
    def run(self):
        """Run the main application menu."""
        self.initialize()
        while True:
            choice = input(
                "Elige: \n\t1) para la información bancaria  o \n\t2) para el módulo de gastos y presupuestos\n\t3 )Inteligencia \n\t4) Ejecutar SQLs\n\t0) para salir\n"
            ).strip()
            if choice == "1":
                print(self.helper.message_print("\n🚀 Iniciando la generación de información bancaria para su posterior minería..."))                     
                self.banking_manager.run_banking_menu()
            elif choice == "2":
                print(self.helper.message_print("\n🚀 Iniciando la generación de partidas presupuestarias..."))                
                self.business_manager.run_business_menu()
            elif choice == "3":
                print(self.helper.message_print("\n🚀 Iniciando el proceso ETL para la inteligencia financiera..."))
                self.datawarehouse.etl_process()
            elif choice == "4":
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
                        print(f"Encontramos consulta desde archivo {file}.")
                except Exception as e:
                    print(f"❌ Error conectando a la fuente: {e}")
                    return                   


            elif choice == "0":
                print("👋 ¡Hasta luego!")
                break
            else:
                print("\n⚠️ Elige una opción válida (1, 2, o 0). Inténtalo de nuevo.\n")

    def __init__(self):
        self.folder_root = os.getcwd()
        self.working_folder = os.path.join(self.folder_root,"Implementación", "Info Bancaria")  # Correct path
        passwords_path = os.path.join(self.working_folder, 'passwords.yaml')
        with open(passwords_path, 'r') as f:
            self.data_access = yaml.safe_load(f)
        # Define paths
        self.path_tc_closed = os.path.join(self.working_folder, 'Meses cerrados')
        self.pickle_debito_corriente = os.path.join(self.path_tc_closed, 'debit_current.pkl')
        self.pickle_credito_corriente = os.path.join(self.path_tc_closed, 'credit_current.pkl')
        self.pickle_credit_closed = os.path.join(self.path_tc_closed, 'pickle_credit_closed.pkl')
        self.pickle_debit_closed = os.path.join(self.path_tc_closed, 'pickle_debit_closed.pkl')
        self.corriente_temporal_downloads = os.path.join(self.working_folder, "Temporal Downloads")
        self.fechas_corte = os.path.join(self.path_tc_closed, "df_fechas_corte_2025.pkl")
        self.TODAY = date.today()
        self.banking_manager = None
        self.business_manager = None
        self.reporting_folder = os.path.join(self.folder_root, "Implementación", "Estrategia")
        self.datawarehouse = DataWarehouse(self.reporting_folder, self.data_access)
        self.helper = Helper()
        self.queries_folder = os.path.join(self.folder_root, 'queries')

    def initialize(self):
        """Initialize the managers."""
        self.banking_manager = BankingManager(
            self.working_folder,
            self.data_access,
            self.folder_root,
            self.path_tc_closed,
            self.corriente_temporal_downloads,
            self.fechas_corte,
            self.TODAY,
            self.pickle_debito_corriente,
            self.pickle_credito_corriente,
            self.pickle_debit_closed,
            self.pickle_credit_closed
        )
        
        self.business_manager = BusinessManager(self.folder_root)



if __name__ == "__main__":
    app = TotalManagementApp()
    app.run()

