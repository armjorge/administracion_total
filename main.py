import os
from banking.banking_manager_workflow import BankingManager
#from business.business_manager import BusinessManager
from datetime import date
import yaml
from datawarehouse.datawarehouse import DataWarehouse
from utils.helpers import Helper    
from sqlalchemy import create_engine, text
from datawarehouse.ETL import ETL
from datawarehouse.conceptos import Conceptos  # Asegúrate de importar la clase ETL
from dotenv import load_dotenv


class TotalManagementApp:
    def run(self):
        """Run the main application menu."""
        self.initialize()
        while True:
            choice = input(
                "Elige: \n\t1) para la información bancaria  o \n\t2) para el módulo de gastos y presupuestos\n\t3) ETL en SQL \n\t4) Completar conceptos \n\t5) Ejecutar SQLs\n\t0) para salir\n"
            ).strip()
            if choice == "1":
                print(self.helper.message_print("\n🚀 Iniciando la generación de información bancaria para su posterior minería..."))                     
                self.banking_manager.run_banking_menu()
            elif choice == "2":
                print(self.helper.message_print("\n🚀 Iniciando la generación de partidas presupuestarias..."))                
                self.business_manager.run_business_menu()
            elif choice == "3":
                print(self.helper.message_print("\n🚀 Iniciando el proceso ETL para la inteligencia financiera..."))
                #self.datawarehouse.etl_process()
                self.ETL_alternativo.main()
            elif choice == "4": 
                print(self.helper.message_print("\n🚀 Iniciando la actualización de conceptos..."))
                self.Conceptos.generador_de_conceptos()
            elif choice == "5":
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
        self.folder_root = self.get_root_path()
        self.working_folder = os.path.join(self.folder_root, "Info Bancaria")  # Correct path
        passwords_path = os.path.join(self.folder_root, 'config.yaml')
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
        self.strategy_folder = os.path.join(self.folder_root, "Estrategia")
        self.datawarehouse = DataWarehouse(self.strategy_folder, self.data_access)
        self.helper = Helper()
        self.queries_folder = os.path.join(self.folder_root, 'queries')
        self.ETL_alternativo = ETL(self.folder_root)
        self.Conceptos = Conceptos(self.strategy_folder, self.data_access)

    def get_root_path(self):
        # Get the directory where main.py lives (repo folder)
        repo_path = os.path.dirname(os.path.abspath(__file__))
        repo_name = os.path.basename(repo_path)
        print(f"Current script path: {os.path.abspath(__file__)}")
        env_file = ".env"
        # Load .env if it exists
        full_repo_path: Optional[str] = None
        if os.path.exists(env_file):
            load_dotenv(env_file)
            full_repo_path = os.getenv("MAIN_PATH") or os.getenv("Main_path")
            if not full_repo_path:
                with open(env_file, "r") as env_handle:
                    for line in env_handle:
                        stripped = line.strip()
                        if not stripped or stripped.startswith("#"):
                            continue
                        if stripped.lower().startswith("main_path"):
                            if ":" in stripped:
                                _, value = stripped.split(":", 1)
                            elif "=" in stripped:
                                _, value = stripped.split("=", 1)
                            else:
                                value = ""
                            full_repo_path = value.strip()
                            break
        if not full_repo_path:
            full_repo_path = "root"

        # If root or invalid, ask the user
        if not full_repo_path or full_repo_path == "root" or not os.path.exists(full_repo_path):
            while True:
                user_input = input("⚠️  .env not found. Please enter a valid path for working files: ").strip()
                if os.path.exists(user_input):
                    # Save .env file with path using repo folder name
                    candidate_path = os.path.join(user_input, repo_name)
                    os.makedirs(candidate_path, exist_ok=True)
                    full_repo_path = candidate_path
                    with open(env_file, "w") as f:
                        f.write(f"Main_path: {full_repo_path}\n")
                    print(f"✅ Path saved to {env_file}: {full_repo_path}")
                    break
                else:
                    print("❌ Invalid path. Try again.")

        return full_repo_path


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
        
        #self.business_manager = BusinessManager(self.folder_root)


if __name__ == "__main__":
    app = TotalManagementApp()
    app.run()

