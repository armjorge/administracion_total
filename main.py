import os
from banking.banking_manager_workflow import BankingManager
from business.business_manager import BusinessManager
from datetime import date
import yaml

# Define global variables
global PATH_TC_CLOSED, PICKLE_DEBITO_CORRIENTE, PICKLE_CREDITO_CORRIENTE, PICKLE_CREDIT_CLOSED, PICKLE_DEBIT_CLOSED, DOWNLOAD_FOLDER, FECHAS_CORTE

class TotalManagementApp:
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
        self.download_folder = os.path.join(self.working_folder, "Temporal Downloads")
        self.fechas_corte = os.path.join(self.path_tc_closed, "df_fechas_corte_2025.pkl")

        # Set global variables
        global PATH_TC_CLOSED, PICKLE_DEBITO_CORRIENTE, PICKLE_CREDITO_CORRIENTE, PICKLE_CREDIT_CLOSED, PICKLE_DEBIT_CLOSED, DOWNLOAD_FOLDER, FECHAS_CORTE
        PATH_TC_CLOSED = self.path_tc_closed
        PICKLE_DEBITO_CORRIENTE = self.pickle_debito_corriente
        PICKLE_CREDITO_CORRIENTE = self.pickle_credito_corriente
        PICKLE_CREDIT_CLOSED = self.pickle_credit_closed
        PICKLE_DEBIT_CLOSED = self.pickle_debit_closed
        DOWNLOAD_FOLDER = self.download_folder
        FECHAS_CORTE = self.fechas_corte
        self.TODAY = date.today()
        self.banking_manager = None
        self.business_manager = None

    def initialize(self):
        """Initialize the managers."""
        self.banking_manager = BankingManager(
            self.working_folder,
            self.data_access,
            self.folder_root,
            PATH_TC_CLOSED,
            FECHAS_CORTE,
            self.TODAY,
            PICKLE_DEBITO_CORRIENTE,
            PICKLE_CREDITO_CORRIENTE,
            PICKLE_DEBIT_CLOSED,
            PICKLE_CREDIT_CLOSED
            
        )
        
        self.business_manager = BusinessManager(self.folder_root)

    def run(self):
        """Run the main application menu."""
        self.initialize()

        while True:
            choice = input(
                "Elige: \n\t1) para la información bancaria  o \n\t2) para el módulo de gastos y presupuestos\n\t0) para salir\n"
            ).strip()

            if choice == "1":
                self.banking_manager.run_banking_menu()
            elif choice == "2":
                self.business_manager.run_business_menu()
            elif choice == "0":
                print("👋 ¡Hasta luego!")
                break
            else:
                print("\n⚠️ Elige una opción válida (1, 2, o 0). Inténtalo de nuevo.\n")

if __name__ == "__main__":
    app = TotalManagementApp()
    app.run()