import os
import yaml
from Library.banking_manager_workflow import BankingManager
import subprocess
import sys
import colorama
from Library.initialize import INITIALIZE 


class TotalManagementApp:
    def run(self):
        """Run the main application menu."""
        while True:
            choice = input(
                "Elige: \n\t1) para la información bancaria  o \n\t2) para el módulo de gastos y presupuestos\n\t3) Conceptos \n\t4) Ejecutar SQLs\n\t0) para salir\n"
            ).strip()
            if choice == "1":
                print("\n🚀 Iniciando la ƒgeneración de información bancaria para su posterior minería...")                     
                self.banking_manager.run_banking_menu()
            elif choice == "2":
                print("\n🚀 Iniciando la generación de partidas presupuestarias...")
                self.business_manager.run_business_menu()
            elif choice == "3":
                print("\n🚀 Abriendo clasificador de conceptos Banorte en navegador...")

                streamlit_path = os.path.join('.', "Library", "concept_filing.py")
                try:
                    subprocess.run([sys.executable, "-m", "streamlit", "run", streamlit_path], check=True)
                except Exception as e:
                    print(f"❌ Error al ejecutar Streamlit: {e}")
            elif choice == "4":
                print("\n🚀 Ejecutando queries")
                print("Ejecutar queries")
            elif choice == "0":
                print("👋 ¡Hasta luego!")
                break
            else:
                print("\n⚠️ Elige una opción válida (1, 2, o 0). Inténtalo de nuevo.\n")

    def __init__(self):
        self.root_folder = os.path.dirname(os.path.abspath(__file__))
        self.working_folder = INITIALIZE().initialize(self.root_folder)
        print(f"Working folder set to: {self.working_folder}")
        # Load config.yaml (same as csv_to_sql.py)
        yaml_path = os.path.join(self.working_folder, 'config.yaml')
        with open(yaml_path, 'r') as file:
            self.data_access = yaml.safe_load(file)
        self.banking_manager = BankingManager(self.working_folder, self.data_access)

if __name__ == "__main__":
    app = TotalManagementApp()
    app.run()