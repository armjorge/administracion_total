from .sheets_updater import SheetsUpdater
from Library.csv_to_sql import CSV_TO_SQL
from Library.downloader_workflow import DownloaderWorkflow

class BankingManager:
    def __init__(self, working_folder, data_access):
        self.working_folder = working_folder
        self.data_access = data_access
        self.csv_to_sql = CSV_TO_SQL(self.working_folder, self.data_access)
        self.downloader_workflow = DownloaderWorkflow(self.working_folder, self.data_access)

    def run_banking_menu(self):
        print("Bienvenido al menú bancario")
        
        """Run the banking menu."""
        while True:
            choice = input(f"""'¿Qué deseas hacer?'
        1. Descargar
        2. Cargar a SQL, GoogleSheet
        0. Salir
        Elige una opción: """).strip()

            if choice == "1":
                # Call the downloader workflow
                self.downloader_workflow.download_missing_files()

            elif choice == "2":
                print("\nCargar a SQL\n")
                dict_dataframes = self.csv_to_sql.csv_to_sql_process()      
                print("\nCargar a GoogleSheets\n")

                self.sheets_updater = SheetsUpdater(
                    self.working_folder,
                    self.data_access,
                )                
                self.sheets_updater.update_multiple_sheets(dict_dataframes)


            elif choice == "0":
            
                print("👋 ¡Hasta luego!")
                return
            else:
                print("\n⚠️ Elige una opción válida (1 o 0). Inténtalo de nuevo.\n")

