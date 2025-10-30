from utils.helpers import Helper
from .downloader_workflow import DownloaderWorkflow
from .sheets_updater import SheetsUpdater
from utils.helpers import Helper  # Import the Helper class
import pandas as pd
import os
import pickle
from datawarehouse.datawarehouse import DataWarehouse
from dotenv import load_dotenv
from Library.csv_to_sql import CSV_TO_SQL

class BankingManager:
    def __init__(self, working_folder, data_access):
        self.working_folder = working_folder
        self.data_access = data_access
        self.csv_to_sql = CSV_TO_SQL(self.working_folder, self.data_access)

        # Pass path_tc_closed and fechas_corte to DownloaderWorkflow
        #self.descargador = DownloaderWorkflow( self.working_folder, self.data_access, self.folder_root, self.path_tc_closed, self.corriente_temporal_downloads, self.fechas_corte, self.today, self.pickle_debito_cerrado, self.pickle_credito_cerrado)

        self.sheets_updater = SheetsUpdater(
            self.working_folder,
            self.data_access,
        )

    def run_banking_menu(self):
        print(Helper.message_print("Bienvenido al menú bancario"))
        
        """Run the banking menu."""
        while True:
            choice = input(f"""{Helper.message_print('¿Qué deseas hacer?')}
        1. Descargar
        2. Cargar a SQL, GoogleSheet
        0. Salir
        Elige una opción: """).strip()

            if choice == "1":
                # Call the downloader workflow
                self.descargador.descargador_workflow()

            elif choice == "2":
                print("\nCargar a SQL\n")
                dict_dataframes = self.csv_to_sql.csv_to_sql_process()      
                print("\nCargar a GoogleSheets\n")
                self.sheets_updater.update_multiple_sheets(dict_dataframes)


            elif choice == "0":
            
                print("👋 ¡Hasta luego!")
                return
            else:
                print("\n⚠️ Elige una opción válida (1 o 0). Inténtalo de nuevo.\n")

