from utils.helpers import Helper
from .downloader_workflow import DownloaderWorkflow
from .sql_operations import SQL_CONNEXION_UPDATING
from .sheets_updater import SheetsUpdater
from utils.helpers import Helper  # Import the Helper class
import pandas as pd
import os
class BankingManager:
    def __init__(self, working_folder, data_access, folder_root, path_tc_closed,corriente_temporal_downloads, fechas_corte, today, PICKLE_DEBITO_CORRIENTE, PICKLE_CREDITO_CORRIENTE, PICKLE_DEBIT_CLOSED, PICKLE_CREDIT_CLOSED):
        self.working_folder = working_folder
        self.data_access = data_access
        self.folder_root = folder_root
        self.path_tc_closed = path_tc_closed
        self.corriente_temporal_downloads = corriente_temporal_downloads
        self.fechas_corte = fechas_corte
        self.today = today
        self.pickle_debito_corriente = PICKLE_DEBITO_CORRIENTE
        self.pickle_credito_corriente= PICKLE_CREDITO_CORRIENTE
        self.pickle_debito_cerrado = PICKLE_DEBIT_CLOSED
        self.pickle_credito_cerrado = PICKLE_CREDIT_CLOSED
        self.helper = Helper()

        # Pass path_tc_closed and fechas_corte to DownloaderWorkflow
        self.descargador = DownloaderWorkflow(
            self.working_folder, self.data_access, self.folder_root, self.path_tc_closed, self.corriente_temporal_downloads,
            self.fechas_corte, self.today, self.pickle_debito_cerrado, self.pickle_credito_cerrado, 
        )
        self.sql_operations = SQL_CONNEXION_UPDATING(
            self.working_folder,
            self.data_access
        )
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
                print("Cargar a SQL, GoogleSheet")
                paths_destino = {}
                suffix_debito = '_debito.csv'
                partial_path_debito = self.helper.archivo_corriente_reciente(self.today, suffix_debito, 'corriente')
                path_debito = os.path.join(self.working_folder,partial_path_debito )
                paths_destino['debito_corriente'] = path_debito
                suffix_credit = '_credito.csv'
                partial_path_credit = self.helper.archivo_corriente_reciente(self.today, suffix_credit, 'corriente')
                path_credito = os.path.join(self.working_folder,partial_path_credit )
                paths_destino['credito_corriente'] = path_credito
                paths_destino['credito_cerrado'] = self.pickle_credito_cerrado
                paths_destino['debito_cerrado'] = self.pickle_debito_cerrado
                schema_lake = 'banorte_lake'
                self.sheets_updater.update_multiple_sheets(paths_destino)
                
                for key, file_path in paths_destino.items():
                    try:
                        # Intentar cargar el archivo como DataFrame
                        if file_path.endswith('.csv'):
                            df = pd.read_csv(file_path)
                        elif file_path.endswith('.xlsx'):
                            df = pd.read_excel(file_path)
                        elif file_path.endswith('.pkl'):
                            df = pd.read_pickle(file_path)
                        else:
                            print(f"⚠️ Formato de archivo no soportado: {file_path}")
                            continue

                        # Actualizar la base de datos SQL
                        df.columns = df.columns.str.replace('.', '_').str.replace(' ', '_').str.lower()
                        
                        self.sql_operations.update_sql(df, schema_lake, key)

                        print(f"✅ Archivo {file_path} cargado exitosamente en la tabla {key} del esquema {schema_lake}.")

                    except FileNotFoundError:
                        print(f"⚠️ Archivo no encontrado: {file_path}")
                    except Exception as e:
                        print(f"❌ Error al cargar el archivo {file_path}: {e}")
            elif choice == "0":
            
                print("👋 ¡Hasta luego!")
                return
            else:
                print("\n⚠️ Elige una opción válida (1 o 0). Inténtalo de nuevo.\n")