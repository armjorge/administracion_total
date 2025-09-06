from utils.helpers import Helper
from .downloader_workflow import DownloaderWorkflow
from .sql_operations import SQL_CONNEXION_UPDATING
from .sheets_updater import SheetsUpdater
from utils.helpers import Helper  # Import the Helper class
import pandas as pd
import os
import pickle
from datawarehouse.datawarehouse import DataWarehouse

class BankingManager:
    def __init__(self, working_folder, data_access, folder_root, path_tc_closed,corriente_temporal_downloads, fechas_corte, today, PICKLE_DEBITO_CORRIENTE, PICKLE_CREDITO_CORRIENTE, PICKLE_DEBIT_CLOSED, PICKLE_CREDIT_CLOSED):
        self.working_folder = working_folder
        self.data_access = data_access
        self.folder_root = folder_root
        self.path_tc_closed = path_tc_closed
        self.path_repository_closed = os.path.join(self.path_tc_closed, "Repositorio por mes")
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
        # Fix path: ensure 'Implementación/Estrategia' with a path separator
        self.reporting_folder = os.path.join(self.folder_root, "Implementación", "Estrategia")
        self.datawarehouse = DataWarehouse(self.reporting_folder, self.data_access)

    def run_banking_menu(self):
        print(Helper.message_print("Bienvenido al menú bancario"))
        
        """Run the banking menu."""
        while True:
            choice = input(f"""{Helper.message_print('¿Qué deseas hacer?')}
        1. Descargar
        2. Cargar a SQL, GoogleSheet
        3. Estrategia y evaluación
        0. Salir
        Elige una opción: """).strip()

            if choice == "1":
                # Call the downloader workflow
                self.descargador.descargador_workflow()

            elif choice == "2":
                print("Cargar a SQL, GoogleSheet")
                self.helper.feed_new_pickles(self.path_repository_closed, self.pickle_debito_cerrado, self.data_access['BANORTE_debit_headers'])
                self.helper.feed_new_pickles(self.path_repository_closed, self.pickle_credito_cerrado, self.data_access['BANORTE_credit_headers'])
                paths_destino = {}
                suffix_debito = '_debito.csv'
                partial_path_debito = self.helper.archivo_corriente_reciente(self.today, suffix_debito, 'corriente')
                path_debito = os.path.join(self.working_folder,partial_path_debito )
                pickle_debito = self.helper.update_pickle(path_debito, self.pickle_debito_corriente)
                paths_destino['debito_corriente'] = self.pickle_debito_corriente
                suffix_credit = '_credito.csv'
                partial_path_credit = self.helper.archivo_corriente_reciente(self.today, suffix_credit, 'corriente')
                path_credito = os.path.join(self.working_folder,partial_path_credit )
                pickle_credito = self.helper.update_pickle(path_credito, self.pickle_credito_corriente)
                paths_destino['credito_corriente'] = self.pickle_credito_corriente
                paths_destino['credito_cerrado'] = self.pickle_credito_cerrado
                paths_destino['debito_cerrado'] = self.pickle_debito_cerrado
                print(f"Rutas de destino preparadas: {paths_destino}")
                
                schema_lake = 'banorte_lake'
                output_excel = os.path.join(os.path.expanduser("~"), "Downloads", "Bancos.xlsx")
                self.export_pickles_to_excel(paths_destino, output_excel)
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
                        df= self.helper.corrige_fechas(df, 'Fecha')
                        df= self.helper.corrige_fechas(df, 'file_date')      
                        # Actualizar la base de datos SQL
                        df.columns = df.columns.str.replace('.', '_').str.replace(' ', '_').str.lower()
                        sql_url = self.data_access['sql_url']
                        self.sql_operations.update_sql(df, schema_lake, key, sql_url)

                        print(f"✅ Fuente de Datos (data source) {os.path.basename(file_path)} cargado exitosamente en la tabla {key} del esquema {schema_lake}.")

                    except FileNotFoundError:
                        print(f"⚠️ Archivo no encontrado: {file_path}")
                    except Exception as e:
                        print(f"❌ Error al cargar el archivo {file_path}: {e}")
            #elif choice == "3":
                #self.datawarehouse.etl_process()
            elif choice == "0":
            
                print("👋 ¡Hasta luego!")
                return
            else:
                print("\n⚠️ Elige una opción válida (1 o 0). Inténtalo de nuevo.\n")

    def export_pickles_to_excel(self, paths_destino, output_excel): 
        try:
            with pd.ExcelWriter(output_excel, engine='openpyxl') as writer:
                for sheet_name, pickle_path in paths_destino.items():
                    if os.path.exists(pickle_path):
                        with open(pickle_path, 'rb') as f:
                            df = pickle.load(f)
                            df= self.helper.corrige_fechas(df, 'Fecha')
                            df= self.helper.corrige_fechas(df, 'file_date')
                            # Limitar el nombre de la hoja a 31 caracteres
                            sheet_name = sheet_name[:31]
                            df.to_excel(writer, sheet_name=sheet_name, index=False)
                            print(f"✅ Exportado {pickle_path} a la hoja {sheet_name}")
                    else:
                        print(f"❌ Archivo pickle no encontrado: {pickle_path}")
            print(f"✅ Todos los pickles exportados a: {output_excel}")
        except Exception as e:
            print(f"❌ Error al exportar pickles a Excel: {e}") 
