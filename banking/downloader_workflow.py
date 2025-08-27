from utils.helpers import Helper  # Import the Helper class
from .web_automation import WebAutomation
import pandas as pd
import os

class DownloaderWorkflow:
    def __init__(self, working_folder, data_access, folder_root, path_tc_closed, fechas_corte, today, pickle_debito_cerrado, pickle_credito_cerrado):
        self.working_folder = working_folder
        self.data_access = data_access
        self.folder_root = folder_root
        self.path_tc_closed = path_tc_closed  # Pass PATH_TC_CLOSED as an argument
        self.fechas_corte = fechas_corte  # Pass FECHAS_CORTE as an argument
        self.today = today
        self.helper = Helper()
        self.pickle_debito_cerrado = pickle_debito_cerrado
        self.pickle_credito_cerrado = pickle_credito_cerrado
        self.folder_temporal_al_corte = os.path.join(self.path_tc_closed, "Descargas temporales")
        self.web_automation = WebAutomation(self.data_access)
    def periodos_credito_cerrado(self):
        today_period = pd.to_datetime(self.today).to_period('M')
        current_period_credit = today_period
        previous_period_credit = current_period_credit - 1

        # Implementación de la función para manejar los períodos de crédito cerrado
        fechas_corte_credito = self.helper.load_pickle_as_dataframe(self.fechas_corte)
        fechas_corte_credito['FECHA CORTE'] = pd.to_datetime(
            fechas_corte_credito['Fecha corte dd-mm-yyyy'],
            format='%d/%m/%Y',
            errors='coerce'
        )
        fechas_corte_credito['FECHA CORTE'] = pd.to_datetime(
            fechas_corte_credito['FECHA CORTE'], 
            errors='coerce'
        )
        # Filtrar filas donde el año y mes de 'FECHA CORTE' coincidan con self.today
        fechas_corte_credito['period'] = fechas_corte_credito['FECHA CORTE'].dt.to_period('M')
        fechas_corte_credito_filtrado = fechas_corte_credito[fechas_corte_credito['period'] == today_period]
        # Check if there are any valid datetime values in the column

        if not fechas_corte_credito_filtrado['FECHA CORTE'].notna().any():
            print("⚠️ La columna 'FECHA CORTE' no contiene valores válidos de fecha.")
            print("Función para actualizar una nueva fecha")
            return
        else:
            print(f"La fecha de corte crédito existe, procedemos a revisar si {fechas_corte_credito_filtrado['Fecha corte dd-mm-yyyy'].values[0]} es mayor a {self.today}")
            print(f"Ruta de fechas de corte: {fechas_corte_credito_filtrado.head()}")
        if pd.Timestamp(self.today) < fechas_corte_credito_filtrado['FECHA CORTE'].values[0]:
            expected_credit = [str(previous_period_credit)]
            return expected_credit
        else:
            expected_credit = [str(current_period_credit), str(previous_period_credit)]
            return expected_credit

    def descargador_workflow(self):
        print(self.helper.message_print("Bienvenido al menú bancario"))
        # Archivos esperados débito
        today_period = pd.to_datetime(self.today).to_period('M')
        current_period = today_period - 1
        previous_period = current_period - 1
        expected_debit = [str(current_period), str(previous_period)]
        print(f"Periodo esperado para débito: {expected_debit}")
        # Archivos eseperados crédito        
        fechas_corte_credito = self.periodos_credito_cerrado()
        print(f"Periodo esperado para crédito: {fechas_corte_credito}")
        print(f"Fecha de hoy: {self.today}")
        # expected_debit: lista con dos veces el periodo anterior como string (ej: ['2024-05', '2024-05'])
        df_credito_cerrado = self.helper.load_pickle_as_dataframe(self.pickle_credito_cerrado)
        df_debito_cerrado = self.helper.load_pickle_as_dataframe(self.pickle_debito_cerrado)
        archivos_credito_cerrado = self.check_expected_file(fechas_corte_credito, df_credito_cerrado)
        archivos_debito_cerrado = self.check_expected_file(expected_debit, df_debito_cerrado)    
        files_to_download = {'credito_columns': archivos_credito_cerrado, 'debito_columns': archivos_debito_cerrado}
        print(f"Archivos faltantes: {files_to_download}")
        if files_to_download:  # Check if the dictionary is not empty
            self.download_missing_files(files_to_download)

    def download_missing_files(self, files_to_download):
        # Process missing files
        grouped_missing_files = {}
        for key, value in files_to_download.items():
            if value:  # Ensure there are missing files to process
                for date in value:
                    if date not in grouped_missing_files:
                        grouped_missing_files[date] = []  # Initialize an empty list for the date
                    grouped_missing_files[date].append(key)  # Add the column type (e.g., 'credito_columns')
        # prueba
        grouped_missing_files = {'2025-08': ['credito_columns'], '2025-07': ['credito_columns', 'debito_columns']}
        # Print grouped missing files
        print(grouped_missing_files)
        for date, columns in grouped_missing_files.items():
            print(f"Fecha: {date}, Archivos faltantes: {columns}")
            self.web_automation.execute_download_session(self.folder_temporal_al_corte)
            # Process each missing file for the date
            for column_type in columns:
                if column_type == 'credito_columns':
                    expected_headers = self.data_access['BANORTE_credit_headers']
                    suffix = 'credito-cerrado.csv'
                elif column_type == 'debito_columns':
                    expected_headers = self.data_access['BANORTE_debit_headers']
                    suffix = 'debito-cerrado.csv'
                else:
                    continue

                print(f"Esperando archivo para la fecha {date} con encabezados: {expected_headers}")
                input("Por favor, descarga el archivo y presiona Enter para continuar...")

                # Check the download folder for the file
                downloaded_files = self.helper.get_files_in_directory(self.folder_temporal_al_corte)
                valid_files = []
                for file in downloaded_files:
                    file_path = os.path.join(self.folder_temporal_al_corte, file)
                    file_headers = self.helper.get_file_headers(file_path)
                    if set(expected_headers).issubset(file_headers):
                        print(f"✅ Archivo válido encontrado: {file}")
                        valid_files.append(file_path)
                    else:
                        print(f"⚠️ Archivo inválido encontrado: {file}. Encabezados no coinciden.")

                # Merge files if multiple valid files exist
                if len(valid_files) > 1:
                    print(f"🔄 Fusionando {len(valid_files)} archivos para la fecha {date}...")
                    merged_file = self.helper.merge_files(valid_files)
                    final_file_name = f"{date}-{suffix}"
                    final_file_path = os.path.join(self.path_tc_closed, "Repositorio por mes", final_file_name)
                    self.helper.move_file(merged_file, final_file_path)
                    print(f"✅ Archivo fusionado movido a: {final_file_path}")
                elif len(valid_files) == 1:
                    final_file_name = f"{date}-{suffix}"
                    final_file_path = os.path.join(self.path_tc_closed, "Repositorio por mes", final_file_name)
                    self.helper.move_file(valid_files[0], final_file_path)
                    print(f"✅ Archivo movido a: {final_file_path}")
                else:
                    print(f"⚠️ No se encontraron archivos válidos para la fecha {date} y tipo {column_type}.")

    def check_expected_file(self, list_expected_suffix, input_dataframe): 
        final_files = []
        sum_columns = ['Cargo', 'Cargos']  # Columns to check for sums

        for item in list_expected_suffix:
            # Filter rows where 'file_name' starts with the given prefix (e.g., '2025-09')
            filtered_rows = input_dataframe[input_dataframe['file_name'].str.startswith(item, na=False)]

            if not filtered_rows.empty:
                # If rows are found, check if sum_columns exist and print their sums
                print(f"✅ Archivos encontrados para el patrón '{item}': {len(filtered_rows)} filas.")
                for col in sum_columns:
                    if col in filtered_rows.columns:
                        column_sum = filtered_rows[col].sum()
                        print(f"   - Suma de la columna '{col}': {column_sum}")
                    else:
                        continue
            else:
                # If no rows are found, add the prefix to final_files
                print(f"⚠️ No se encontraron archivos para el patrón '{item}'.")
                final_files.append(item)

        # Return the list of prefixes for which no matching rows were found
        if final_files:
            print(f"❌ Archivos faltantes para los patrones: {final_files}")
            return final_files  
        else:
            print("✅ Todos los archivos esperados están presentes.")
            return None