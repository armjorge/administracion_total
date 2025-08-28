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
    def gestor_de_credito_al_mes(self, expected_files):
        # Convertir el periodo actual a string
        today_period = str(pd.to_datetime(self.today).to_period('M'))

        # Cargar y procesar las fechas de corte
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
        # Filtrar filas donde el año y mes de 'FECHA CORTE' coincidan con el periodo actual
        fechas_corte_credito['period'] = fechas_corte_credito['FECHA CORTE'].dt.to_period('M')
        fechas_corte_credito_filtrado = fechas_corte_credito[fechas_corte_credito['period'] == today_period]
        # Eliminar 'credito_cerrado' de la lista asociada a today_period si la fecha de corte es mayor a la fecha actual        # Si no hay fechas válidas en 'FECHA CORTE'
        if not fechas_corte_credito_filtrado['FECHA CORTE'].notna().any():
            print("⚠️ La columna 'FECHA CORTE' no contiene valores válidos de fecha.")
            print("Función para actualizar una nueva fecha")
            while True:
                try:
                    user_input = int(input("Escribe 1 si ya pasó la fecha de corte, 2 si no ha pasado: "))
                    if user_input == 1:
                        return expected_files  # Devuelve el diccionario actualizado
                    elif user_input == 2:
                        return expected_files  # Devuelve el diccionario original
                    else:
                        print("Entrada no válida. Por favor, escribe 1 o 2.")
                except ValueError:
                    print("Entrada no válida. Por favor, escribe un número (1 o 2).")
        else:
            # Si hay fechas válidas, verifica si la fecha de corte es mayor a la fecha actual
            print(f"La fecha de corte crédito existe, procedemos a revisar si {fechas_corte_credito_filtrado['Fecha corte dd-mm-yyyy'].values[0]} es mayor a {self.today}")
            print(f"Ruta de fechas de corte: {fechas_corte_credito_filtrado.head()}")

        # Comparar la fecha actual con la primera fecha de corte válida
        if pd.Timestamp(self.today) < fechas_corte_credito_filtrado['FECHA CORTE'].values[0]:
            return expected_files[today_period].remove('credito_cerrado')  # Devuelve el diccionario actualizado
        else:
            return expected_files  # Devuelve el diccionario original
    def confirmar_si_existen(self, expected_files): 
        """expected_files = 
        Periodo: 2025-08, Archivos esperados: ['credito_corriente', 'credito_cerrado', 'debito_corriente']
        Periodo: 2025-07, Archivos esperados: ['credito_cerrado', 'debito_cerrado']
        Periodo: 2025-06, Archivos esperados: ['debito_cerrado']
        """
        for key, value in expected_files.items():  # key es el periodo, value es la lista de archivos esperados
            print(f"Periodo: {key}, Archivos esperados: {value}")
            for item in value:  # Iterar sobre la lista de archivos esperados
                if item == 'credito_cerrado':
                    print(f"Procesando 'credito_cerrado' para el periodo {key}")
                    # Lógica para 'credito_cerrado'
                elif item == 'credito_corriente': 
                    print(f"Procesando 'credito_corriente' para el periodo {key}")
                    # Lógica para 'credito_corriente'
                elif item == 'debito_corriente':
                    print(f"Procesando 'debito_corriente' para el periodo {key}")
                    # Lógica para 'debito_corriente'
                elif item == 'debito_cerrado':
                    print(f"Procesando 'debito_cerrado' para el periodo {key}")
                    # Lógica para 'debito_cerrado'
                else:
                    print(f"⚠️ Tipo de archivo desconocido: {item}")

    def descargador_workflow(self):
        print(self.helper.message_print("Bienvenido al menú bancario"))
        # Archivos esperados débito
        today_period = str(pd.to_datetime(self.today).to_period('M'))  # Convertir a string
        previous_period = str((pd.to_datetime(self.today).to_period('M') - 1))  # 1 mes atrás
        two_previous_period = str((pd.to_datetime(self.today).to_period('M') - 2))  # 2 meses atrás

        expected_files = {
            today_period: ['credito_corriente', 'credito_cerrado', 'debito_corriente'],
            previous_period: ['credito_cerrado', 'debito_cerrado'],
            two_previous_period: ['debito_cerrado']
        }        # Retirar credito cerrado de today_period si tenemos la fecha de corte
        expected_files =  self.gestor_de_credito_al_mes(expected_files)
        for key, value in expected_files.items():
            print(f"Periodo: {key}, Archivos esperados: {value}")
        expected_files = self.confirmar_si_existen(expected_files)

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
        grouped_missing_files = {'2025-08': ['credito_columns', 'debito_columns'], '2025-07': ['credito_columns', 'debito_columns']}
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