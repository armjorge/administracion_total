from .web_automation import WebAutomation
import pandas as pd
import os

class DownloaderWorkflow:
    def __init__(self, working_folder, data_access, folder_root, path_tc_closed,corriente_temporal_downloads, fechas_corte, today, pickle_debito_cerrado, pickle_credito_cerrado):
        self.working_folder = working_folder
        self.data_access = data_access
        self.today = today
        self.current_folder = os.path.join(self.working_folder,'Info Bancaria', f'{self.today.year}-{self.today.month:02d}')
        self.closed_folder = os.path.join(self.working_folder,'Info Bancaria', 'Meses cerrados', 'Repositorio por mes')
        self.web_automation = WebAutomation(self.data_access, self.today, self.closed_folder, self.working_folder)
    
    def descargador_workflow(self):
        print("Bienvenido al menú bancario")
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

        expected_files = self.confirmar_si_existen(expected_files)
        for key, value in expected_files.items():
            print(f"Periodo: {key}, Archivos esperados: {value}")
        expected_files = {key: value for key, value in expected_files.items() if value}
        self.download_missing_files(expected_files)
    
    def download_missing_files(self, files_to_download):
        print(f"download_missing_files Directorio de descargas para archivos cerrados: {self.folder_al_corte_descargas_cerradas}")
        print("Función download_missing_files: le pasa la lista de archivos que se requieren")
        print("Parámetro files_to_download:")
        print(files_to_download)
        for periodo, tipo_archivos in files_to_download.items():
            print(f"\nProcesando archivos faltantes para el periodo {periodo}...")

            # Si hay archivos cerrados en la lista
            if 'credito_cerrado' in tipo_archivos or 'debito_cerrado' in tipo_archivos:
                print(f"Archivos cerrados detectados en el periodo {periodo}: {tipo_archivos}")

                # 1. Eliminar los archivos que terminen con '_corriente' de la lista
                tipo_archivos = [archivo for archivo in tipo_archivos if not archivo.endswith('_corriente')]
                print(f"Lista después de eliminar archivos '_corriente': {tipo_archivos}")

                # 2. Detectar cuál de los dos (credito_cerrado o debito_cerrado) está presente
                archivos_faltantes_cerrado = []
                if 'credito_cerrado' in tipo_archivos:
                    archivos_faltantes_cerrado.append('credito_cerrado')
                if 'debito_cerrado' in tipo_archivos:
                    archivos_faltantes_cerrado.append('debito_cerrado')

                print(f"Archivos faltantes cerrados para el periodo {periodo}: {archivos_faltantes_cerrado}")

                # 3. Si la lista tiene elementos, ejecutar la descarga
                if archivos_faltantes_cerrado:
                    print(f"Ejecutando descarga para archivos cerrados: {archivos_faltantes_cerrado}")
                    self.web_automation.execute_download_session(
                        self.folder_al_corte_descargas_cerradas, archivos_faltantes_cerrado, periodo
                    )

            elif 'credito_corriente' in tipo_archivos or 'debito_corriente' in tipo_archivos:
                # Remover 'credito_corriente' si no está mencionado
                if not 'credito_corriente' in tipo_archivos:
                    print("Removiendo 'credito_corriente'..., no es necesario")
                    # Aquí puedes agregar la lógica para verificar si 'credito_corriente' debe ser eliminado
                    tipo_archivos.remove('credito_corriente')  # Remover si es necesario

                # Remover 'debito_corriente' si no está mencionado
                if not 'debito_corriente' in tipo_archivos:
                    print("Removiendo 'debito_corriente'..., no es necesario")
                    # Aquí puedes agregar la lógica para verificar si 'debito_corriente' debe ser eliminado
                    tipo_archivos.remove('debito_corriente')  # Remover si es necesario
                archivos_faltantes_corriente = tipo_archivos  # Actualizar la lista de archivos faltantes
                print(f"Archivos faltantes Mes corriente a descargar: {archivos_faltantes_corriente}")
                print(f"Vamos a descargar {archivos_faltantes_corriente} del mes {periodo}")
                
                if archivos_faltantes_corriente: 
                    self.web_automation.execute_download_session(self.corriente_temporal_downloads, archivos_faltantes_corriente, periodo)

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