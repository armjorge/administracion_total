from utils.helpers import Helper  # Import the Helper class
from .web_automation import WebAutomation
import pandas as pd
import os

class DownloaderWorkflow:
    def __init__(self, working_folder, data_access, folder_root, path_tc_closed,corriente_temporal_downloads, fechas_corte, today, pickle_debito_cerrado, pickle_credito_cerrado):
        self.working_folder = working_folder
        self.data_access = data_access
        self.folder_root = folder_root
        self.path_tc_closed = path_tc_closed  # Pass PATH_TC_CLOSED as an argument
        self.corriente_temporal_downloads = corriente_temporal_downloads
        self.fechas_corte = fechas_corte  # Pass FECHAS_CORTE as an argument
        self.today = today
        self.helper = Helper()
        self.pickle_debito_cerrado = pickle_debito_cerrado
        self.pickle_credito_cerrado = pickle_credito_cerrado
        self.folder_al_corte_descargas_corrientes = os.path.join(self.working_folder, "Descargas temporales")
        self.folder_al_corte_descargas_cerradas = os.path.join(self.path_tc_closed, "Temporal Downloads")
        print(f"Directorio de descargas cerradas: {self.folder_al_corte_descargas_cerradas}")
        self.web_automation = WebAutomation(self.data_access, self.today, self.path_tc_closed, self.working_folder)
    def gestor_de_credito_al_mes(self, expected_files):
        # Convertir el periodo actual a string
        today_period = str(pd.to_datetime(self.today).to_period('M'))

        # Cargar y procesar las fechas de corte
        fechas_corte_credito = self.helper.load_pickle_as_dataframe(self.fechas_corte)
        print(fechas_corte_credito.head(12))
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

        # Si no hay fechas válidas, escribe en el dataframe el input: verifica si la fecha de corte es mayor a la fecha actual
        # Si no hay fechas válidas, escribe en el dataframe el input: verifica si la fecha de corte es mayor a la fecha actual
        if not fechas_corte_credito_filtrado['FECHA CORTE'].notna().any():
            print("⚠️ La columna 'FECHA CORTE' no contiene valores válidos de fecha.")
            # Crear la fecha por defecto: día 5 del mes del periodo actual
            default_date = pd.to_datetime(today_period + '-05', format='%Y-%m-%d')
            
            df_to_update = self.helper.load_pickle_as_dataframe(self.fechas_corte)
            # Agregar las columnas necesarias para procesar 'period'
            df_to_update['FECHA CORTE'] = pd.to_datetime(
                df_to_update['Fecha corte dd-mm-yyyy'],
                format='%d/%m/%Y',
                errors='coerce'
            )
            df_to_update['period'] = df_to_update['FECHA CORTE'].dt.to_period('M')
            
            # Mapear el mes numérico a español
            month_num = pd.to_datetime(today_period).month
            month_spanish = {
                1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril', 5: 'Mayo', 6: 'Junio',
                7: 'Julio', 8: 'Agosto', 9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
            }.get(month_num, 'Desconocido')
            
            # Verificar si existe una fila para el mes en español
            if (df_to_update['Mes'] == month_spanish).any():
                # Actualizar la fila existente
                df_to_update.loc[df_to_update['Mes'] == month_spanish, 'Fecha corte dd-mm-yyyy'] = default_date.strftime('%d/%m/%Y')
            else:
                # Agregar una nueva fila si no existe (aunque probablemente siempre exista)
                new_row = {'Mes': month_spanish, 'Fecha corte dd-mm-yyyy': default_date.strftime('%d/%m/%Y')}
                df_to_update = pd.concat([df_to_update, pd.DataFrame([new_row])], ignore_index=True)
            
            # Eliminar las columnas temporales antes de guardar
            df_to_update.drop(columns=['FECHA CORTE', 'period'], inplace=True, errors='ignore')
            # Guardar directamente sin confirmación
            self.helper.save_dataframe_to_pickle(df_to_update, self.fechas_corte)
            print("✅ Fecha actualizada en el pickle.")
            
            # Recargar el DataFrame actualizado y recalcular el filtro
            fechas_corte_credito = self.helper.load_pickle_as_dataframe(self.fechas_corte)
            fechas_corte_credito['FECHA CORTE'] = pd.to_datetime(
                fechas_corte_credito['Fecha corte dd-mm-yyyy'],
                format='%d/%m/%Y',
                errors='coerce'
            )
            fechas_corte_credito['period'] = fechas_corte_credito['FECHA CORTE'].dt.to_period('M')
            fechas_corte_credito_filtrado = fechas_corte_credito[fechas_corte_credito['period'] == today_period]
            
        else:
            # Si hay fechas válidas, verifica si la fecha de corte es mayor a la fecha actual
            print(f"La fecha de corte crédito existe, procedemos a revisar si {fechas_corte_credito_filtrado['Fecha corte dd-mm-yyyy'].values[0]} es mayor a {self.today}")
            print(f"Ruta de fechas de corte: {fechas_corte_credito_filtrado.head()}")

        # Comparar la fecha actual con la primera fecha de corte válida
        if pd.Timestamp(self.today) < fechas_corte_credito_filtrado['FECHA CORTE'].values[0]:
            expected_files[today_period].remove('credito_cerrado')
            return expected_files  # Devuelve el diccionario actualizado
        else:
            return expected_files  # Devuelve el diccionario original



    def get_newest_file(self, period, suffix):
        path_dinamico = os.path.join(self.working_folder, f"{period}")
        self.helper.create_directory_if_not_exists(path_dinamico)
        formatted_today = self.today.strftime('%Y-%m-%d')
        filename = f"{formatted_today}{suffix}.csv"
        file_path = os.path.join(path_dinamico, filename)
        return file_path, formatted_today

    def confirmar_si_existen(self, expected_files):
        """expected_files =
        Periodo: 2025-08, Archivos esperados: ['credito_corriente', 'credito_cerrado', 'debito_corriente']
        Periodo: 2025-07, Archivos esperados: ['credito_cerrado', 'debito_cerrado']
        Periodo: 2025-06, Archivos esperados: ['debito_cerrado']
        """
        for key, value in expected_files.items():  # key es el periodo, value es la lista de archivos esperados
            print(f"Periodo: {key}, Archivos esperados: {value}")
            for item in value[:]:  # Iterar sobre la lista de archivos esperados (usar copia para modificar la lista)
                if item == 'credito_cerrado':
                    print(f"Procesando 'credito_cerrado' para el periodo {key}")
                    df_credito = self.helper.load_pickle_as_dataframe(self.pickle_credito_cerrado)
                    # Filtrar filas donde 'file_name' comienza con el patrón {key}
                    filtered_rows = df_credito[df_credito['file_name'].str.startswith(key, na=False)]
                    if not filtered_rows.empty:
                        print(f"✅ Archivo encontrado en el DataFrame para el patrón '{key}':")
                        unique_files = filtered_rows['file_name'].unique()
                        print(unique_files)  # Mostrar los nombres de archivo únicos encontrados
                        expected_files[key].remove('credito_cerrado')
                          # Eliminar 'credito_cerrado' de la lista
                    else:
                        print(f"⚠️ No se encontraron archivos para el patrón '{key}' en el DataFrame.")
                        if pd.to_datetime(self.today).day <= 10:
                            user_choice = input("Estamos cerca de los días de corte. ¿El banco tiene disponible el archivo con corte? (s/n): ")
                            if user_choice.lower() in ['n', 'no']:
                                expected_files[key].remove('credito_cerrado')
                elif item == 'credito_corriente': 
                    print(f"Procesando 'credito_corriente' para el periodo {key}")
                    suffix = '_credito'
                    archivo_credito, fecha = self.get_newest_file(key, suffix)
                    print("\nBuscando archivos de crédito corriente en:", archivo_credito, "\n")
                    if os.path.exists(archivo_credito): 
                        print(f"✅ Archivo de hoy {fecha} encontrado: {archivo_credito}")
                        if 'credito_corriente' in expected_files[key]:  # Check if the item exists before removing
                            expected_files[key].remove('credito_corriente')
                    else: 
                        print(f"⚠️ No se encontró archivo de hoy {fecha} reciente para {suffix}.")

                elif item == 'debito_corriente':
                    suffix = '_debito'
                    archivo_debito, fecha = self.get_newest_file(key, suffix)
                    print(f"Procesando 'debito_corriente' para el periodo {key}")
                    if os.path.exists(archivo_debito):
                        print(f"✅ Archivo de hoy {fecha} encontrado: {archivo_debito}")
                        if 'debito_corriente' in expected_files[key]:  # Check if the item exists before removing
                            expected_files[key].remove('debito_corriente')
                    else:
                        print(f"⚠️ No se encontró archivo de hoy {fecha} reciente para {suffix}.")

                elif item == 'debito_cerrado':
                    print(f"Procesando 'debito_cerrado' para el periodo {key}")
                    df_debito = self.helper.load_pickle_as_dataframe(self.pickle_debito_cerrado)
                    # Filtrar filas donde 'file_name' comienza con el patrón {key}
                    filtered_rows = df_debito[df_debito['file_name'].str.startswith(key, na=False)]
                    if not filtered_rows.empty:
                        print(f"✅ Archivo encontrado en el DataFrame para el patrón '{key}':")
                        unique_files = filtered_rows['file_name'].unique()
                        print(unique_files)  # Mostrar los nombres de archivo únicos encontrados
                        expected_files[key].remove('debito_cerrado')  # Eliminar 'debito_cerrado' de la lista
                    else:
                        print(f"⚠️ No se encontraron archivos para el patrón '{key}' en el DataFrame.")

                else:
                    print(f"⚠️ Tipo de archivo desconocido: {item}")
        print("\n\nDEBUG\n\n", expected_files)
        return expected_files
    
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
                print(self.helper.message_print(f"Vamos a descargar {archivos_faltantes_corriente} del mes {periodo}"))
                
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