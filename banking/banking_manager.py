import os
import pickle
from datetime import datetime, date
from .file_processor import FileProcessor
from .web_automation import WebAutomation
from .sheets_updater import SheetsUpdater
from utils.helpers import message_print, create_directory_if_not_exists
import sys
import pandas as pd
from glob import glob 
import hashlib
from collections import defaultdict
from .sql_operations import SQL_Operations

class BankingManager:
    def __init__(self, working_folder, data_access, folder_root):
        self.working_folder = working_folder
        self.data_access = data_access
        self.folder_root = folder_root
        
        # Configurar rutas
        self.download_folder = os.path.join(working_folder, "Temporal Downloads")
        self.path_tc_closed = os.path.join(self.working_folder, 'Meses cerrados')
        
        # Crear directorios necesarios
        create_directory_if_not_exists([
            self.download_folder, 
            self.path_tc_closed
        ])
        
        # Inicializar componentes
        self.file_processor = FileProcessor(working_folder, data_access) 
        self.web_automation = WebAutomation(self.download_folder, folder_root) #Aquí le dices dónde descargar. 
        self.sheets_updater = SheetsUpdater(working_folder, data_access)
        self.sql_operations = SQL_Operations(data_access)
        
    
    def run_banking_menu(self):
        """Ejecuta el menú principal del sistema bancario"""
        
        while True:
            choice = input(f"""{message_print('¿Qué deseas hacer?')}
        1. Descargar, renombrar y mover archivos CSV corrientes
        2. Exportar CSV's a local y cargar a google sheets
        3. Procesar archivos CSV al mes corte
        4. Conectar a la base de datos SQL
        0. Salir
        Elige una opción: """)

            if choice == "1":
                self._process_current_files()
            elif choice == "2":
                self._cargar_gsheet_exportar_excel()
            elif choice == "3":
                self._process_monthly_cut_files()
                self._process_closed_credit_accounts()
            elif choice == "4":
                # Llamar a la función para conectar a la base de datos
                self.sql_operations.sql_business_mining()
            elif choice == "0":
                print("👋 ¡Hasta luego!")
                return
            else:
                print("⚠️ Opción no válida. Por favor elige 1, 2, 3, 4 o 0.\n")
    
    def _process_current_files(self):
        """Procesa archivos corrientes (Opción 1)"""
        print("📥 Descargando archivos corrientes...")
        
        # Descargar archivos
        success = self.web_automation.execute_download_session(self.data_access)
        
        if success:
            # Procesar archivos descargados
            self.file_processor.process_downloaded_files(
                self.download_folder, 
                mode="normal"
            )
    
    def _cargar_gsheet_exportar_excel(self, mode="precut"):
        """Procesa archivos posteriores al corte (Opción 2)"""
        print("📦 Procesando archivos CSV después del corte...")
        
        # Procesar archivos existentes
        dataframes = self.file_processor.process_post_cut_files(mode=mode)
        
        # ✅ NUEVO: Cargar DataFrames de crédito y débito cerrados
        df_credit, df_debit = self._load_existing_pickles()
        df_credit['file_name'] = df_credit['file_name'].str.replace('__HASH__.*', '', regex=True)
        df_debit['file_name'] = df_debit['file_name'].str.replace('__HASH__.*', '', regex=True)
        
        # ✅ NUEVO:
        #  Agregar DataFrames cerrados al diccionario
        if dataframes is None:
            dataframes = {}
        
        # Agregar datos cerrados al diccionario de dataframes
        dataframes['credit_closed'] = df_credit
        dataframes['debit_closed'] = df_debit
        
        print(f"📊 DataFrames preparados para Google Sheets:")
        for key, df in dataframes.items():
            if df is not None and not df.empty:
                print(f"   ✅ {key}: {df.shape[0]} registros")
            else:
                print(f"   ⚠️ {key}: vacío")
        
        # Actualizar Google Sheets
        if dataframes:
            self.sheets_updater.update_multiple_sheets(dataframes)
        # Normalizar fechas con función pequeña
        dataframes = self._fix_date_format(dataframes)
        self._export_to_excel(dataframes)
        
    def _process_monthly_cut_files(self):
        """Procesa archivos de corte mensual (Opción 3)"""
        print("📅 Procesando archivos de corte mensual...")
    
        # Crear nueva instancia para carpeta específica
        cut_final_folder = os.path.join(self.working_folder, "Meses cerrados")
        cut_download_folder = os.path.join(cut_final_folder, "Descargas temporales")
        # Crear directorios si no existen
        create_directory_if_not_exists([cut_final_folder, cut_download_folder])
        cut_web_automation = WebAutomation(cut_download_folder, self.folder_root)
                
        # Verificar fecha de corte y procesar si es necesario
        necesita_descarga = self._check_cut_date()
        
        if necesita_descarga:
            # Usar la instancia específica para archivos de corte (no la original)
            success = cut_web_automation.execute_download_session(
                self.data_access, 
                mode="monthly_cut"
            )
            
            if success:
                # Procesar archivos de corte en modo especial
                self.file_processor.process_downloaded_files(
                    cut_download_folder, 
                    mode="postcut"
                )
        
        # Procesar archivos existentes en modo post-corte
        # Cargar dataframes de debit y credit para eventualmente cargarlo a google sheet
        df_credit = self.file_processor.process_post_cut_files(mode="postcut").get("credit", pd.DataFrame())
        df_debit = self.file_processor.process_post_cut_files(mode="postcut").get("debit", pd.DataFrame())

    def _check_cut_date(self):
        """Verifica la fecha de corte y determina si se necesita descarga"""
        today = date.today()
        year = today.year
        month = today.month
        index_row = month - 1  # para índice 0-based
        
                
        # 1. Manejar archivo de fechas de corte
        self._handle_cut_dates_file(self.path_tc_closed, year, index_row)
        
        # 2. Cargar y mostrar pickles existentes
        self._load_and_display_pickles(self.path_tc_closed)
        
        # 3. Verificar archivos faltantes
        missing_credit = self._missing_credit(self.path_tc_closed)
        missing_debit = self._missing_debit(self.path_tc_closed)
        
        # 4. Determinar si necesita descarga
        necesita_descarga = missing_credit or missing_debit
        
        if necesita_descarga:
            print(f"\n🔍 Resumen de archivos faltantes:")
            if missing_credit:
                print(f"   ❌ Faltan archivos de CRÉDITO")
            if missing_debit:
                print(f"   ❌ Faltan archivos de DÉBITO")
        else:
            print(f"\n✅ Todos los archivos están actualizados")
        
        return necesita_descarga
    
    def _missing_credit(self, cut_folder):
        """Verifica si faltan archivos de crédito para descargar"""
        print(f"\n🔍 Verificando archivos de crédito...")
        
        try:
            # Cargar DataFrame de fechas de corte
            fechas_file = os.path.join(cut_folder, "2025 df_fechas_corte.pickle")
            with open(fechas_file, 'rb') as f:
                df_fechas_de_corte = pickle.load(f)
            
            # Cargar DataFrame de crédito al corte
            credit_file = os.path.join(cut_folder, "pickle_credit_closed.pkl")
            with open(credit_file, 'rb') as f:
                df_credito_al_corte = pickle.load(f)
            
            # Procesar file_name en crédito: quitar desde '.csv__HASH__' en adelante
            df_credito_al_corte['file_name'] = df_credito_al_corte['file_name'].str.split('__HASH__').str[0]
            
            # NUEVA LÍNEA: Quitar también la extensión .csv
            df_credito_al_corte['file_name_clean'] = df_credito_al_corte['file_name_clean'].str.replace('.csv', '', regex=False)
            
            # Convertir fechas de corte a datetime y truncar al mes
            df_fechas_de_corte['fecha_corte_dt'] = pd.to_datetime(
                df_fechas_de_corte['Fecha corte dd-mm-yyyy'], 
                format='%d/%m/%Y', 
                errors='coerce'
            )
            df_fechas_de_corte['year_month'] = df_fechas_de_corte['fecha_corte_dt'].dt.to_period('M')
            
            # Obtener mes actual y anterior
            today = date.today()
            year = today.year
            month = today.month
            
            current_period = pd.Period(f"{year}-{month:02d}", freq='M')
            previous_period = current_period - 1
            
            print(f"   📅 Verificando archivos para: {current_period} y {previous_period}")
            
            # 1) Verificar que year-month está en fechas de corte
            fecha_corte_exists = current_period in df_fechas_de_corte['year_month'].values
            if not fecha_corte_exists:
                print(f"   ⚠️  Fecha de corte para {current_period} no está registrada")
                return True

            # 2) ✅ NUEVO: Verificar archivos esperados CON Y SIN sufijo
            expected_files_with_suffix = [f"{current_period}_credit", f"{previous_period}_credit"]
            expected_files_without_suffix = [f"{current_period}", f"{previous_period}"]
            existing_files = df_credito_al_corte['file_name_clean'].unique()
            
            print(f"   📋 Archivos esperados (con sufijo): {expected_files_with_suffix}")
            print(f"   📋 Archivos esperados (sin sufijo): {expected_files_without_suffix}")
            print(f"   📋 Archivos existentes: {list(existing_files)}")
            
            # ✅ NUEVO: Verificar si existen archivos con o sin sufijo
            missing_files = []
            found_files = []
            
            for i, period in enumerate([current_period, previous_period]):
                # Intentar encontrar con sufijo primero
                with_suffix = f"{period}_credit"
                without_suffix = f"{period}"
                
                if with_suffix in existing_files:
                    found_files.append(with_suffix)
                    print(f"   ✅ Encontrado con sufijo: {with_suffix}")
                elif without_suffix in existing_files:
                    found_files.append(without_suffix)
                    print(f"   ✅ Encontrado sin sufijo: {without_suffix}")
                else:
                    missing_files.append(f"{period} (esperado: {with_suffix} o {without_suffix})")
                    print(f"   ❌ No encontrado: {period}")
            
            # 3) Imprimir mensajes de archivos faltantes
            if missing_files:
                print(f"   ❌ Archivos de CRÉDITO faltantes:")
                for missing in missing_files:
                    print(f"      - {missing}")
                necesita_descarga_credito = True
            else:
                print(f"   ✅ Todos los archivos de CRÉDITO están presentes")
                print(f"   📁 Archivos encontrados: {found_files}")
                necesita_descarga_credito = False
            
            # 4) Retornar resultado
            return necesita_descarga_credito
            
        except Exception as e:
            print(f"   ❌ Error verificando archivos de crédito: {e}")
            return True  # Si hay error, asumir que necesita descarga

    def _missing_debit(self, cut_folder):
        """Verifica si faltan archivos de débito para descargar"""
        print(f"\n🔍 Verificando archivos de débito...")
        
        try:
            # Cargar DataFrame de fechas de corte
            # Generar DataFrame con fecha del primer día del mes actual
            today = date.today()
            year = today.year
            month = today.month
            first_day_current_month = f"01/{month:02d}/{year}"
            
            df_fechas_de_corte = pd.DataFrame({
                'Fecha corte dd-mm-yyyy': [first_day_current_month]
            })


            # Cargar DataFrame de débito al corte
            debit_file = os.path.join(cut_folder, "pickle_debit_closed.pkl")
            with open(debit_file, 'rb') as f:
                df_debito_al_corte = pickle.load(f)
            
            # Procesar file_name en débito: quitar desde '.csv__HASH__' en adelante
            df_debito_al_corte['file_name_clean'] = df_debito_al_corte['file_name'].str.split('__HASH__').str[0]
            
            # NUEVA LÍNEA: Quitar también la extensión .csv
            df_debito_al_corte['file_name_clean'] = df_debito_al_corte['file_name_clean'].str.replace('.csv', '', regex=False)
            
            # Convertir fechas de corte a datetime y truncar al mes
            df_fechas_de_corte['fecha_corte_dt'] = pd.to_datetime(
                df_fechas_de_corte['Fecha corte dd-mm-yyyy'], 
                format='%d/%m/%Y', 
                errors='coerce'
            )
            df_fechas_de_corte['year_month'] = df_fechas_de_corte['fecha_corte_dt'].dt.to_period('M')
            
            # Obtener mes actual y anterior

            
            current_period = pd.Period(f"{year}-{month:02d}", freq='M')
            previous_period = current_period - 1
            
            print(f"   📅 Verificando archivos para: {current_period} y {previous_period}")
            
            # 1) Verificar que year-month está en fechas de corte
            fecha_corte_exists = current_period in df_fechas_de_corte['year_month'].values
            if not fecha_corte_exists:
                print(f"   ⚠️  Fecha de corte para {current_period} no está registrada")
                return True
            
            # 2) ✅ NUEVO: Verificar archivos esperados CON Y SIN sufijo
            expected_files_with_suffix = [f"{current_period}_debit", f"{previous_period}_debit"]
            expected_files_without_suffix = [f"{current_period}", f"{previous_period}"]
            existing_files = df_debito_al_corte['file_name_clean'].unique()
            
            print(f"   📋 Archivos esperados (con sufijo): {expected_files_with_suffix}")
            print(f"   📋 Archivos esperados (sin sufijo): {expected_files_without_suffix}")
            print(f"   📋 Archivos existentes: {list(existing_files)}")
            
            # ✅ NUEVO: Verificar si existen archivos con o sin sufijo
            missing_files = []
            found_files = []
            
            for i, period in enumerate([current_period, previous_period]):
                # Intentar encontrar con sufijo primero
                with_suffix = f"{period}_debit"
                without_suffix = f"{period}"
                
                if with_suffix in existing_files:
                    found_files.append(with_suffix)
                    print(f"   ✅ Encontrado con sufijo: {with_suffix}")
                elif without_suffix in existing_files:
                    found_files.append(without_suffix)
                    print(f"   ✅ Encontrado sin sufijo: {without_suffix}")
                else:
                    missing_files.append(f"{period} (esperado: {with_suffix} o {without_suffix})")
                    print(f"   ❌ No encontrado: {period}")
            
            # 3) Imprimir mensajes de archivos faltantes
            if missing_files:
                print(f"   ❌ Archivos de DÉBITO faltantes:")
                for missing in missing_files:
                    print(f"      - {missing}")
                necesita_descarga_debito = True
            else:
                print(f"   ✅ Todos los archivos de DÉBITO están presentes")
                print(f"   📁 Archivos encontrados: {found_files}")
                necesita_descarga_debito = False
            
            # 4) Retornar resultado
            return necesita_descarga_debito
            
        except Exception as e:
            print(f"   ❌ Error verificando archivos de débito: {e}")
            return True  # Si hay error, asumir que necesita descarga
    
    def _handle_cut_dates_file(self, cut_folder, year, index_row):
        """Maneja el archivo de fechas de corte"""
        date_file = os.path.join(cut_folder, f"{year} df_fechas_corte.pickle")
        months_name = ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
                       'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre']
        
        # Si no existe, crear dataframe base
        if not os.path.exists(date_file):
            df_fechas = pd.DataFrame({
                'Fecha corte dd-mm-yyyy': [None] * 12,
                'Mes': months_name
            })
            # Asegurar orden correcto
            df_fechas = df_fechas.set_index('Mes').loc[months_name].reset_index()
            with open(date_file, "wb") as f:
                pickle.dump(df_fechas, f)
            print(f"📁 {os.path.basename(date_file)} creado con meses predefinidos.")
        
        # Cargar archivo de fechas
        with open(date_file, "rb") as f:
            df_fechas = pickle.load(f)
        
        # Validar orden de meses; reordenar si hace falta
        if list(df_fechas['Mes']) != months_name:
            df_fechas = df_fechas.set_index('Mes').reindex(months_name).reset_index()
        
        # Obtener fecha existente
        raw_fecha = df_fechas.at[index_row, 'Fecha corte dd-mm-yyyy']
        fecha_corte = None
        estatus_fecha_corte = False
        
        if raw_fecha:
            if isinstance(raw_fecha, str):
                fecha_corte = self._parse_fecha_ddmmyyyy(raw_fecha)
            elif isinstance(raw_fecha, date):
                fecha_corte = raw_fecha
            elif isinstance(raw_fecha, datetime):
                fecha_corte = raw_fecha.date()
        
        if not fecha_corte:
            # No hay fecha válida: pedirla
            while True:
                entrada = input(f"Escribe la fecha de corte para {months_name[index_row]} en formato dd/mm/YYYY: ").strip()
                parsed = self._parse_fecha_ddmmyyyy(entrada)
                if parsed:
                    fecha_corte = parsed
                    df_fechas.at[index_row, 'Fecha corte dd-mm-yyyy'] = fecha_corte.strftime("%d/%m/%Y")
                    with open(date_file, "wb") as f:
                        pickle.dump(df_fechas, f)
                    print(f"📅 Fecha de corte para {months_name[index_row]} guardada: {fecha_corte.strftime('%d/%m/%Y')}")
                    estatus_fecha_corte = True
                    break
                else:
                    print("❌ Formato inválido. Intenta de nuevo (ejemplo: 15/08/2025).")
        else:
            # Ya había fecha válida
            estatus_fecha_corte = True
            print(f"📅 Fecha registrada existente para {months_name[index_row]}: {fecha_corte.strftime('%d/%m/%Y')}")
            # Preguntar si se quiere corregir
            choice = input("¿Quieres corregir la fecha de corte? (Si o No): ").strip().lower()
            if choice in ("si", "sí", "s"):
                while True:
                    nueva = input(f"Escribe la nueva fecha de corte para {months_name[index_row]} en formato dd/mm/YYYY: ").strip()
                    parsed = self._parse_fecha_ddmmyyyy(nueva)
                    if parsed:
                        fecha_corte = parsed
                        df_fechas.at[index_row, 'Fecha corte dd-mm-yyyy'] = fecha_corte.strftime("%d/%m/%Y")
                        with open(date_file, "wb") as f:
                            pickle.dump(df_fechas, f)
                        print(f"📅 Fecha de corte actualizada: {fecha_corte.strftime('%d/%m/%Y')}")
                        break
                    else:
                        print("❌ Formato inválido. Intenta de nuevo.")
            
    def _parse_fecha_ddmmyyyy(self, fecha_str):
        """Parsea fecha en formato dd/mm/yyyy"""
        try:
            return datetime.strptime(fecha_str, "%d/%m/%Y").date()
        except ValueError:
            return None
    
    def _load_and_display_pickles(self, cut_folder):
        """Carga y muestra información de los pickles de crédito y débito"""
        print(f"\n📊 Analizando archivos pickle en: {os.path.basename(cut_folder)}")
        
        # Archivos pickle a verificar
        pickle_files = {
            'pickle_credit_closed.pkl': 'Crédito Cerrado',
            'pickle_debit_closed.pkl': 'Débito Cerrado',
            '2025 df_fechas_corte.pickle': 'Fechas Corte 2025'
        }
        
        for pickle_filename, description in pickle_files.items():
            pickle_path = os.path.join(cut_folder, pickle_filename)
            
            if os.path.exists(pickle_path):
                try:
                    # Cargar pickle
                    with open(pickle_path, 'rb') as f:
                        df = pickle.load(f)
                        print(df.sample(5))
                    print(f"\n📋 {description} ({pickle_filename}):")
                    print(f"   📏 Dimensiones: {df.shape[0]} filas x {df.shape[1]} columnas")
                    print(f"   📄 Columnas:")
                    for i, col in enumerate(df.columns, 1):
                        print(f"      {i:2d}. {col}")
                    
                    # Mostrar algunos datos adicionales útiles
                    if not df.empty:
                        print(f"   📅 Memoria utilizada: {df.memory_usage(deep=True).sum() / 1024:.1f} KB")
                        
                        # Si hay columna de fecha, mostrar rango
                        date_columns = [col for col in df.columns if 'fecha' in col.lower() or 'date' in col.lower()]
                        if date_columns:
                            for date_col in date_columns:
                                try:
                                    min_date = df[date_col].min()
                                    max_date = df[date_col].max()
                                    print(f"   📆 Rango {date_col}: {min_date} → {max_date}")
                                except:
                                    pass
                
                except Exception as e:
                    print(f"   ❌ Error al cargar {pickle_filename}: {e}")
            
            else:
                print(f"\n📋 {description} ({pickle_filename}):")
                print(f"   ⚠️  Archivo no encontrado en {cut_folder}")
                
                # Crear archivo vacío para futuro uso
                self._create_empty_pickle(pickle_path, description)
    
    def _create_empty_pickle(self, pickle_path, description):
        """Crea un pickle vacío con estructura básica"""
        try:
            # Crear DataFrame vacío con columnas básicas
            if 'credit' in description.lower():
                empty_df = pd.DataFrame(columns=[
                    'file_name', 'fecha', 'concepto', 'cargo', 'hash'
                ])
            else:  # debit
                empty_df = pd.DataFrame(columns=[
                    'file_name', 'fecha', 'descripcion', 'monto', 'hash'
                ])
            
            with open(pickle_path, 'wb') as f:
                pickle.dump(empty_df, f)
            
            print(f"   ✅ Archivo vacío creado: {os.path.basename(pickle_path)}")
            print(f"   📄 Columnas iniciales: {list(empty_df.columns)}")
            
        except Exception as e:
            print(f"   ❌ Error al crear archivo vacío: {e}")

    def process_closed_credit_accounts(self):
        """Procesa cuentas de crédito y débito cerradas desde archivos mensuales"""
        print(f"\n🏦 Iniciando procesamiento de cuentas cerradas...")
        print(f"📁 Carpeta de trabajo: {os.path.basename(self.path_tc_closed)}")
        
        # 1. Cargar DataFrames existentes
        df_credit, df_debit = self._load_existing_pickles()
        
        # 2. Procesar archivos del repositorio mensual
        repositorio_mes = os.path.join(self.path_tc_closed, 'Repositorio por mes')
        
        if not os.path.exists(repositorio_mes):
            print(f"⚠️ No existe la carpeta: {os.path.basename(repositorio_mes)}")
            return
        
        # 3. Procesar archivos de débito
        df_debit = self._process_repository_files(df_debit, repositorio_mes, "_debit.csv", "débito")
        
        # 4. Procesar archivos de crédito
        df_credit = self._process_repository_files(df_credit, repositorio_mes, "_credit.csv", "crédito")
        
        # 5. Guardar DataFrames actualizados
        self._save_updated_pickles(df_credit, df_debit)
        
    
    def _load_existing_pickles(self):
        """Carga los DataFrames existentes de crédito y débito"""
        print(f"\n📂 Cargando DataFrames existentes...")
        
        # Cargar crédito
        credit_path = os.path.join(self.path_tc_closed, 'pickle_credit_closed.pkl')
        if os.path.exists(credit_path):
            with open(credit_path, 'rb') as f:
                df_credit = pickle.load(f)
            print(f"   ✅ Crédito cargado: {df_credit.shape[0]} registros")
        else:
            df_credit = pd.DataFrame(columns=['Fecha', 'Concepto', 'Abono', 'Cargo', 'Tarjeta', 'file_date', 'file_name'])
            print(f"   ⚠️ Archivo de crédito no existe, creando DataFrame vacío")
        
        # Cargar débito
        debit_path = os.path.join(self.path_tc_closed, 'pickle_debit_closed.pkl')
        if os.path.exists(debit_path):
            with open(debit_path, 'rb') as f:
                df_debit = pickle.load(f)
            print(f"   ✅ Débito cargado: {df_debit.shape[0]} registros")
        else:
            df_debit = pd.DataFrame(columns=['Fecha', 'Concepto', 'Cargos', 'Abonos', 'Saldos', 'file_date', 'file_name'])
            print(f"   ⚠️ Archivo de débito no existe, creando DataFrame vacío")
        
        return df_credit, df_debit
    
    def _process_repository_files(self, existing_df, repositorio_path, suffix, tipo):
        """Procesa archivos del repositorio con el sufijo especificado"""
        print(f"\n💳 Procesando archivos de {tipo}...")
        
        # Buscar archivos con el sufijo específico
        pattern = os.path.join(repositorio_path, f"*{suffix}")
        csv_files = sorted(glob(pattern))
        
        if not csv_files:
            print(f"   ⚠️ No se encontraron archivos {suffix} en {os.path.basename(repositorio_path)}")
            return existing_df
        
        print(f"   📋 Archivos encontrados: {len(csv_files)}")
        for file in csv_files:
            print(f"      - {os.path.basename(file)}")
        
        # Extraer hashes existentes para detectar duplicados
        existing_hashes = set()
        if not existing_df.empty and 'file_name' in existing_df.columns:
            existing_hashes = set(existing_df['file_name'].apply(
                lambda x: x.split("__HASH__")[-1] if "__HASH__" in str(x) else ""
            ))
        
        # Detectar duplicados entre archivos nuevos
        file_hashes = {}
        hash_to_files = defaultdict(list)
        
        for csv_file in csv_files:
            file_hash = self._hash_file_content(csv_file)
            file_hashes[csv_file] = file_hash
            hash_to_files[file_hash].append(csv_file)
        
        # Reportar duplicados internos
        for file_hash, files in hash_to_files.items():
            if len(files) > 1:
                print(f"\n⚠️ Archivos con contenido duplicado entre sí:")
                for f in files:
                    print(f"      - {os.path.basename(f)}")
        
        # Procesar cada archivo
        updated = False
        files_processed = 0
        
        for csv_file in csv_files:
            file_hash = file_hashes[csv_file]
            
            # Verificar si ya está registrado
            if file_hash in existing_hashes:
                print(f"   🔁 Ya registrado: {os.path.basename(csv_file)}")
                continue
            
            # Verificar si es duplicado y no es el primero
            if len(hash_to_files[file_hash]) > 1 and hash_to_files[file_hash][0] != csv_file:
                print(f"   🚫 Duplicado ignorado: {os.path.basename(csv_file)}")
                continue
            
            # Procesar archivo
            try:
                # Leer CSV
                df_csv = pd.read_csv(csv_file)
                
                # Validar columnas según el tipo
                required_columns = self._get_required_columns(tipo)
                if not all(col in df_csv.columns for col in required_columns):
                    print(f"   ❌ Columnas incorrectas: {os.path.basename(csv_file)}")
                    continue
                
                # Extraer fecha del archivo
                try:
                    file_date = pd.to_datetime(df_csv['Fecha'].iloc[0], dayfirst=True)
                except Exception:
                    file_date = pd.to_datetime(os.path.getmtime(csv_file), unit='s')
                
                # Preparar DataFrame
                df_processed = df_csv[required_columns].copy()
                df_processed['Fecha'] = pd.to_datetime(df_processed['Fecha'], dayfirst=True, errors='coerce')
                df_processed['file_date'] = file_date.strftime('%d/%m/%Y')
                df_processed['file_name'] = f"{os.path.basename(csv_file)}__HASH__{file_hash}"
                
                # Agregar al DataFrame principal
                existing_df = pd.concat([existing_df, df_processed], ignore_index=True)
                existing_hashes.add(file_hash)
                updated = True
                files_processed += 1
                
                print(f"   ✅ Procesado: {os.path.basename(csv_file)}")
                
            except Exception as e:
                print(f"   ❌ Error procesando {os.path.basename(csv_file)}: {e}")
                continue
        
        if updated:
            # Ordenar por fecha
            existing_df['Fecha'] = pd.to_datetime(existing_df['Fecha'], dayfirst=True, errors='coerce')
            existing_df.sort_values(by='file_date', ascending=False, inplace=True)
            print(f"   💾 {files_processed} archivos nuevos de {tipo} agregados")
        else:
            print(f"   📭 No hay archivos nuevos de {tipo} para procesar")
        
        return existing_df
    
    def _get_required_columns(self, tipo):
        """Retorna las columnas requeridas según el tipo de archivo"""
        if tipo == "crédito":
            return self.data_access.get('BANORTE_credit_headers', ['Fecha', 'Concepto', 'Abono', 'Cargo', 'Tarjeta'])
        else:  # débito
            return self.data_access.get('BANORTE_debit_headers', ['Fecha', 'Concepto', 'Cargos', 'Abonos', 'Saldos'])
    
    def _hash_file_content(self, file_path):
        """Genera hash del contenido del archivo"""
        try:
            df = pd.read_csv(file_path)
            content_str = df.to_string()
            return hashlib.md5(content_str.encode()).hexdigest()
        except Exception as e:
            print(f"⚠️ Error generando hash para {os.path.basename(file_path)}: {e}")
            return hashlib.md5(str(os.path.getmtime(file_path)).encode()).hexdigest()
    
    def _save_updated_pickles(self, df_credit, df_debit):
        """Guarda los DataFrames actualizados en archivos pickle"""
        print(f"\n💾 Guardando DataFrames actualizados...")
        
        # Guardar crédito
        credit_path = os.path.join(self.path_tc_closed, 'pickle_credit_closed.pkl')
        with open(credit_path, 'wb') as f:
            pickle.dump(df_credit, f)
        print(f"   ✅ Crédito guardado: {df_credit.shape[0]} registros")
        
        # Guardar débito
        debit_path = os.path.join(self.path_tc_closed, 'pickle_debit_closed.pkl')
        with open(debit_path, 'wb') as f:
            pickle.dump(df_debit, f)
        print(f"   ✅ Débito guardado: {df_debit.shape[0]} registros")
    
    def _export_to_excel(self, dataframes_dict):
        """Exporta múltiples DataFrames a Excel usando el diccionario"""
        print(f"\n📊 Exportando datos a Excel...")
        
        # Ruta de destino
        excel_path = os.path.expanduser("~/Downloads/Datosbancarios.xlsx")
        
        # ✅ MAPEO: Convertir keys del diccionario a nombres de hojas
        sheet_mapping = {
            'credit': 'Crédito_corriente',
            'debit': 'Débito_corriente', 
            'mfi': 'MFI_corriente',
            'credit_closed': 'Crédito_cerrado',
            'debit_closed': 'Débito_cerrado'
        }
        
        try:
            # Usar ExcelWriter para manejar múltiples hojas
            with pd.ExcelWriter(excel_path, engine='openpyxl', mode='w') as writer:
                
                sheets_created = 0
                
                for data_key, df in dataframes_dict.items():
                    # Obtener nombre de hoja
                    sheet_name = sheet_mapping.get(data_key, data_key.replace('_', ' ').title())
                    
                    if df is not None and not df.empty:
                        # Limpiar nombres de archivos para mejor legibilidad
                        df_export = df.copy()
                        
                        # Si tiene columna file_name, limpiarla
                        if 'file_name' in df_export.columns:
                            df_export['file_name'] = df_export['file_name'].str.split('__HASH__').str[0]
                        if 'file_date' in df_export.columns:
                            df_export.drop('file_date', axis=1, inplace=True)
                        if 'filename' in df_export.columns:
                            df_export.rename(columns={'filename': 'file_name'}, inplace=True)
                        # Exportar a Excel
                        df_export.to_excel(writer, sheet_name=sheet_name, index=False)
                        print(f"   ✅ Hoja '{sheet_name}': {df.shape[0]} registros")
                        sheets_created += 1
                        
                    else:
                        # Crear hoja vacía
                        pd.DataFrame().to_excel(writer, sheet_name=sheet_name, index=False)
                        print(f"   ⚠️ Hoja '{sheet_name}': vacía")
                        sheets_created += 1
            
            print(f"\n🎉 Archivo Excel exportado exitosamente:")
            print(f"   📁 Ubicación: {excel_path}")
            print(f"   📋 Hojas creadas: {sheets_created}")
            
            # Abrir carpeta de destino
            try:
                from utils.helpers import open_folder
                open_folder(os.path.dirname(excel_path))
            except:
                print(f"   💡 Puedes encontrar el archivo en: {excel_path}")
        
        except Exception as e:
            print(f"   ❌ Error exportando a Excel: {e}")

        if 'credit' in dataframes_dict and dataframes_dict['credit'] is not None:
            with open(os.path.join(self.path_tc_closed, 'credit_current.pkl'), 'wb') as f:
                pickle.dump(dataframes_dict['credit'], f)
            print(f"   💾 credit → credit_current.pkl")
        
        if 'debit' in dataframes_dict and dataframes_dict['debit'] is not None:
            with open(os.path.join(self.path_tc_closed, 'debit_current.pkl'), 'wb') as f:
                pickle.dump(dataframes_dict['debit'], f)
            print(f"   💾 debit → debit_current.pkl")
                        
    def _fix_date_format(self, dataframes_dict):
        """Convierte columnas 'Fecha' de string a datetime en todos los DataFrames"""
        print(f"\n📅 Normalizando fechas...")
        
        for key, df in dataframes_dict.items():
            if df is not None and not df.empty and 'Fecha' in df.columns:
                # Verificar si ya es datetime
                if not pd.api.types.is_datetime64_any_dtype(df['Fecha']):
                    print(f"   🔄 {key}: Convirtiendo Fecha de string → datetime")
                    df['Fecha'] = pd.to_datetime(df['Fecha'], dayfirst=True, errors='coerce')
                else:
                    print(f"   ✅ {key}: Fecha ya en formato datetime")
        
        return dataframes_dict