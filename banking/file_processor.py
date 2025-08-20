import os
import pandas as pd
from datetime import datetime
from glob import glob
from collections import defaultdict
from utils.helpers import message_print

class FileProcessor:
    def __init__(self, working_folder, data_access):
        self.working_folder = working_folder
        self.data_access = data_access
        self.loaded_hashes = set()
        
        # Cargar encabezados esperados
        self.headers_credit = data_access.get('BANORTE_credit_headers', [])
        self.headers_debit = data_access.get('BANORTE_debit_headers', [])
        self.headers_mfi = data_access.get('BANORTE_month_free_headers', [])
    
    def classify_csv_file(self, file_path):
        """Clasifica un archivo CSV según sus encabezados"""
        file_info = {}
        
        # Extraer fecha de modificación
        timestamp = os.path.getmtime(file_path)
        dt = datetime.fromtimestamp(timestamp)
        file_info.update({
            'year': dt.year,
            'month': dt.month,
            'day': dt.day,
            'path': file_path
        })
        
        try:
            # Leer encabezados
            df = self._read_csv_safe(file_path, nrows=1)
            file_headers = list(df.columns)
            
            # Clasificar por tipo
            if file_headers == self.headers_credit:
                file_info['type'] = 'credit'
            elif file_headers == self.headers_debit:
                file_info['type'] = 'debit'
            elif file_headers == self.headers_mfi:
                file_info['type'] = 'MFI'
            else:
                print(f"⚠️ Archivo '{os.path.basename(file_path)}' no coincide con ninguna categoría.")
                return None
                
        except Exception as e:
            print(f"❌ Error al leer '{os.path.basename(file_path)}': {e}")
            return None
        
        return file_info
    
    def process_downloaded_files(self, download_folder, mode="normal"):
        """Procesa archivos descargados según el modo especificado"""
        print(message_print("Procesando archivos descargados"))
        
        # Buscar archivos CSV
        csv_files = glob(os.path.join(download_folder, "*.csv"))
        
        if not csv_files:
            print("⚠️ No se encontraron archivos CSV para procesar")
            return
        
        # Clasificar archivos
        classified_files = {}
        for file_path in csv_files:
            file_info = self.classify_csv_file(file_path)
            if file_info:
                classified_files[file_path] = file_info
        
        if mode == "normal":
            self._process_normal_mode(classified_files)
        elif mode == "monthly_cut":
            self._process_monthly_cut_mode(classified_files, download_folder)
    
    def _process_normal_mode(self, classified_files):
        """Procesa archivos en modo normal (por día y tipo)"""
        # Agrupar por fecha y tipo
        grouped_files = defaultdict(list)
        for file_path, info in classified_files.items():
            key = (info['year'], info['month'], info['day'], info['type'])
            grouped_files[key].append(file_path)
        
        # Procesar cada grupo
        for (year, month, day, typ), file_list in grouped_files.items():
            self._merge_and_save_group(year, month, day, typ, file_list)
        
        # Limpiar archivos originales
        self._cleanup_original_files(classified_files.keys())
    
    def _merge_and_save_group(self, year, month, day, typ, file_list):
        """Fusiona archivos del mismo grupo y los guarda"""
        print(f"📁 Fusionando archivos: {typ} {year}-{month:02d}-{day:02d}")
        
        dataframes = []
        for file_path in file_list:
            try:
                df = self._read_csv_safe(file_path)
                
                # Verificar duplicados
                df_hash = pd.util.hash_pandas_object(df, index=True).sum()
                if df_hash in self.loaded_hashes:
                    print(f"⚠️ Archivo duplicado ignorado: {os.path.basename(file_path)}")
                    continue
                
                self.loaded_hashes.add(df_hash)
                dataframes.append(df)
                
            except Exception as e:
                print(f"❌ Error al procesar '{os.path.basename(file_path)}': {e}")
        
        if not dataframes:
            print(f"⚠️ No se cargaron archivos válidos para {year}-{month:02d}-{day:02d} ({typ})")
            return
        
        # Fusionar y guardar
        final_df = pd.concat(dataframes, ignore_index=True)
        self._save_merged_file(final_df, year, month, day, typ)
    
    def _save_merged_file(self, df, year, month, day, typ):
        """Guarda el archivo fusionado en la carpeta correspondiente"""
        from utils.helpers import create_directory_if_not_exists
        
        # Crear carpeta de destino
        csv_folder = os.path.join(self.working_folder, f"{year}-{month:02d}")
        create_directory_if_not_exists(csv_folder)
        
        # Guardar archivo
        csv_path = os.path.join(csv_folder, f"{year}-{month:02d}-{day:02d}_{typ}.csv")
        df.to_csv(csv_path, index=False)
        print(f"✅ Archivo guardado: {os.path.basename(csv_path)}")
    
    def _read_csv_safe(self, file_path, **kwargs):
        """Lee un CSV con manejo seguro de encoding"""
        try:
            return pd.read_csv(file_path, encoding='utf-8', **kwargs)
        except UnicodeDecodeError:
            return pd.read_csv(file_path, encoding='latin1', **kwargs)
    
    def _cleanup_original_files(self, file_paths):
        """Elimina archivos originales después del procesamiento"""
        for file_path in file_paths:
            try:
                os.remove(file_path)
                print(f"🗑️ Eliminado: {os.path.basename(file_path)}")
            except Exception as e:
                print(f"⚠️ No se pudo eliminar {os.path.basename(file_path)}: {e}")
    
    def process_post_cut_files(self):
        """Procesa archivos posteriores al corte"""
        print(message_print("Procesando CSVs posteriores al corte"))
        
        # Obtener archivos más recientes
        latest_files = self._get_latest_files()
        
        # Cargar DataFrames
        dataframes = {}
        for file_type, file_path in latest_files.items():
            if file_path:
                df = self._read_csv_safe(file_path)
                df["filename"] = os.path.basename(file_path)
                dataframes[file_type] = df
        
        return dataframes
    
    def _get_latest_files(self):
        """Obtiene los archivos más recientes de cada tipo"""
        from datetime import datetime, date
        
        today = datetime.now().date()
        monthly_folder = os.path.join(
            self.working_folder, 
            f"{today.year:04d}-{today.month:02d}"
        )
        
        if not os.path.isdir(monthly_folder):
            from utils.helpers import create_directory_if_not_exists
            create_directory_if_not_exists(monthly_folder)
        
        # Buscar archivos más recientes por tipo
        groups = {
            "_credit.csv": "credit",
            "_debit.csv": "debit", 
            "_stdMFI.csv": "mfi"
        }
        
        latest_files = {}
        for suffix, key in groups.items():
            files = [f for f in os.listdir(monthly_folder) if f.endswith(suffix)]
            if files:
                # Ordenar por fecha del nombre del archivo
                files.sort(key=lambda fn: self._parse_date_from_filename(fn), reverse=True)
                latest_files[key] = os.path.join(monthly_folder, files[0])
                
                # Reportar retraso si existe
                file_date = self._parse_date_from_filename(files[0])
                delay = (today - file_date).days
                if delay > 0:
                    print(f"⚠️ Archivo {suffix} tiene {delay} día(s) de retraso")
            else:
                print(f"⚠️ No existe archivo para la categoría {suffix}")
                latest_files[key] = None
        
        return latest_files
    
    def _parse_date_from_filename(self, filename):
        """Extrae fecha del nombre de archivo formato YYYY-MM-DD_tipo.csv"""
        try:
            basename = os.path.basename(filename)
            date_str = basename.split("_")[0]
            return datetime.strptime(date_str, "%Y-%m-%d").date()
        except (ValueError, IndexError):
            return date.min