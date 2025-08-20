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
        elif mode == "postcut":
            self._process_postcut_mode(classified_files, download_folder)
    
    def _process_normal_mode(self, classified_files):
        """Procesa archivos en modo normal (por día y tipo)"""
        # Agrupar por fecha y tipo
        grouped_files = defaultdict(list)
        for file_path, info in classified_files.items():
            key = (info['year'], info['month'], info['day'], info['type'])
            grouped_files[key].append(file_path)
        
        # Procesar cada grupo
        for (year, month, day, typ), file_list in grouped_files.items():
            self._merge_and_save_group(year, month, day, typ, file_list, mode="normal")
        
        # Limpiar archivos originales
        self._cleanup_original_files(classified_files.keys())
    
    def _process_postcut_mode(self, classified_files, download_folder):
        """Procesa archivos en modo post-corte (para archivos de corte mensual)"""
        print("📅 Procesando archivos en modo post-corte...")
        
        # Agrupar por tipo (sin considerar día específico)
        grouped_files = defaultdict(list)
        for file_path, info in classified_files.items():
            key = (info['year'], info['month'], info['type'])
            grouped_files[key].append(file_path)
        repositorio_mes = os.path.join(self.working_folder, 'Meses cerrados', 'Repositorio por mes')
        # Procesar cada grupo
        for (year, month, typ), file_list in grouped_files.items():
            self._merge_and_save_group(year, month, None, typ, file_list,
                                     mode="postcut", target_folder=repositorio_mes)

        # NO limpiar archivos originales en modo postcut
        print("ℹ️ Archivos originales conservados en modo post-corte")
        self._cleanup_original_files(classified_files.keys())

    def _merge_and_save_group(self, year, month, day, typ, file_list, mode="normal", target_folder=None):
        """Fusiona archivos del mismo grupo y los guarda"""
        if mode == "postcut":
            print(f"📁 Fusionando archivos de corte: {typ} {year}-{month:02d}")
        else:
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
            if mode == "postcut":
                print(f"⚠️ No se cargaron archivos válidos para {year}-{month:02d} ({typ})")
            else:
                print(f"⚠️ No se cargaron archivos válidos para {year}-{month:02d}-{day:02d} ({typ})")
            return
        
        # Fusionar y guardar
        final_df = pd.concat(dataframes, ignore_index=True)
        self._save_merged_file(final_df, year, month, day, typ, mode, target_folder)
    
    def _save_merged_file(self, df, year, month, day, typ, mode="normal", target_folder=None):
        """Guarda el archivo fusionado en la carpeta correspondiente"""
        from utils.helpers import create_directory_if_not_exists
        
        if mode == "postcut":
            # Modo post-corte: guardar en la carpeta de corte específica
            csv_folder = target_folder
            csv_filename = f"{year}-{month:02d}_{typ}.csv"
            
        else:
            # Modo normal: crear carpeta por fecha
            csv_folder = os.path.join(self.working_folder, f"{year}-{month:02d}")
            csv_filename = f"{year}-{month:02d}-{day:02d}_{typ}.csv"
        
        # Crear carpeta de destino si no existe
        create_directory_if_not_exists(csv_folder)
        
        # Guardar archivo
        csv_path = os.path.join(csv_folder, csv_filename)
        df.to_csv(csv_path, index=False)
        print(f"✅ Archivo guardado: {csv_filename} en {os.path.basename(csv_folder)}")
    
    def process_post_cut_files(self, mode="precut"):
        """Procesa archivos posteriores al corte"""
        if mode == "precut":
            print(message_print("Procesando CSVs posteriores al corte"))
        else:
            print(message_print("Procesando CSVs en modo post-corte"))
        
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
        

    def classify_csv_file(self, file_path):
        """Clasifica un archivo CSV según su contenido y nombre"""
        try:
            basename = os.path.basename(file_path)
            
            # Leer el archivo para determinar el tipo por encabezados
            df = pd.read_csv(file_path)
            headers = list(df.columns)
            
            # Determinar tipo basado en encabezados
            file_type = self._determine_file_type(headers)
            
            if not file_type:
                print(f"⚠️ No se pudo clasificar el archivo: {basename}")
                return None
            
            # Extraer fecha del nombre del archivo o usar fecha actual
            year, month, day = self._extract_date_from_filename(basename)
            
            return {
                'type': file_type,
                'year': year,
                'month': month,
                'day': day,
                'headers': headers,
                'basename': basename
            }
            
        except Exception as e:
            print(f"❌ Error al clasificar archivo '{os.path.basename(file_path)}': {e}")
            return None
    
    def _determine_file_type(self, headers):
        """Determina el tipo de archivo basado en los encabezados"""
        # Convertir a minúsculas para comparación
        headers_lower = [h.lower() for h in headers]
        
        # Verificar si coincide con crédito
        credit_match = self._headers_match(headers_lower, self.headers_credit)
        if credit_match:
            return 'credit'
        
        # Verificar si coincide con débito
        debit_match = self._headers_match(headers_lower, self.headers_debit)
        if debit_match:
            return 'debit'
        
        # Verificar si coincide con MFI (month free interest)
        mfi_match = self._headers_match(headers_lower, self.headers_mfi)
        if mfi_match:
            return 'stdMFI'
        
        # Si no coincide con ninguno conocido, intentar clasificar por contenido común
        if any(keyword in ' '.join(headers_lower) for keyword in ['cargo', 'abono', 'credito', 'credit']):
            return 'credit'
        elif any(keyword in ' '.join(headers_lower) for keyword in ['debito', 'debit', 'saldo']):
            return 'debit'
        elif any(keyword in ' '.join(headers_lower) for keyword in ['mfi', 'interest', 'interes']):
            return 'stdMFI'
        
        return None
    
    def _headers_match(self, file_headers, expected_headers):
        """Verifica si los encabezados del archivo coinciden con los esperados"""
        if not expected_headers:
            return False
        
        expected_lower = [h.lower() for h in expected_headers]
        
        # Verificar coincidencia exacta
        if file_headers == expected_lower:
            return True
        
        # Verificar si al menos el 80% de los encabezados esperados están presentes
        matches = sum(1 for header in expected_lower if header in file_headers)
        match_percentage = matches / len(expected_lower) if expected_lower else 0
        
        return match_percentage >= 0.8
    
    def _extract_date_from_filename(self, filename):
        """Extrae fecha del nombre de archivo o usa fecha actual"""
        from datetime import date
        
        today = date.today()
        
        try:
            # Intentar extraer fecha del formato: prefijo_YYYY-MM-DD_sufijo.csv
            # o YYYY-MM-DD_tipo.csv
            parts = filename.replace('.csv', '').split('_')
            
            for part in parts:
                if len(part) == 10 and part.count('-') == 2:  # Formato YYYY-MM-DD
                    year, month, day = map(int, part.split('-'))
                    return year, month, day
            
            # Si no se encuentra fecha en el nombre, usar fecha actual
            return today.year, today.month, today.day
            
        except (ValueError, IndexError):
            # Si falla la extracción, usar fecha actual
            return today.year, today.month, today.day
    
    def _read_csv_safe(self, file_path):
        """Lee un archivo CSV de forma segura con diferentes encodings"""
        encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
        
        for encoding in encodings:
            try:
                df = pd.read_csv(file_path, encoding=encoding)
                return df
            except UnicodeDecodeError:
                continue
            except Exception as e:
                print(f"❌ Error al leer {os.path.basename(file_path)} con {encoding}: {e}")
                continue
        
        # Si todos los encodings fallan, intentar con el encoding por defecto
        try:
            df = pd.read_csv(file_path)
            return df
        except Exception as e:
            print(f"❌ Error crítico al leer {os.path.basename(file_path)}: {e}")
            raise
    
    def _cleanup_original_files(self, file_paths):
        """Elimina archivos originales después del procesamiento"""
        for file_path in file_paths:
            try:
                os.remove(file_path)
                print(f"🗑️ Archivo original eliminado: {os.path.basename(file_path)}")
            except Exception as e:
                print(f"⚠️ No se pudo eliminar {os.path.basename(file_path)}: {e}")
    
