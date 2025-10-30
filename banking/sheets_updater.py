import os
import pandas as pd
import numpy as np
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from utils.helpers import Helper  # Import the Helper class


class SheetsUpdater:
    @staticmethod
    def _get_gspread_client(working_folder):
        """Devuelve una conexión persistente a Google Sheets"""
        import gspread
        from oauth2client.service_account import ServiceAccountCredentials
        if not hasattr(SheetsUpdater, "_cached_client"):
            scope = [
                'https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive'
            ]
            json_path = os.path.join(working_folder, 'armjorgeSheets.json')
            creds = ServiceAccountCredentials.from_json_keyfile_name(json_path, scope)
            SheetsUpdater._cached_client = gspread.authorize(creds)
        return SheetsUpdater._cached_client

    def __init__(self, working_folder, data_access):
        self.working_folder = working_folder
        self.data_access = data_access
        self.spreadsheet = None
        self.helper = Helper()
        # Inicializar conexión
        self._setup_connection()
    
    def _setup_connection(self):
        """Configura la conexión con Google Sheets"""
        try:
            # Configurar permisos
            scope = [
                'https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive'
            ]
            
            # Cargar credenciales
            json_path = os.path.join(self.working_folder, 'armjorgeSheets.json')
            creds = ServiceAccountCredentials.from_json_keyfile_name(json_path, scope)
            
            # Autorizar cliente
            client = self._get_gspread_client(self.working_folder)
            url = self.data_access['url_google_sheet']
            self.spreadsheet = client.open_by_url(url)
            
            print("✅ Conexión con Google Sheets establecida")
            
        except Exception as e:
            print(f"❌ Error al conectar con Google Sheets: {e}")
            self.spreadsheet = None
    
    def update_sheet(self, sheet_name, dataframe):
        """Actualiza una hoja específica con un DataFrame"""
        print(f"update_sheet llamado con key: {sheet_name} y df de tamaño: {dataframe.shape}")

        if not self.spreadsheet:
            print("❌ No hay conexión con Google Sheets")
            return False
        
        try:
            # Obtener hoja de trabajo
            worksheet = self.spreadsheet.worksheet(sheet_name)
            worksheet.clear()
            
            # Procesar fechas si existen
            df_processed = self._process_dataframe_for_sheets(dataframe)
            
            # Mostrar información de debug para fechas
            #self._debug_date_conversion(sheet_name, dataframe, df_processed)
            
            # Preparar datos para subida
            values = [df_processed.columns.tolist()] + df_processed.astype(str).values.tolist()
            
            # Subir datos
            self.spreadsheet.values_update(
                f"{sheet_name}!A1",
                params={'valueInputOption': 'USER_ENTERED'},
                body={'values': values}
            )
            
            print(f"✅ Hoja '{sheet_name}' actualizada exitosamente")
            return True
            
        except Exception as e:
            print(f"❌ Error al actualizar hoja '{sheet_name}': {e}")
            return False
    
    def _process_dataframe_for_sheets(self, df):
        """Procesa el DataFrame para compatibilidad con Google Sheets"""
        # Limpiar valores infinitos y NaN
        df = df.replace([np.inf, -np.inf], np.nan)
        df = df.fillna("")
        
        # Procesar fechas
 
        if 'fecha' in df.columns:
            df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce')
            df['fecha'] = df['fecha'].dt.strftime('%Y-%m-%d')  # Cambia el formato según sea necesario
        
        return df
    
    def _debug_date_conversion(self, sheet_name, original_df, processed_df):
        """Muestra información de debug para la conversión de fechas"""
        if 'Fecha' not in original_df.columns:
            return
        
        print(f"\tCambiando el tipo de la columna Fecha a date de la hoja {sheet_name}\n")
        print(f"\t\tFechas originales (primeras 3): {original_df['Fecha'].head(3).tolist()}")
        print(f"\t\tFechas convertidas (primeras 3): {processed_df['Fecha'].head(3).tolist()}")
    
    def update_multiple_sheets(self, dataframes_dict):
        print("📂 Cargando DataFrames y actualizando Google Sheets...")
        
        loaded_dataframes = dataframes_dict
    
        for data_type, df in loaded_dataframes.items():
            if df is not None and not df.empty:
                sheet_name = data_type
                print(f"📋 Subiendo {data_type} → {sheet_name} ({df.shape[0]} registros)")
                self.update_sheet(sheet_name, df)
            else:
                sheet_name = data_type
                print(f"⚠️ No hay datos para actualizar: {data_type} → {sheet_name}")