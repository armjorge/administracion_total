import os
from datetime import date
from .file_processor import FileProcessor
from .web_automation import WebAutomation
from .sheets_updater import SheetsUpdater
from utils.helpers import message_print, create_directory_if_not_exists
import sys

class BankingManager:
    def __init__(self, working_folder, data_access, folder_root):
        self.working_folder = working_folder
        self.data_access = data_access
        self.folder_root = folder_root
        
        # Configurar rutas
        self.download_folder = os.path.join(working_folder, "Temporal Downloads")
        self.path_tc_closed = os.path.join(working_folder, 'TC al corte')
        self.path_debit_closed = os.path.join(working_folder, 'Débito al mes')
        
        # Crear directorios necesarios
        create_directory_if_not_exists([
            self.download_folder, 
            self.path_tc_closed, 
            self.path_debit_closed
        ])
        
        # Inicializar componentes
        self.file_processor = FileProcessor(working_folder, data_access)
        self.web_automation = WebAutomation(self.download_folder, folder_root)
        self.sheets_updater = SheetsUpdater(working_folder, data_access)
        
        # Cargar módulos externos
        self._load_external_modules()
    
    def _load_external_modules(self):
        """Carga módulos externos necesarios"""
        libs_dir = os.path.join(self.folder_root, "Librería")
        sys.path.insert(0, libs_dir)
        
        try:
            from credit_closed import process_closed_credit_accounts, export_pickle
            self.process_closed_credit = process_closed_credit_accounts
            self.export_pickle = export_pickle
        except ImportError:
            print("⚠️ No se pudieron cargar los módulos de crédito cerrado")
    
    def run_banking_menu(self):
        """Ejecuta el menú principal del sistema bancario"""
        # Verificar archivos de corte existentes
        self._check_existing_cuts()
        
        while True:
            choice = input(f"""{message_print('¿Qué deseas hacer?')}
        1. Descargar, renombrar y mover archivos CSV corrientes
        2. Procesar archivos CSV posteriores al corte
        3. Procesar archivos CSV al mes corte
        0. Salir
        Elige una opción: """)

            if choice == "1":
                self._process_current_files()
            elif choice == "2":
                self._process_post_cut_files()
            elif choice == "3":
                self._process_monthly_cut_files()
            elif choice == "0":
                print("👋 ¡Hasta luego!")
                break
            else:
                print("⚠️ Opción no válida. Por favor elige 1, 2, 3 o 0.\n")
    
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
    
    def _process_post_cut_files(self):
        """Procesa archivos posteriores al corte (Opción 2)"""
        print("📦 Procesando archivos CSV después del corte...")
        
        # Procesar archivos existentes
        dataframes = self.file_processor.process_post_cut_files()
        
        # Actualizar Google Sheets
        if dataframes:
            self.sheets_updater.update_multiple_sheets(dataframes)
    
    def _process_monthly_cut_files(self):
        """Procesa archivos de corte mensual (Opción 3)"""
        print("📅 Procesando archivos de corte mensual...")
        
        # Verificar fecha de corte y procesar si es necesario
        fecha_corte, necesita_descarga = self._check_cut_date()
        
        if necesita_descarga:
            # Descargar archivo de corte
            success = self.web_automation.execute_download_session(
                self.data_access, 
                mode="monthly_cut"
            )
            
            if success:
                # Procesar archivo de corte
                self.file_processor.process_monthly_cut(self.path_tc_closed)
        
        # Procesar débito mensual
        self._process_monthly_debit()
    
    def _check_existing_cuts(self):
        """Verifica el estado de los archivos de corte existentes"""
        if hasattr(self, 'process_closed_credit'):
            # Usar la función original adaptada
            pass
    
    def _check_cut_date(self):
        """Verifica la fecha de corte y determina si se necesita descarga"""
        # Implementar lógica de fechas_corte_tarjeta
        today = date.today()
        # ... lógica de fecha de corte ...
        return date.today(), True
    
    def _process_monthly_debit(self):
        """Procesa archivos de débito mensual"""
        print("💳 Procesando débito mensual...")
        # Implementar lógica específica de débito