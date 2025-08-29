from utils.helpers import Helper
from .downloader_workflow import DownloaderWorkflow

class BankingManager:
    def __init__(self, working_folder, data_access, folder_root, path_tc_closed,corriente_temporal_downloads, fechas_corte, today, PICKLE_DEBITO_CORRIENTE, PICKLE_CREDITO_CORRIENTE, PICKLE_DEBIT_CLOSED, PICKLE_CREDIT_CLOSED):
        self.working_folder = working_folder
        self.data_access = data_access
        self.folder_root = folder_root
        self.path_tc_closed = path_tc_closed
        self.corriente_temporal_downloads = corriente_temporal_downloads
        self.fechas_corte = fechas_corte
        self.today = today
        self.pickle_debito_corriente = PICKLE_DEBITO_CORRIENTE
        self.pickle_credito_corriente= PICKLE_CREDITO_CORRIENTE
        self.pickle_debito_cerrado = PICKLE_DEBIT_CLOSED
        self.pickle_credito_cerrado = PICKLE_CREDIT_CLOSED
        # Pass path_tc_closed and fechas_corte to DownloaderWorkflow
        self.descargador = DownloaderWorkflow(
            self.working_folder, self.data_access, self.folder_root, self.path_tc_closed, self.corriente_temporal_downloads,
            self.fechas_corte, self.today, self.pickle_debito_cerrado, self.pickle_credito_cerrado, 
        )

    def run_banking_menu(self):
        print(Helper.message_print("Bienvenido al menú bancario"))
        
        """Run the banking menu."""
        while True:
            choice = input(f"""{Helper.message_print('¿Qué deseas hacer?')}
        1. Descargar
        0. Salir
        Elige una opción: """).strip()

            if choice == "1":
                # Call the downloader workflow
                self.descargador.descargador_workflow()
            elif choice == "0":
                print("👋 ¡Hasta luego!")
                return
            else:
                print("\n⚠️ Elige una opción válida (1 o 0). Inténtalo de nuevo.\n")