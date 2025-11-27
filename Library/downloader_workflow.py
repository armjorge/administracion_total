import pandas as pd
import os
from datetime import date, datetime
import yaml
from dotenv import load_dotenv
from sqlalchemy import create_engine
from dateutil.relativedelta import relativedelta
import os, glob


try:
    from Library.web_automation import WebAutomation
except ModuleNotFoundError:
    # fallback if running inside the Library folder
    from web_automation import WebAutomation
from urllib.parse import urlparse
import psycopg2
from colorama import Fore, Style, init


class DownloaderWorkflow:
    def __init__(self, working_folder, data_access):
        self.today = date.today()
        self.working_folder = working_folder
        self.data_access = data_access
        self.current_folder = os.path.join(self.working_folder,'Info Bancaria', f'{self.today.year}-{self.today.month:02d}')
        self.closed_folder = os.path.join(self.working_folder,'Info Bancaria', 'Meses cerrados', 'Repositorio por mes')
        self.temporal_downloads = os.path.join(self.working_folder, 'Info Bancaria', 'Descargas temporales')
        self.web_automation = WebAutomation(self.working_folder, self.data_access)
        print("Arhivos CSV", glob.glob(os.path.join(self.temporal_downloads, "*.csv")))

    def download_missing_files(self):
        # Get current cutoff_days 
        connexion = self.sql_conexion(self.data_access['sql_workflow']).connect()
        if connexion is None:
            print("❌ No se pudo establecer conexión con SQL Server.")
            return False
        # 2️⃣ Intentar leer tabla de cuentas
        try:
            actualiza = "SELECT banorte_load.refresh_account_cutoffs();"
            cutofss = "SELECT * FROM banorte_load.account_cutoffs"

            # Periodos cerrados
            period_debit = "SELECT DISTINCT period, cuenta FROM banorte_load.debito_cerrado;"
            period_credit = "SELECT DISTINCT period, cuenta FROM banorte_load.credito_cerrado;"
            self.df_account_cutoffs = pd.read_sql(cutofss, connexion)
            self.periods_debit = pd.read_sql(period_debit, connexion)
            self.periods_credit = pd.read_sql(period_credit, connexion)

            self.df_account_cutoffs.sort_values(['account_number', 'type', 'cutoff_period'], ascending=[True, True, False], inplace=True)
            top_two = (
                self.df_account_cutoffs
                .groupby(['account_number', 'type'])
                .head(2)
                .reset_index(drop=True)
            )

            # 4️⃣ Construir lista de tuplas (period, account) para available_files
            #    Si vienen en dataframes separados (periods_debit y periods_credit)
            # convertir a tuplas directamente
            available_debit = list(self.periods_debit.apply(lambda x: (str(x['period']), str(x['cuenta'])), axis=1))
            available_credit = list(self.periods_credit.apply(lambda x: (str(x['period']), str(x['cuenta'])), axis=1))
            available_files = set(available_debit + available_credit)
            # 5️⃣ Detectar cuáles de los top_two no están en available_files
            closed_needed = []
            for _, row in top_two.iterrows():
                pair = (row['cutoff_period'], row['account_number'])
                if pair not in available_files:
                    closed_needed.append({
                        'type': row['type'],
                        'period': row['cutoff_period'],
                        'account': row['account_number'],
                        'status': 'closed'
                    })


            # Construir lista needed_open
            needed_open = []
            current_period = f"{self.today.year}-{self.today.month:02d}"
            next_period_date = self.today + relativedelta(months=1)
            next_period = f"{next_period_date.year}-{next_period_date.month:02d}"    
            today = pd.Timestamp(self.today)
            # dataframe cuentas
            accounts_query = "SELECT * FROM banorte_load.accounts"
            df_accounts = pd.read_sql(accounts_query, connexion)
            print(df_accounts.head())

            debit_accounts = df_accounts[df_accounts['type'] == 'debit']['account_number'].astype(str).tolist()
            credit_accounts = df_accounts[df_accounts['type'] == 'credit']['account_number'].astype(str).tolist()

            needed_open = []

            for item in debit_accounts:
                needed_open.append({
                    'type': 'debit',
                    'period': today.date().strftime('%Y-%m-%d'),
                    'account': str(item),
                    'status': 'open'
                })

            for item in credit_accounts:
                needed_open.append({
                    'type': 'credit',
                    'period': today.date().strftime('%Y-%m-%d'),
                    'account': str(item),
                    'status': 'open'
                })

            # Procesar tablas abiertas
            query_debit = "SELECT cuenta, MAX(file_date) AS max_date FROM banorte_load.debito_abierto GROUP BY cuenta;"
            query_credit = "SELECT cuenta, MAX(file_date) AS max_date FROM banorte_load.credito_abierto GROUP BY cuenta;"

            df_open_debit = pd.read_sql(query_debit, connexion)
            df_open_credit = pd.read_sql(query_credit, connexion)

            # ⚠️ Corregido: debe ser lista, no dict
            files_and_dates = []

            for _, row in df_open_debit.iterrows():
                files_and_dates.append({
                    'type': 'debit',
                    'period': row['max_date'].strftime('%Y-%m-%d'),
                    'account': str(row['cuenta']),
                    'status': 'open'
                })

            for _, row in df_open_credit.iterrows():
                files_and_dates.append({
                    'type': 'credit',
                    'period': row['max_date'].strftime('%Y-%m-%d'),
                    'account': str(row['cuenta']),
                    'status': 'open'
                })
            print(f"Archivos teóricos: {needed_open}")
            print(f"Archivos presentes en el servidor: {files_and_dates}")
            needed_open = [
                entry for entry in needed_open
                if entry not in files_and_dates
            ]

            print("Registro retirado:", needed_open)
            # Combinar
            final_files = closed_needed + needed_open            
            
            if final_files:
                for file in final_files:
                    print(f"⬇️ Necesita descargar: {file['status']} - {file['account']} - {file['period']}")                             
                self.web_automation.execute_download_session(final_files)
            # Agregar: envíar query de actualización de cutoffs
            # Que esta función se corra si cron_query no está actualizado o de alguna manera si no existe registro de cuándo corrimos la función. 
            self.execute_cron_query()
            connexion.commit()
            connexion.close()
            print("Todos los archivos están actualizados, buen trabajo 👍")
            print("✅ Proceso de descarga completado.")

        except Exception as e:
            error_msg = str(e)

    def execute_cron_query(self):
        print("\t🔄 Ejecutando actualización de cutoffs en la base de datos...")
        stmt = 'SELECT banorte_load.refresh_account_cutoffs();'
        db_url = self.data_access["sql_workflow"]
        parsed = urlparse(db_url)
        conn_params = {
            "dbname": parsed.path.lstrip("/"),
            "user": parsed.username,
            "password": parsed.password,
            "host": parsed.hostname,
            "port": parsed.port or 5432,
        }

        # Conexión directa psycopg2
        try:
            raw_conn = psycopg2.connect(**conn_params)
            raw_conn.autocommit = True
            cur = raw_conn.cursor()
            print(f"{Fore.GREEN}✅ Direct PostgreSQL connection established.{Style.RESET_ALL}")
        except Exception as e:
            print(f"❌ Error creating raw PostgreSQL connection: {e}")
            return False
        try:
            cur.execute(stmt)
            print(f"{Fore.GREEN}✅ Cutoffs updated successfully.{Style.RESET_ALL}")
        except Exception as e:
            print(f"{Fore.RED}❌ Error executing statement: {e}{Style.RESET_ALL}")

    def sql_conexion(self, sql_url):
        try:
            engine = create_engine(sql_url)
            return engine
        except Exception as e:
            print(f"❌ Error connecting to database: {e}")
            return None

        


if __name__ == "__main__":
    # 1️⃣ Obtiene la ruta absoluta al archivo .env (un nivel arriba del archivo actual)
    env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '.env'))
    dot_env_name = "MAIN_PATH"

    # 2️⃣ Carga variables del .env si existe
    if os.path.exists(env_path):
        load_dotenv(dotenv_path=env_path)
        # 3️⃣ Obtiene la variable MAIN_PATH
        working_folder = os.getenv(dot_env_name)
        if not working_folder:
            raise ValueError(f"La variable {dot_env_name} no está definida en {env_path}")
        # 4️⃣ Construye la ruta absoluta hacia config.yaml dentro del MAIN_PATH
        yaml_path = os.path.join(working_folder, 'config.yaml')
        if not os.path.exists(yaml_path):
            raise FileNotFoundError(f"No se encontró config.yaml en {yaml_path}")
        # 5️⃣ Carga el archivo YAML
        with open(yaml_path, 'r') as file:
            data_access = yaml.safe_load(file)
        # 6️⃣ Ejecuta la aplicación principal
        app = DownloaderWorkflow(working_folder, data_access)
        app.download_missing_files()

    else:
        raise FileNotFoundError(f"No se encontró el archivo .env en {env_path}")