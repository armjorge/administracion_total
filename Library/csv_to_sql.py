import os
from sqlalchemy import create_engine
import yaml
from datetime import date, datetime
import pandas as pd
import glob
import numpy as np
from psycopg2.extras import execute_values
from pandas._libs.missing import NAType
from pandas._libs.tslibs.nattype import NaTType
from dateutil.relativedelta import relativedelta

try:
    from Library.initialize import INITIALIZE
except ModuleNotFoundError:
    # fallback if running inside the Library folder
    from initialize import INITIALIZE
from dotenv import load_dotenv


class CSV_TO_SQL:
    def csv_to_sql_process(self):
        # 1️⃣ Conectar
        connexion = self.sql_conexion(self.data_access['sql_workflow']).connect()
        if connexion is None:
            print("❌ No se pudo establecer conexión con SQL Server.")
            return False

        # 2️⃣ Intentar leer tabla de cuentas
        try:
            query = "SELECT * FROM banorte_load.accounts"
            self.df_accounts = pd.read_sql(query, connexion)
            print(f"✅ Loaded accounts: {len(self.df_accounts)} registros.")

        except Exception as e:
            error_msg = str(e)

            # Si la tabla no existe
            if "UndefinedTable" in error_msg or "does not exist" in error_msg:
                print("⚠️ Table 'banorte_load.accounts' not found.")
                print("🛠️ Running INITIALIZE().initialize_postgres_db() to create schema and tables...")
                initializer = INITIALIZE()
                initializer.initialize_postgres_db(self.data_access, self.working_folder)

                # Reintento
                try:
                    self.df_accounts = pd.read_sql("SELECT * FROM banorte_load.accounts", connexion)
                    print(f"✅ Loaded accounts after creation: {len(self.df_accounts)} registros.")
                except Exception as e2:
                    print(f"❌ Error after trying to create schema/tables: {e2}")
                    return False

            # Si el esquema no existe
            elif "InvalidSchemaName" in error_msg or "schema" in error_msg.lower():
                print("⚠️ Schema 'banorte_load' not found.")
                print("🛠️ Running INITIALIZE().initialize_postgres_db() to create schema and tables...")
                initializer = INITIALIZE()
                initializer.initialize_postgres_db(self.data_access, self.working_folder)

                # Reintento
                try:
                    self.df_accounts = pd.read_sql("SELECT * FROM banorte_load.accounts", connexion)
                    print(f"✅ Loaded accounts after creation: {len(self.df_accounts)} registros.")
                except Exception as e2:
                    print(f"❌ Error after trying to create schema/tables: {e2}")
                    return False
            else:
                print(f"❌ Error ejecutando la consulta SQL: {e}")
                return False

        # 3️⃣ Validar contenido de cuentas
        if self.df_accounts.empty:
            print("⚠️ No hay registros en 'banorte_load.accounts'. Captura cuentas antes de comenzar.")
            return False
        
        # Mapping columns for both debit and credit
        mapping_debito = self.data_access['mapping_debito_banorte']
        mapping_credito = self.data_access['mapping_credito_banorte']
        primary_keys = ['fecha', 'unique_concept', 'cargo', 'abono']

        # CLOSED DATAFRAMES
        # Generate closed dataframes to upload
        df_debit_closed = self.get_dataframes_to_upload(self.closed_folder, 'BANORTE_debit_headers',  {'debit': 'cerrado'})
        df_credit_closed = self.get_dataframes_to_upload(self.closed_folder, 'BANORTE_credit_headers',  {'credit': 'cerrado'})

        # CURRENT DATAFRAMES
        # Generate current dataframes to upload
        df_debit_current = self.get_dataframes_to_upload(self.current_folder, 'BANORTE_debit_headers',  {'debit': 'abierto'})
        df_credit_current = self.get_dataframes_to_upload(self.current_folder, 'BANORTE_credit_headers',  {'credit':'abierto'}) 
        print("DataFrames 'Current' to upload summary:")
        print(df_credit_current.groupby('cuenta').size())
        print(df_debit_current.groupby('cuenta').size())
       # Save uploaded to excel         
        excel_output = os.path.join(os.path.expanduser("~"), "Downloads", "Banorte_SQL_upload_Data.xlsx")
        with pd.ExcelWriter(excel_output) as writer:
            df_debit_closed.to_excel(writer, sheet_name='Debit_Closed', index=False)
            df_credit_closed.to_excel(writer, sheet_name='Credit_Closed', index=False)
            df_debit_current.to_excel(writer, sheet_name='Debit_Current', index=False)
            df_credit_current.to_excel(writer, sheet_name='Credit_Current', index=False)
        print(f"✅ DataFrames exported to Excel at {excel_output}")

        # Column normalization to set query ready
        df_debit_closed = self.column_normalization(df_debit_closed, mapping_debito)
        df_credit_closed = self.column_normalization(df_credit_closed, mapping_credito)
        # Column normalization to set query ready
        df_debit_current = self.column_normalization(df_debit_current, mapping_debito)
        df_credit_current = self.column_normalization(df_credit_current, mapping_credito)

 


        # Upload closed dataframes to SQL
        self.upsert_dataframe(connexion, df_debit_closed, "banorte_load", "debito_cerrado", primary_keys)
        self.upsert_dataframe(connexion, df_credit_closed, "banorte_load", "credito_cerrado", primary_keys)
        self.upsert_dataframe(connexion, df_debit_current, "banorte_load", "debito_abierto", primary_keys, overwrite_all = True)
        self.upsert_dataframe(connexion, df_credit_current, "banorte_load", "credito_abierto", primary_keys, overwrite_all = True)
        # Commit and close the connection
        connexion.commit()
        connexion.close()
        
        dict_dataframes = {
            'debit_closed': df_debit_closed,
            'credit_closed': df_credit_closed,
            'debit_current': df_debit_current,
            'credit_current': df_credit_current}
        
        return dict_dataframes

    def get_dataframes_to_upload(self, folder, header, estado):
        df_generated = pd.DataFrame()

        expected_columns = self.data_access[header] + ['cuenta']

        # 1️⃣ Detect files based on estado
        if list(estado.values())[0] == 'cerrado':
            csv_files = glob.glob(os.path.join(folder, '*.csv'))
        elif list(estado.values())[0] == 'abierto':
            #(f"Archivos CSV encontrados: {[f for f in glob.glob(os.path.join(folder, '*.csv'))]}")
            csv_files = [f for f in glob.glob(os.path.join(folder, '*.csv')) if self.get_file_date(f) == self.today]
            print(f"Archivos CSV encontrados para estado abierto: {[os.path.basename(i) for i in csv_files]}")
        else:
            csv_files = []

        # 2️⃣ Handle case where no files are found
        if not csv_files:
            print(f"⚠️ No CSV files found in {folder} for estado={list(estado.values())[0]}")
            # Return an empty dataframe with expected columns (and add the ones used later)
            df_empty = pd.DataFrame(columns=expected_columns + [
                'Fecha', 'unique_concept', 'estado', 'file_name', 'file_date', 'saldo'
            ])
            return df_empty

        # 3️⃣ Process files normally
        for file in csv_files:
            try:
                df_file = pd.read_csv(file)
            except Exception as e:
                print(f"⚠️ Error reading file {os.path.basename(file)}: {e}, skipping.")
                continue

            filename = os.path.basename(file)
            matched_accounts = [str(acc) for acc in self.df_accounts['account_number'] if str(acc) in filename]
            if matched_accounts:
                df_file['cuenta'] = matched_accounts[0]
            else:
                print(f"⚠️ No se encontró número de cuenta en {filename}, saltando archivo.")
                continue

            file_date = self.get_file_date(file)

            if list(df_file.columns) == expected_columns:
                def extract_unique_concept(val):
                    if pd.isna(val):
                        return ''
                    val_str = str(val)
                    digits = ''.join(filter(str.isdigit, val_str))
                    if digits:
                        return digits
                    letters = ''.join(filter(str.isalpha, val_str))
                    return letters

                df_file['Fecha'] = df_file['Fecha'].apply(self.parse_fecha)
                df_file['unique_concept'] = df_file['Concepto'].apply(extract_unique_concept)
                df_file['estado'] = list(estado.values())[0]
                df_file['file_name'] = filename
                df_file['file_date'] = file_date

                # Generate period column 
                key, value = list(estado.items())[0]  # Ej. ('debit', 'abierto')

                if value == 'abierto':
                    if key == 'debit':
                        df_file['period'] = self.today.strftime('%Y-%m')
                    elif key == 'credit':
                        next_month = self.today + relativedelta(months=+1)
                        df_file['period'] = next_month.strftime('%Y-%m')

                if list(estado.keys())[0] == 'credit':
                    df_file['saldo'] = np.nan

                df_generated = pd.concat([df_generated, df_file], ignore_index=True)
            else:
                continue

        # 4️⃣ If still empty after processing
        if df_generated.empty:
            df_generated = pd.DataFrame(columns=expected_columns + [
                'Fecha', 'unique_concept', 'estado', 'file_name', 'file_date', 'saldo'
            ])

        return df_generated

    def upsert_dataframe(self, conn, df: pd.DataFrame, schema: str, table_name: str, primary_keys: list, overwrite_all: bool = False):
        df = df.copy()

        # Ensure PKs exist
        missing = [pk for pk in primary_keys if pk not in df.columns]
        if missing:
            raise ValueError(f"Primary keys not found in DataFrame columns: {missing}")

        # Drop duplicates on PK
        df = df.drop_duplicates(subset=primary_keys, keep="last")
        df = df.where(pd.notnull(df), None)

        null_markers = {"", "nat", "nan", "none", "null", "n/a", "<na>"}
        for col in df.columns:
            if pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(df[col]):
                mask = df[col].apply(lambda v: isinstance(v, str) and v.strip().lower() in null_markers)
                if mask.any():
                    df.loc[mask, col] = None

        cols = list(df.columns)
        col_list_sql = ", ".join(cols)
        pk_list_sql = ", ".join(primary_keys)

        # Build the update clause for DO UPDATE SET
        update_cols = [col for col in cols if col not in primary_keys]
        update_clause = ", ".join(f"{col} = EXCLUDED.{col}" for col in update_cols)
        if overwrite_all == False:
            insert_sql = f"""
                INSERT INTO {schema}.{table_name} ({col_list_sql})
                VALUES %s
                ON CONFLICT ({pk_list_sql})
                DO UPDATE SET {update_clause}
            """
        elif overwrite_all == True:
            insert_sql = f"""
                INSERT INTO {schema}.{table_name} ({col_list_sql})
                VALUES %s
            """
        date_like_cols = {col for col in cols if 'fecha' in col or 'date' in col}
        dummy_date = datetime(1900, 1, 1)

        def sanitize_value(value, column_name):
            if isinstance(value, (NaTType, NAType)):
                return dummy_date if column_name in date_like_cols else None
            if value is None:
                return dummy_date if column_name in date_like_cols else None
            if isinstance(value, pd.Timestamp):
                return value.to_pydatetime()
            if isinstance(value, str):
                cleaned = value.strip()
                lowered = cleaned.lower()
                if lowered in null_markers:
                    return dummy_date if column_name in date_like_cols else None
                return cleaned
            if pd.isna(value):
                return dummy_date if column_name in date_like_cols else None
            return value

        total = len(df)
        if total == 0:
            print(f"-- No hay filas para upsert en {schema}.{table_name}.")
            return

        raw_conn = conn.connection
        cur = raw_conn.cursor()
        try:
            if overwrite_all:
                cur.execute(f"TRUNCATE TABLE {schema}.{table_name} RESTART IDENTITY CASCADE;")
            values_iter = (
                tuple(sanitize_value(val, col) for val, col in zip(row, cols))
                for row in df.itertuples(index=False, name=None)
            )
            execute_values(cur, insert_sql, values_iter, page_size=10000)
        finally:
            cur.close()  # commit y close los maneja SQLAlchemy

        print(f"OK {total} filas upserted en {schema}.{table_name}")

    def column_normalization(self, df_input, mapping_dict):
        # Renombrar columnas según el mapping
        df_input = df_input.rename(columns=mapping_dict)
        # Detectar columnas faltantes o extra
        expected_cols = list(mapping_dict.values())
        existing_cols = [col for col in expected_cols if col in df_input.columns]
        missing_cols = [col for col in expected_cols if col not in df_input.columns]
        extra_cols = [col for col in df_input.columns if col not in expected_cols]

        if missing_cols:
            print(f"⚠️ Columnas faltantes en DataFrame: {missing_cols}")
        if extra_cols:
            print(f"ℹ️ Columnas adicionales no esperadas: {extra_cols}")

        # Usar solo las columnas existentes del mapping
        df_input = df_input.loc[:, existing_cols]

        return df_input
    
    def parse_fecha(self, value):
        formats = ['%d/%m/%Y', '%Y-%m-%d', '%Y-%m-%d %H:%M:%S']
        for fmt in formats:
            try:
                dt = pd.to_datetime(value, format=fmt, errors='coerce')
                return dt.date()  # Devuelve solo la fecha, sin hora
            except (ValueError, TypeError):
                continue

        # Fallback
        dt = pd.to_datetime(value, errors='coerce')
        if pd.isna(dt):
            return pd.NaT
        return dt.date()

    def get_file_date(self, file):
        try:
            file_path = os.path.abspath(file)
            stat = os.stat(file_path)

            creation_time = getattr(stat, "st_birthtime", stat.st_ctime or stat.st_mtime)
            dt = pd.to_datetime(datetime.fromtimestamp(creation_time))
            return dt.floor('D')  # igual, datetime64 sin hora
        except Exception as e:
            print(f"⚠️ Error obteniendo fecha de creación de {file}: {e}")
            return pd.NaT

    def sql_conexion(self, sql_url):
        try:
            engine = create_engine(sql_url)
            return engine
        except Exception as e:
            print(f"❌ Error connecting to database: {e}")
            return None
    
    def get_file_date(self, file):
        """
        Devuelve la fecha de creación del archivo CSV (metadatos del sistema).
        Usa st_birthtime si está disponible (macOS, algunos Unix),
        o st_ctime / st_mtime como fallback.
        Retorna pd.Timestamp (datetime64 compatible).
        """
        try:
            file_path = os.path.abspath(file)
            stat = os.stat(file_path)

            # Preferir la fecha de creación si está disponible
            if hasattr(stat, "st_birthtime"):
                creation_time = stat.st_birthtime
            else:
                # En Windows st_ctime suele ser la fecha de creación,
                # en Linux puede ser el último cambio de metadatos.
                creation_time = stat.st_ctime or stat.st_mtime

            # Convertir a tipo pandas Timestamp y extraer solo la fecha
            file_date = pd.to_datetime(datetime.fromtimestamp(creation_time)).date()
            return file_date
        
        except Exception as e:
            print(f"⚠️ Error obteniendo fecha de creación de {file}: {e}")
            return pd.NaT

    def sql_to_excel_export(self):
        print("Exportando datos con conceptos del servidor SQL a Excel local...")

        connexion = self.sql_conexion(self.data_access['sql_workflow']).connect()
        if connexion is None:
            print("❌ No se pudo establecer conexión con SQL Server.")
            return False

        debit_query = """
            WITH debito AS (
                SELECT 
                    fecha, concepto, cargo, abono, saldo, file_date, file_name,
                    estado, cuenta, unique_concept, period
                FROM banorte_load.debito_cerrado

                UNION ALL

                SELECT 
                    fecha, concepto, cargo, abono, saldo, file_date, file_name,
                    estado, cuenta, unique_concept, period
                FROM banorte_load.debito_abierto
            )
            SELECT 
                d.*,
                c.category_group,
                c.category_subgroup,
                c.beneficiario
            FROM debito d
            LEFT JOIN banorte_load.debito_conceptos c
                ON  d.fecha = c.fecha
                AND d.cargo = c.cargo
                AND d.abono = c.abono
                AND d.unique_concept = c.unique_concept
            ORDER BY d.fecha DESC;
        """

        df_debit = pd.read_sql(debit_query, connexion)
        query_credit = """
                    WITH credito AS (
                        SELECT 
                            fecha, concepto, cargo, abono, saldo, file_date, file_name,
                            estado, cuenta, unique_concept, period
                        FROM banorte_load.credito_cerrado

                        UNION ALL

                        SELECT 
                            fecha, concepto, cargo, abono, saldo, file_date, file_name,
                            estado, cuenta, unique_concept, period
                        FROM banorte_load.credito_abierto
                    )
                    SELECT 
                        credito.*,
                        credito_conceptos.category_group,
                        credito_conceptos.category_subgroup,
                        credito_conceptos.beneficiario
                    FROM credito
                    LEFT JOIN banorte_load.credito_conceptos AS credito_conceptos
                        ON  credito.fecha = credito_conceptos.fecha
                        AND credito.cargo = credito_conceptos.cargo
                        AND credito.abono = credito_conceptos.abono
                        AND credito.unique_concept = credito_conceptos.unique_concept
                    ORDER BY credito.fecha DESC;        
            """
        df_credit = pd.read_sql(query_credit, connexion)

        print(df_credit.head())
        # Ruta final del archivo
        home = os.path.expanduser("~")
        output_path = os.path.join(home, "Downloads", "SQL_bank_data.xlsx")

        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            df_credit.to_excel(writer, sheet_name="df_credit", index=False)
            df_debit.to_excel(writer, sheet_name="df_debit", index=False)

        print(f"Archivo guardado correctamente en: {output_path}")


        return df_debit
    def __init__(self, working_folder, data_access):
        self.today = date.today()
        self.working_folder = working_folder
        self.data_access = data_access
        self.current_folder = os.path.join(self.working_folder,'Info Bancaria', f'{self.today.year}-{self.today.month:02d}')
        self.closed_folder = os.path.join(self.working_folder,'Info Bancaria', 'Meses cerrados', 'Repositorio por mes')
        
if __name__ == "__main__":
    env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    folder_name = "MAIN_PATH"
    if os.path.exists(env_path):
        load_dotenv(dotenv_path=env_path)
        working_folder = os.getenv(folder_name)    
    yaml_path = os.path.join(working_folder, 'config.yaml')
    with open(yaml_path, 'r') as file:
        data_access = yaml.safe_load(file)
    app = CSV_TO_SQL(working_folder, data_access)
    app.csv_to_sql_process()