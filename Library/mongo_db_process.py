import os
from datetime import date
from dotenv import load_dotenv
import yaml
import pandas as pd 
from sqlalchemy import create_engine
from pymongo import MongoClient
from bson import Binary
from dateutil.relativedelta import relativedelta  # pip install python-dateutil
from pymongo import MongoClient, UpdateOne
from bson.binary import Binary
from datetime import datetime
import os
from pymongo.errors import PyMongoError


class MONGO_DB_PROCESS:
    def pdf_to_mongo_orchestrator(self):
        # Cargar el todos los periodos en la base
        accounts_df = self.csv_to_sql_process()
        loaded_pdfs = self.pdf_alreadyloaded()
        # Retiramos renglones con PDF cargado 
        keys = ["cutoff_period", "account_number", "type"]
        # 1️⃣ Nos quedamos sólo con los que YA tienen PDF
        loaded_with_pdf = (
            loaded_pdfs
            .loc[loaded_pdfs["has_pdf"], keys]   # has_pdf == True
            .drop_duplicates()
        )
        # 2️⃣ Hacemos un anti-join: accounts_df - filas que ya tienen PDF
        accounts_df = (
            accounts_df
            .merge(loaded_with_pdf, on=keys, how="left", indicator=True)
        )

        # 3️⃣ Nos quedamos solo con las filas que NO encontraron match en loaded_with_pdf
        accounts_df = accounts_df[accounts_df["_merge"] == "left_only"].drop(columns="_merge")

        print("📉 Filas restantes en accounts_df después de quitar las que ya tienen PDF:")
        print(len(accounts_df))

        if accounts_df is not False or not accounts_df.empty:
            print(f"✅ {len(accounts_df)} Iniciando carga de PDFs a MongoDB...")
            # Obtén nuestro periodo actual 
            today = date.today()
            current_period = today.strftime("%Y-%m")
            print(f"🔍 Periodo actual: {current_period}")

            # Limpiar los renglones que correspondan al periodo actual 
            available_periods = [
                (today - relativedelta(months=offset)).strftime("%Y-%m")
                for offset in range(0, 3)
            ]
            available_files = accounts_df[accounts_df['cutoff_period'].isin(available_periods)]
            #print(avaiable_files.head(10))        
        files = []
        for index, row in accounts_df.iterrows():
            file_name = f"{row['cutoff_period']}_{row['account_number']}_{row['type']}.pdf"
            files.append(file_name)
        # Buscamos archivos con el nombre que coincida con los esperados
        # Archivos que existen físicamente en el folder temporal_downloads
        existing_files = set(os.listdir(self.temporal_downloads))

        files_to_insert = [
            os.path.join(self.temporal_downloads, f)
            for f in files
            if f in existing_files
        ]

        print("\n🚀 Archivos PDF encontrados para cargar en MongoDB:")
        for f in files_to_insert:
            print("   -", f)

        print(f"📂 Archivos encontrados para cargar: {len(files_to_insert)}")

        if len(files_to_insert) > 0:
            # Aquí estaba el bug: antes mandabas `existing_files`
            print("\n🚀 Archivos PDF encontrados para cargar en MongoDB:", files_to_insert)
            self.insert_pdfs_to_mongo(accounts_df, files_to_insert)        

    def pdf_alreadyloaded(self):
        MONGO_URI = self.data_access["MONGO_URI"]

        expected_columns = ["cutoff_period", "account_number", "type", "pdf_status", "has_pdf"]

        try:
            client = MongoClient(MONGO_URI)
            db = client["banorte_db"]
            collection = db["estados_cuenta"]

            cursor = collection.find(
                {},
                {
                    "_id": 0,
                    "cutoff_period": 1,
                    "account_number": 1,
                    "type": 1,
                    "pdf_file": 1,
                },
            )

            rows = []
            for doc in cursor:
                has_pdf = bool(doc.get("pdf_file"))
                rows.append({
                    "cutoff_period": doc.get("cutoff_period"),
                    "account_number": doc.get("account_number"),
                    "type": doc.get("type"),
                    "pdf_status": "Has PDF" if has_pdf else "No PDF",
                    "has_pdf": has_pdf,
                })

        except PyMongoError as e:
            print(f"⚠️ Error al consultar MongoDB: {e}")
            rows = []
        finally:
            try:
                client.close()
            except Exception:
                pass

        # Si no hay filas, devolvemos un DF vacío pero con las columnas esperadas
        if not rows:
            return pd.DataFrame(columns=expected_columns)

        df_status = pd.DataFrame(rows)

        # Garantizamos el orden de columnas esperado
        df_status = df_status.reindex(columns=expected_columns)

        return df_status
    
    def csv_to_sql_process(self):
        print('MONGO_DB_PROCESS')
        print(f"Working folder: {self.working_folder}")
        # 1️⃣ Conectar
        connexion = self.sql_conexion(self.data_access['sql_workflow']).connect()
        if connexion is None:
            print("❌ No se pudo establecer conexión con SQL Server.")
            return False

        # 2️⃣ Intentar leer tabla de cuentas
        try:
            query = "SELECT * FROM banorte_load.account_cutoffs"
            self.account_cutoffs = pd.read_sql(query, connexion)
            print(f"✅ Loaded accounts: {len(self.account_cutoffs)} registros.")
        except Exception as e:
            error_msg = str(e)    

        if self.account_cutoffs.empty:
            print("⚠️ No hay registros en 'banorte_load.account_cutoffs'. Captura cuentas antes de comenzar.")
            return False
        print(self.account_cutoffs.sample(5))
        return self.account_cutoffs

    def insert_pdfs_to_mongo(self, accounts_df, files_to_insert):
        """
        accounts_df: DataFrame con columnas:
            - account_number
            - type
            - cutoff_period
            - updated_at (opcional, si no viene usamos datetime.utcnow())
        existing_files: lista de rutas completas de PDFs que ya existen físicamente.
                        Los nombres de archivo tienen formato:
                        {cutoff_period}_{account_number}_{type}.pdf
                        Ej: "{temporal_downloads}/2025-10_0639_debit.pdf"
        """
        # 📦 Conexión a MongoDB (cluster JACJDB, ya configurado en self.data_access)
        MONGO_URI = self.data_access["MONGO_URI"]
        client = MongoClient(MONGO_URI)

        db = client["banorte_db"]
        collection = db["estados_cuenta"]

        # 🧱 Aseguramos índice único como "PK" compuesta
        collection.create_index(
            [
                ("cutoff_period", 1),
                ("account_number", 1),
                ("type", 1),
            ],
            unique=True,
            name="uniq_cutoff_account_type"
        )

        # 🗺️ Mapeamos (cutoff_period, account_number, type) -> ruta del PDF
        file_map = {}

        for path in files_to_insert:
            fname = os.path.basename(path)
            print(f"Path a procesar: {path}")
            # Nos aseguramos de que sea PDF
            if not fname.lower().endswith(".pdf"):
                print(f"⚠️ Archivo ignorado (no es PDF): {fname}")
                continue

            try:
                # Esperamos: cutoff_period_account_type.pdf
                cutoff_period, account_number, type_with_ext = fname.split("_", 3)
                file_type, _ = os.path.splitext(type_with_ext)  # quitamos .pdf
            except ValueError:
                print(f"⚠️ Nombre de archivo inesperado: {fname}. Se omite.")
                continue

            key = (cutoff_period, account_number, file_type)
            file_map[key] = path

        operations = []

        print("\n🚀 Iniciando upsert de estados de cuenta en MongoDB...\n")

        for _, row in accounts_df.iterrows():
            cutoff_period = str(row["cutoff_period"])
            account_number = str(row["account_number"])
            acc_type = str(row["type"])

            key = (cutoff_period, account_number, acc_type)
            pdf_path = file_map.get(key)

            pdf_data = None
            if pdf_path and os.path.exists(pdf_path):
                with open(pdf_path, "rb") as f:
                    pdf_data = Binary(f.read())
                print(f"📎 PDF encontrado para {key}: {os.path.basename(pdf_path)}")
            else:
                print(f"ℹ️ Sin PDF para {key}, se guarda solo el documento.")

            # Si el DF trae updated_at lo usamos, si no generamos uno nuevo
            if "updated_at" in accounts_df.columns:
                updated_at = row["updated_at"]
            else:
                updated_at = datetime.utcnow()

            # Documento base
            doc_set = {
                "account_number": account_number,
                "type": acc_type,
                "cutoff_period": cutoff_period,
                "updated_at": updated_at,
            }

            # Solo agregamos el binario si lo tenemos
            if pdf_data is not None:
                doc_set["pdf_file"] = pdf_data

            operations.append(
                UpdateOne(
                    {
                        "cutoff_period": cutoff_period,
                        "account_number": account_number,
                        "type": acc_type,
                    },
                    {"$set": doc_set},
                    upsert=True,
                )
            )

        if operations:
            result = collection.bulk_write(operations, ordered=False)
            print("\n✅ Operación completada en MongoDB:")
            print(f"   ➕ Insertados (upserted): {len(result.upserted_ids)}")
            print(f"   🔁 Actualizados: {result.modified_count}")
        else:
            print("⚠️ No se generaron operaciones para ejecutar en MongoDB.")

        client.close()
        print("\n🎉 Proceso terminado: documentos y PDFs sincronizados en 'banorte_db.estados_cuenta'.")


    def sql_conexion(self, sql_url):
        try:
            engine = create_engine(sql_url)
            return engine
        except Exception as e:
            print(f"❌ Error connecting to database: {e}")
            return None
        
    def __init__(self, working_folder, data_access):
        self.working_folder = working_folder
        self.data_access = data_access
        self.account_cutoffs = pd.DataFrame()
        self.temporal_downloads = os.path.join(self.working_folder, 'Info Bancaria', 'Descargas temporales')

if __name__ == "__main__":
    env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    folder_name = "MAIN_PATH"
    working_folder = os.path.dirname(env_path)
    if os.path.exists(env_path):
        load_dotenv(dotenv_path=env_path)
        main_path = os.getenv(folder_name)
        if main_path:
            working_folder = main_path
    yaml_path = os.path.join(working_folder, 'config.yaml')
    with open(yaml_path, 'r') as file:
        data_access = yaml.safe_load(file)
    app = MONGO_DB_PROCESS(working_folder, data_access)
    app.pdf_to_mongo_orchestrator()