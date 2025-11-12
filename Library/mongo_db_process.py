import os
from dotenv import load_dotenv
import yaml
import pandas as pd 
from sqlalchemy import create_engine
from pymongo import MongoClient
from bson import Binary


class MONGO_DB_PROCESS:
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

        # 3️⃣ Guardar en MongoDB
        MONGO_URI = self.data_access["MONGO_URI"]
        client = MongoClient(MONGO_URI)
        db = client["banorte_db"]
        collection = db["account_pdfs"]        
        # 📂 Cargar PDF por fila
        df = self.account_cutoffs
        for idx, row in df.iterrows():
            print(f"\n➡️  Subiendo PDF para cuenta {row['account_number']} ({row['cutoff_period']})")
            pdf_path = input("👉 Ingresa la ruta completa del archivo PDF: ").strip()

            if not os.path.exists(pdf_path):
                print(f"⚠️ No se encontró el archivo: {pdf_path}")
                continue

            with open(pdf_path, "rb") as f:
                pdf_data = Binary(f.read())

            document = {
                "account_number": row["account_number"],
                "type": row["type"],
                "cutoff_period": row["cutoff_period"],
                "pdf_file": pdf_data
            }

            result = collection.insert_one(document)
            print(f"✅ PDF cargado con _id: {result.inserted_id}")

        print("\n🎉 Todos los archivos cargados correctamente.")

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

if __name__ == "__main__":
    env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    folder_name = "MAIN_PATH"
    if os.path.exists(env_path):
        load_dotenv(dotenv_path=env_path)
        working_folder = os.getenv(folder_name)    
    yaml_path = os.path.join(working_folder, 'config.yaml')
    with open(yaml_path, 'r') as file:
        data_access = yaml.safe_load(file)
    app = MONGO_DB_PROCESS(working_folder, data_access)
    app.csv_to_sql_process()