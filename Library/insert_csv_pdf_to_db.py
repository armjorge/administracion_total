import os
from datetime import date
from dateutil.relativedelta import relativedelta  # pip install python-dateutil
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
import yaml
import pandas as pd
from pymongo import MongoClient
from bson.binary import Binary
from datetime import datetime

class MONGO_DB_LAKE:
    def __init__(self, working_folder, data_access):
        self.working_folder = working_folder
        self.data_access = data_access
        self.account_cutoffs = pd.DataFrame()
        self.temporal_downloads = os.path.join(
            self.working_folder, "Info Bancaria", "Descargas temporales"
        )
    def mongo_db_feed(self):
        print("📡 Obteniendo los documentos para los que falte csv y pdf...")
        csv_to_insert, pdf_to_insert = self.get_mongo_db_files()

        # Nada que hacer
        if (csv_to_insert is None or csv_to_insert.empty) and (
            pdf_to_insert is None or pdf_to_insert.empty
        ):
            print("✅ No hay documentos pendientes de recibir archivos (CSV/PDF).")
            return

        # ---------- 1) Construir nombres de archivo esperados ----------

        # CSV
        csv_files_df = pd.DataFrame()
        if csv_to_insert is not None and not csv_to_insert.empty:
            csv_to_insert = csv_to_insert.copy()

            def build_csv_name(row):
                if str(row["cutoff_period"]) == "open":
                    return f"open_{row['account_number']}_{row['type']}.csv"
                else:
                    return f"{row['cutoff_period']}_{row['account_number']}_{row['type']}.csv"

            csv_to_insert["filename"] = csv_to_insert.apply(build_csv_name, axis=1)
            csv_to_insert["filetype"] = "csv"
            csv_files_df = csv_to_insert

        # PDF
        pdf_files_df = pd.DataFrame()
        if pdf_to_insert is not None and not pdf_to_insert.empty:
            pdf_to_insert = pdf_to_insert.copy()

            # Aseguramos que no entren 'open' por si acaso
            pdf_to_insert = pdf_to_insert[pdf_to_insert["cutoff_period"] != "open"]

            if not pdf_to_insert.empty:
                pdf_to_insert["filename"] = pdf_to_insert.apply(
                    lambda row: f"{row['cutoff_period']}_{row['account_number']}_{row['type']}.pdf",
                    axis=1,
                )
                pdf_to_insert["filetype"] = "pdf"
                pdf_files_df = pdf_to_insert

        # Unimos CSV + PDF en un solo DataFrame de trabajo
        if csv_files_df.empty and pdf_files_df.empty:
            print("⚠️ No hay archivos CSV/PDF a procesar después de filtrar.")
            return

        files_df = pd.concat([csv_files_df, pdf_files_df], ignore_index=True)

        # ---------- 2) Ver qué archivos existen físicamente en la carpeta de descargas ----------

        files_df["full_path"] = files_df["filename"].apply(
            lambda fn: os.path.join(self.temporal_downloads, fn)
        )
        files_df["exists"] = files_df["full_path"].apply(os.path.exists)

        files_to_load = files_df[files_df["exists"]].reset_index(drop=True)

        if files_to_load.empty:
            print(
                f"⚠️ Ninguno de los archivos esperados se encontró en: {self.temporal_downloads}"
            )
            print("   Archivos esperados eran:")
            print(files_df[["filename", "cutoff_period", "account_number", "type"]])
            return

        print("\n📂 Archivos encontrados para cargar a MongoDB:")
        for _, r in files_to_load.iterrows():
            print(f"   - {r['filetype'].upper()}: {r['filename']}")

        # ---------- 3) Conectar a Mongo y cargar los binarios ----------

        MONGO_URI = self.data_access["MONGO_URI"]
        client = MongoClient(MONGO_URI)
        db = client["banorte_db"]
        collection = db["estados_cuenta"]

        for _, row in files_to_load.iterrows():
            cutoff_period = str(row["cutoff_period"])
            account_number = str(row["account_number"])
            acc_type = str(row["type"])
            filetype = row["filetype"]
            full_path = row["full_path"]
            filename = row["filename"]

            print(
                f"\n➡️ Cargando {filetype.upper()} para "
                f"{cutoff_period}-{account_number}-{acc_type}"
            )

            # Leer archivo como binario
            try:
                with open(full_path, "rb") as f:
                    file_data = Binary(f.read())
            except FileNotFoundError:
                print(f"⚠️ Archivo no encontrado al intentar abrirlo: {full_path}")
                continue

            # Campos a setear
            update_fields = {
                "updated_at": datetime.utcnow(),
            }

            if filetype == "csv":
                update_fields["csv_file"] = file_data
                update_fields["csv_filename"] = filename
            elif filetype == "pdf":
                update_fields["pdf_file"] = file_data
                update_fields["pdf_filename"] = filename

            # Actualizamos el documento correspondiente
            result = collection.update_one(
                {
                    "cutoff_period": cutoff_period,
                    "account_number": account_number,
                    "type": acc_type,
                },
                {"$set": update_fields},
                upsert=False,  # asumimos que el doc ya existe (lo crea get_mongo_db_files si falta 'open')
            )

            if result.matched_count == 0:
                print(
                    f"⚠️ No se encontró documento en Mongo para "
                    f"{cutoff_period}-{account_number}-{acc_type}. No se insertó el archivo."
                )
            else:
                print(
                    f"✅ {filetype.upper()} cargado correctamente para "
                    f"{cutoff_period}-{account_number}-{acc_type}"
                )

        client.close()
        print("\n🎉 Proceso terminado: archivos CSV/PDF sincronizados en MongoDB.")

    def get_mongo_db_files(self):
        """
        Lee la colección banorte_db.estados_cuenta y construye:

        - Crea documentos 'open' para cada (account_number, type) que exista en el año actual
        y que aún no tenga un documento con cutoff_period = 'open'.

        - csv_to_insert: documentos que NO tienen csv_file y cuyo cutoff_period es:
            * 'open' y (updated_at es NULL o updated_at (día) < hoy)
            * O bien un periodo tipo 'YYYY-MM' >= periodo_actual - 2 meses

        - pdf_to_insert: documentos que NO tienen pdf_file y cuyo cutoff_period:
            * NO es 'open'
            * pertenece al año actual
        """

        MONGO_URI = self.data_access["MONGO_URI"]

        # Fecha actual y reglas de periodos
        today = date.today()
        current_period = today.strftime("%Y-%m")
        limit_csv_period = (today - relativedelta(months=2)).strftime("%Y-%m")
        current_year = str(today.year)

        print(f"📅 Periodo actual: {current_period}")
        print(f"📉 Límite CSV (periodo mínimo): {limit_csv_period}")
        print(f"📄 Año actual para PDFs y 'open': {current_year}")

        client = MongoClient(MONGO_URI)
        db = client["banorte_db"]
        collection = db["estados_cuenta"]

        # Traemos lo necesario desde Mongo, incluyendo updated_at
        cursor = collection.find(
            {},
            {
                "_id": 0,
                "cutoff_period": 1,
                "account_number": 1,
                "type": 1,
                "pdf_file": 1,
                "csv_file": 1,
                "updated_at": 1,
            },
        )

        rows = []
        for doc in cursor:
            rows.append(
                {
                    "cutoff_period": doc.get("cutoff_period"),
                    "account_number": str(doc.get("account_number")),
                    "type": str(doc.get("type")),
                    "has_pdf": bool(doc.get("pdf_file")),
                    "has_csv": bool(doc.get("csv_file")),
                    "updated_at": doc.get("updated_at"),
                }
            )

        # Si no hay nada, de todas formas necesitamos crear los 'open' a partir de nada,
        # así que df puede empezar vacío.
        df = pd.DataFrame(rows, columns=[
            "cutoff_period", "account_number", "type", "has_pdf", "has_csv", "updated_at"
        ])

        # Normalizamos updated_at a datetime (puede quedar NaT)
        df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")

        # ---------- CREAR DOCUMENTOS 'open' QUE FALTAN ----------
        # Pares (account_number, type) en el año actual, solo periodos normales YYYY-MM
        mask_not_open = df["cutoff_period"].ne("open") & df["cutoff_period"].notna()
        mask_current_year = mask_not_open & df["cutoff_period"].astype(str).str.startswith(current_year + "-")

        base_pairs = (
            df.loc[mask_current_year, ["account_number", "type"]]
            .drop_duplicates()
            .reset_index(drop=True)
        )

        # Pares que YA tienen 'open'
        existing_open_pairs = (
            df.loc[df["cutoff_period"] == "open", ["account_number", "type"]]
            .drop_duplicates()
            .reset_index(drop=True)
        )

        # Anti-join: pares para los que NO existe aún un doc 'open'
        if not base_pairs.empty:
            merged = base_pairs.merge(
                existing_open_pairs,
                on=["account_number", "type"],
                how="left",
                indicator=True,
            )
            missing_open = merged[merged["_merge"] == "left_only"].drop(columns="_merge")
        else:
            missing_open = base_pairs.iloc[0:0]  # vacío

        new_open_docs = []
        new_open_rows = []

        for _, r in missing_open.iterrows():
            acc = r["account_number"]
            acc_type = r["type"]

            doc = {
                "cutoff_period": "open",
                "account_number": acc,
                "type": acc_type,
                "pdf_file": None,
                "csv_file": None,
                "updated_at": None,
            }
            new_open_docs.append(doc)

            new_open_rows.append(
                {
                    "cutoff_period": "open",
                    "account_number": acc,
                    "type": acc_type,
                    "has_pdf": False,
                    "has_csv": False,
                    "updated_at": pd.NaT,
                }
            )

        # Insertamos en Mongo los 'open' que faltan
        if new_open_docs:
            result = collection.insert_many(new_open_docs)
            print(f"🆕 Creados {len(result.inserted_ids)} documentos 'open' en MongoDB.")

        # Cerramos conexión a Mongo
        client.close()

        # Añadimos los nuevos 'open' también al DataFrame en memoria
        if new_open_rows:
            df_new_open = pd.DataFrame(new_open_rows)
            df = pd.concat([df, df_new_open], ignore_index=True)

        # Re-normalizamos updated_at por si se mezcló con NaT
        df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")
        today_date = pd.to_datetime(today)

        # ---------- CSV A INSERTAR ----------
        # Falta CSV
        mask_missing_csv = ~df["has_csv"].fillna(False)

        # Periodos "open" → elegibles si:
        #  - updated_at es NULL (nunca cargado)
        #  - O updated_at (fecha) < hoy
        mask_open = df["cutoff_period"] == "open"
        mask_open_never_updated = mask_open & df["updated_at"].isna()
        mask_open_old = mask_open & df["updated_at"].notna() & (
            df["updated_at"].dt.normalize() < today_date
        )
        mask_open_eligible = mask_open_never_updated | mask_open_old

        # Periodos tipo 'YYYY-MM' recientes (>= current_period - 2 meses)
        mask_regular_period = df["cutoff_period"].ne("open") & df["cutoff_period"].notna()
        mask_csv_recent = mask_regular_period & (
            df["cutoff_period"].astype(str) >= limit_csv_period
        )

        mask_csv_eligible = mask_missing_csv & (mask_open_eligible | mask_csv_recent)

        csv_to_insert = df.loc[
            mask_csv_eligible, ["cutoff_period", "account_number", "type"]
        ].reset_index(drop=True)

        # ---------- PDF A INSERTAR ----------
        # Falta PDF
        mask_missing_pdf = ~df["has_pdf"].fillna(False)

        # Solo periodos del año actual y que no sean 'open'
        mask_not_open = df["cutoff_period"].ne("open") & df["cutoff_period"].notna()
        mask_current_year_pdf = (
            mask_not_open
            & df["cutoff_period"].astype(str).str.startswith(current_year + "-")
        )

        mask_pdf_eligible = mask_missing_pdf & mask_current_year_pdf

        pdf_to_insert = df.loc[
            mask_pdf_eligible, ["cutoff_period", "account_number", "type"]
        ].reset_index(drop=True)

        # ---------- OUTPUT ----------
        print("\n📥 CSV pendientes de insertar (incluyendo 'open' nuevos o desactualizados):")
        print(csv_to_insert)

        print("\n📥 PDFs pendientes de insertar (solo año actual):")
        print(pdf_to_insert)

        return csv_to_insert, pdf_to_insert

if __name__ == "__main__":
    # Cargar MAIN_PATH desde .env si existe
    env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
    folder_name = "MAIN_PATH"
    working_folder = os.path.dirname(env_path)

    if os.path.exists(env_path):
        load_dotenv(dotenv_path=env_path)
        main_path = os.getenv(folder_name)
        if main_path:
            working_folder = main_path

    # Cargar config.yaml
    yaml_path = os.path.join(working_folder, "config.yaml")
    with open(yaml_path, "r") as file:
        data_access = yaml.safe_load(file)

    app = MONGO_DB_LAKE(working_folder, data_access)
    app.mongo_db_feed()