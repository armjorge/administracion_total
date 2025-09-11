import os  # Import the os module
#from utils.helpers import Helper  # Import the Helper class
import yaml
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
from glob import glob
import numpy as np
import re
try:
    from utils.helpers import Helper
except ModuleNotFoundError:
    import sys
    from pathlib import Path
    project_root = Path(__file__).resolve().parent.parent  # repo root
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    from utils.helpers import Helper



class Conceptos:
    def __init__(self, strategy_folder, data_access):
        self.strategy_folder = strategy_folder
        self.data_access = data_access
        self.helper = Helper()
        self.mirror_credito_path = os.path.join(self.strategy_folder, 'mirror_credito.pkl')
        self.mirror_debito_path = os.path.join(self.strategy_folder, 'mirror_debito.pkl')
        self.conceptos_master = os.path.join(self.strategy_folder, 'Conceptos.xlsx')
        self.mirror_excel_path = os.path.join(self.strategy_folder, 'Mirror.xlsx')
        self.conceptos_temporal_path = os.path.join(self.strategy_folder, 'Conceptos temporales')
        self.excel_credit_conceptos = os.path.join(self.conceptos_temporal_path, "credito","credito_corriente.xlsx")
        self.excel_debito_conceptos = os.path.join(self.conceptos_temporal_path, "debito", "debito_corriente.xlsx")
        self.excel_debito_perdidos = os.path.join(self.strategy_folder, "debito_perdidos.xlsx")
        self.excel_credito_perdidos = os.path.join(self.strategy_folder, "credito_perdidos.xlsx")
        self.conceptos_dict = {}
        self.columns_to_upload = ['fecha', 'concepto', 'cargo', 'abono', 'saldo', 'file_date', 'file_name', 'estado', 'cuenta', 'unic_concept']
        self.concept_columns = ['beneficiario', 'categoria', 'grupo', 'id_presupuesto', 'concepto_procesado', 'ubicacion']
        self.primary_key_columns = ['fecha', 'unic_concept', 'cargo', 'abono']

    def generador_de_conceptos(self):
        conceptos_url = self.data_access['sql_url']
        source_schema = 'banorte_load'   
        print(f"Conectando a la fuente de datos: {conceptos_url}")
        # Crear motores de conexión (source y, si quieres, target para futuras cargas)
        try:
            src_engine = create_engine(conceptos_url, pool_pre_ping=True)
            with src_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("✅ Conexión a la fuente exitosa.")
        except Exception as e:
            print(f"❌ Error conectando a la fuente: {e}")
            return
        df_credito_conceptos = pd.read_sql(text(f'SELECT * FROM "{source_schema}"."credito_conceptos"'), src_engine)
        df_debito_conceptos = pd.read_sql(text(f'SELECT * FROM "{source_schema}"."debito_conceptos"'), src_engine)
        self.conceptos_dict['credito_conceptos'] = df_credito_conceptos
        self.conceptos_dict['debito_conceptos'] = df_debito_conceptos
        for key, df in self.conceptos_dict.items():
            print(f"Procesando {key} con {len(df)} registros.")

        self.filter_download_upload_concepts()
        return True

        
    def filter_download_upload_concepts(self):
        # Dict with dataframes already loaded in self.conceptos_dict
        try:
            if not self.conceptos_dict:
                print("❌ No hay dataframes cargados en 'conceptos_dict'.")
                return False

            # 1) Elegir tabla a actualizar
            options = {"1": "credito_conceptos", "2": "debito_conceptos"}
            print("¿Qué tabla de conceptos deseas actualizar?")
            print("  1) credito_conceptos")
            print("  2) debito_conceptos")
            choice = None
            while choice not in options:
                choice = input("Elige 1 o 2: ").strip()
            table_name = options[choice]

            if table_name not in self.conceptos_dict:
                print(f"❌ {table_name} no está disponible en conceptos_dict.")
                return False

            df = self.conceptos_dict[table_name].copy()
            if df.empty:
                print(f"❌ {table_name} está vacío.")
                return False

            # Asegurar tipo de fecha
            df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce')
            if df['fecha'].isna().all():
                print("❌ La columna 'fecha' no contiene valores de fecha válidos.")
                return False

            # 2) Elegir un periodo (mes-año) para completar
            print("Elige un mes-año para completar")
            periods = (
                df['fecha']
                .dt.to_period('M')
                .astype(str)
                .dropna()
                .unique()
                .tolist()
            )
            periods = sorted(periods, reverse=True)  # newest top
            if not periods:
                print("❌ No hay periodos disponibles en la columna 'fecha'.")
                return False

            for idx, p in enumerate(periods):
                print(f"  [{idx}] {p}")
            chosen_index = None
            while True:
                try:
                    raw = input("Escribe el índice del periodo a usar: ").strip()
                    chosen_index = int(raw)
                    if 0 <= chosen_index < len(periods):
                        break
                except Exception:
                    pass
                print("Índice inválido. Intenta de nuevo.")

            chosen_period = periods[chosen_index]
            # 3) Filtrar DF por periodo seleccionado
            mask = df['fecha'].dt.to_period('M').astype(str) == chosen_period
            work = df.loc[mask].copy()
            if work.empty:
                print(f"❌ No hay filas para el periodo seleccionado {chosen_period}.")
                return False

            # 4) Crear archivo temporal de Excel para edición
            self.helper.create_directory_if_not_exists(self.conceptos_temporal_path)
            filename = f"{table_name}_{chosen_period}.xlsx".replace(':', '-')
            temporal_concepts_path = os.path.join(self.conceptos_temporal_path, filename)

            # Incluir columnas de enriquecimiento si no existen
            for col in self.concept_columns:
                if col not in work.columns:
                    work[col] = pd.NA

            # Ordenar columnas: PK + concepto + columnas de enriquecimiento + resto
            preferred = self.primary_key_columns + ['concepto'] + self.concept_columns
            cols = [c for c in preferred if c in work.columns] + [c for c in work.columns if c not in preferred]
            work = work[cols]

            # Guardar y abrir
            work.to_excel(temporal_concepts_path, index=False)
            print(f"📝 Archivo para editar: {temporal_concepts_path}")
            self.helper.open_xlsx_file(temporal_concepts_path)
            input("Llena las columnas que necesites y pulsa Enter para cargar a SQL...")

            # 5) Cargar archivo editado
            df_new = pd.read_excel(temporal_concepts_path)
            if df_new.empty:
                print("❌ El archivo editado no tiene filas.")
                return False

            # Normalizar tipos clave y columnas de enriquecimiento
            df_new['fecha'] = pd.to_datetime(df_new['fecha'], errors='coerce').dt.date
            for col in ['unic_concept']:
                if col in df_new.columns:
                    df_new[col] = df_new[col].astype(str)
            for col in ['cargo', 'abono']:
                if col in df_new.columns:
                    # Mantener número si es posible
                    df_new[col] = pd.to_numeric(df_new[col], errors='coerce')

            # Mantener solo filas con PK completo
            pk = self.primary_key_columns
            df_new = df_new.dropna(subset=[c for c in pk if c in df_new.columns])
            if df_new.empty:
                print("❌ No hay filas con llave primaria completa para actualizar.")
                return False

            # Filas con al menos un campo de enriquecimiento no vacío
            enrich_cols = [c for c in self.concept_columns if c in df_new.columns]
            if not enrich_cols:
                print("❌ El archivo no contiene columnas de enriquecimiento para actualizar.")
                return False

            def non_empty_any(row):
                for c in enrich_cols:
                    v = row.get(c, None)
                    if pd.notna(v) and str(v).strip() != "":
                        return True
                return False

            mask_enrich = df_new.apply(non_empty_any, axis=1)
            rows_to_update = df_new.loc[mask_enrich].copy()
            if rows_to_update.empty:
                print("ℹ️ No hay cambios de enriquecimiento para aplicar.")
                return True

            print(f"Se actualizarán {len(rows_to_update)} filas en {table_name} para el periodo {chosen_period}.")

            # 6) Actualizar en SQL con base en la PK (solo columnas de enriquecimiento no vacías)
            conceptos_url = self.data_access['sql_url']
            concepts_schema = 'banorte_load'
            updated = 0
            with create_engine(conceptos_url, pool_pre_ping=True).begin() as conn:
                for _, row in rows_to_update.iterrows():
                    set_parts = []
                    params = {}
                    for c in enrich_cols:
                        val = row.get(c, None)
                        if pd.isna(val) or (isinstance(val, str) and val.strip() == ""):
                            continue  # No sobreescribir con vacío/NaN
                        pname = f"set_{c}"
                        set_parts.append(f"{c} = :{pname}")
                        params[pname] = val

                    # Si no hay columnas para actualizar en esta fila, saltar
                    if not set_parts:
                        continue

                    # Parámetros de PK
                    params.update({
                        'pk_fecha': row['fecha'],
                        'pk_unic_concept': str(row['unic_concept']),
                        'pk_cargo': row['cargo'],
                        'pk_abono': row['abono'],
                    })

                    sql = text(
                        f"""
                        UPDATE "{concepts_schema}"."{table_name}"
                        SET {', '.join(set_parts)}
                        WHERE fecha = :pk_fecha
                          AND unic_concept = :pk_unic_concept
                          AND cargo = :pk_cargo
                          AND abono = :pk_abono
                        """
                    )
                    res = conn.execute(sql, params)
                    updated += res.rowcount if hasattr(res, 'rowcount') else 0

            print(f"✅ Actualización completada. Filas afectadas: {updated}")
            return True

        except KeyboardInterrupt:
            print("Operación cancelada por el usuario.")
            return False
        except Exception as e:
            print(f"❌ Error en filter_download_upload_concepts: {e}")
            return False 


def main():
    folder_root = os.getcwd()
    strategy_folder = os.path.join(folder_root, "Implementación", "Estrategia")
    passwords_path = os.path.join(folder_root, "Implementación", "Info Bancaria", 'passwords.yaml')
    with open(passwords_path, 'r') as f:
        data_access = yaml.safe_load(f)
    print(f"Folder de trabajo {strategy_folder}")
    
    if Conceptos(strategy_folder, data_access).generador_de_conceptos():
        print("✅ Proceso de generación de conceptos finalizado exitosamente.")
    return True

if __name__ == "__main__":
    main()

    
