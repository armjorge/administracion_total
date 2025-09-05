import os  # Import the os module
#from utils.helpers import Helper  # Import the Helper class
import yaml
from sqlalchemy import create_engine, text
import pandas as pd
from glob import glob
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

    def export_mirror_excel(self):
        # Cargar pickles (si no existen, usar DF vacío)
        df_credito = pd.read_pickle(self.mirror_credito_path)
        df_debito = pd.read_pickle(self.mirror_debito_path)
        
        print(f"📝 Escribiendo mirrors a Excel: {self.mirror_excel_path}")
        with pd.ExcelWriter(self.mirror_excel_path, engine='openpyxl') as writer:
            # Según especificación: hoja debito <= mirror_credito; hoja credito <= mirror_debito
            df_debito.to_excel(writer, sheet_name='pickle_debito', index=False)
            df_credito.to_excel(writer, sheet_name='pickle_credito', index=False)
        print("✅ Mirror.xlsx actualizado con hojas 'pickle_debito' y 'pickle_credito'.")

    def generar_mirror_dataframes(self, dict_dataframes):
        print("\nGenerando dataframes espejo en caso de que no existan en localmente como pickles\n")
        if not os.path.exists(self.conceptos_master):
            print(f"🗂️  No existe {os.path.basename(self.conceptos_master)}. Creando plantilla con columnas de ejemplo…")
            df_conceptos_columnas = pd.DataFrame({
                'beneficiario': [None] * 5,
                'categoria': ['Pensiones', 'Reembolsos Eseotres', 'Reembolsos Bombón', 'Casa', 'Ocio', 'Salud'], 
                'grupo': ['Gasto fijo', 'No clasificado', 'Vivienda'] + [None] * 3,
                'clave_presupuestal': [None] * 6,
                'concepto_procesado': [None] * 6
            })
            df_conceptos_columnas.to_excel(self.conceptos_master, index=False)

        df_conceptos = pd.read_excel(self.conceptos_master)
        conceptos_cols = list(df_conceptos.columns)
        # Load previous mirrors
        try:
            df_mirror_debito = pd.read_pickle(self.mirror_debito_path)
        except Exception as e:
            print(f"⚠️  No se pudo leer {os.path.basename(self.mirror_debito_path)}; generando uno nuevo + agregando las columnas de {os.path.basename(self.conceptos_master)}")
            df_source_debito = pd.concat([
                dict_dataframes.get('debito_corriente', pd.DataFrame()),
                dict_dataframes.get('debito_cerrado', pd.DataFrame()),
            ], ignore_index=True, sort=False)
            # Add missing columns from conceptos_cols without replacing existing ones
            for col in conceptos_cols:
                if col not in df_source_debito.columns:
                    df_source_debito[col] = None
            df_source_debito.to_pickle(self.mirror_debito_path)
            df_mirror_debito = df_source_debito  # Ensure df_mirror_debito is defined
        try:
            df_mirror_credito = pd.read_pickle(self.mirror_credito_path)
        except Exception as e:
            print(f"⚠️  No se pudo leer {os.path.basename(self.mirror_credito_path)}; generando uno nuevo + agregando las columnas de {os.path.basename(self.conceptos_master)}")
            df_source_credito = pd.concat([
                dict_dataframes.get('credito_corriente', pd.DataFrame()),
                dict_dataframes.get('credito_cerrado', pd.DataFrame())
            ], ignore_index=True, sort=False)
            # Add missing columns from conceptos_cols without replacing existing ones
            for col in conceptos_cols:
                if col not in df_source_credito.columns:
                    df_source_credito[col] = None
            df_source_credito.to_pickle(self.mirror_credito_path)
            df_mirror_credito = df_source_credito  # Ensure df_mirror_credito is defined
        print(f"✅ Dataframes espejo creados con {len(df_mirror_debito)} filas en DÉBITO y {len(df_mirror_credito)} filas en CRÉDITO.")
        self.export_mirror_excel()

    def dataframes_from_source(self, dict_dataframes):
        df_source_credito = pd.concat([
            dict_dataframes.get('credito_corriente', pd.DataFrame()),
            dict_dataframes.get('credito_cerrado', pd.DataFrame())
        ], ignore_index=True, sort=False)        
        df_source_debito = pd.concat([
            dict_dataframes.get('debito_corriente', pd.DataFrame()),
            dict_dataframes.get('debito_cerrado', pd.DataFrame()),
        ], ignore_index=True, sort=False)
        return df_source_credito, df_source_debito
    

    def generador_de_conceptos(self, dict_dataframes):
        print(self.helper.message_print("Iniciando generador de conceptos..."))

        print("1) Confirmando que existen pickles con dataframes espejo…")
        self.generar_mirror_dataframes(dict_dataframes)        
        print(self.helper.message_print("2 Actualizando los espejos locales con los archivos de la base SQL"))
        self.actualiza(dict_dataframes)
      

    def actualiza(self, dict_dataframes):
        print(self.helper.message_print("\n🔄 Actualizando mirrors desde archivos de Conceptos temporales…"))
        df_source_credito, df_source_debito = self.dataframes_from_source(dict_dataframes)
        column_key_credito = ['fecha', 'concepto', 'abono', 'cargo', 'tarjeta']
        column_key_debito = ['fecha', 'concepto', 'cargos', 'abonos', 'saldos']
        update_columns = ['file_date', 'file_name']
        self.mirror(df_source_credito, self.mirror_credito_path, column_key_credito, update_columns)
        self.mirror(df_source_debito, self.mirror_debito_path, column_key_debito, update_columns)

    def mirror(self, df_source, mirror_path, match_columns, update_columns):
        # Cargar espejo
        try:
            df_mirror = pd.read_pickle(mirror_path)
        except Exception as e:
            print(f"❌ No se pudo leer el mirror '{mirror_path}': {e}")
            return

        # Validaciones mínimas
        if not isinstance(df_source, pd.DataFrame):
            print("⚠️ df_source no es un DataFrame válido")
            return
        if not all(c in df_mirror.columns for c in match_columns):
            print("⚠️ Columnas de llave faltantes en mirror; no se puede continuar:", [c for c in match_columns if c not in df_mirror.columns])
            return
        if not all(c in df_source.columns for c in match_columns):
            print("⚠️ Columnas de llave faltantes en source; no se puede continuar:", [c for c in match_columns if c not in df_source.columns])
            return
        if not all(c in df_mirror.columns for c in update_columns):
            print("⚠️ Columnas a actualizar faltantes en mirror; no se puede continuar:", [c for c in update_columns if c not in df_mirror.columns])
            return
        if not all(c in df_source.columns for c in update_columns):
            print("⚠️ Columnas a actualizar faltantes en source; no se puede continuar:", [c for c in update_columns if c not in df_source.columns])
            return

        # No agregar columnas nuevas: solo operar con las que ya existen en el mirror
        common_cols = [c for c in df_mirror.columns if c in df_source.columns]

        # Claves únicas de ambos
        source_keys = df_source[match_columns].drop_duplicates()
        mirror_keys = df_mirror[match_columns].drop_duplicates()

        # Filas nuevas (en source y no en mirror) - agregar una fila por clave
        new_keys = source_keys.merge(mirror_keys, on=match_columns, how='left', indicator=True)
        new_keys = new_keys.loc[new_keys['_merge'] == 'left_only', match_columns]
        if len(new_keys) > 0:
            # Representante por clave desde source
            src_unique = df_source.drop_duplicates(subset=match_columns, keep='last')
            rows_to_add = src_unique.merge(new_keys, on=match_columns, how='inner')
            rows_to_add = rows_to_add[common_cols]
            df_mirror = pd.concat([df_mirror, rows_to_add], ignore_index=True)
        added = len(new_keys)

        # Filas perdidas (en mirror y no en source) - eliminar
        lost_keys = mirror_keys.merge(source_keys, on=match_columns, how='left', indicator=True)
        lost_keys = lost_keys.loc[lost_keys['_merge'] == 'left_only', match_columns]
        if len(lost_keys) > 0:
            mk = df_mirror[match_columns]
            keep_mask = mk.merge(source_keys, on=match_columns, how='left', indicator=True)['_merge'].eq('both')
            df_mirror = df_mirror.loc[keep_mask].reset_index(drop=True)
        removed = len(lost_keys)

        # Actualizar columnas actualizables basadas en las llaves
        # Copia previa para medir cambios
        prev_updates = None
        try:
            prev_updates = df_mirror[update_columns].copy()
        except Exception:
            prev_updates = None

        src_unique_for_update = df_source.drop_duplicates(subset=match_columns, keep='last')
        upd = df_mirror[match_columns].merge(
            src_unique_for_update[match_columns + update_columns],
            on=match_columns,
            how='left'
        )
        for col in update_columns:
            df_mirror[col] = upd[col].values

        # Conteo de filas actualizadas
        updated = 0
        if prev_updates is not None:
            diff_any = None
            for col in update_columns:
                a = df_mirror[col]
                b = prev_updates[col]
                neq = ~((a == b) | (a.isna() & b.isna()))
                diff_any = neq if diff_any is None else (diff_any | neq)
            updated = int(diff_any.sum()) if diff_any is not None else 0

        # Verificación básica: mismas llaves en ambos
        final_keys = df_mirror[match_columns].drop_duplicates()
        only_source = source_keys.merge(final_keys, on=match_columns, how='left', indicator=True)
        only_source = int((only_source['_merge'] == 'left_only').sum())
        only_mirror = final_keys.merge(source_keys, on=match_columns, how='left', indicator=True)
        only_mirror = int((only_mirror['_merge'] == 'left_only').sum())
        if only_source == 0 and only_mirror == 0:
            print(f"✅ Mirror actualizado: +{added} añadidas, -{removed} eliminadas, {updated} filas con valores de actualización cambiados")
        else:
            print(f"⚠️ Desajuste de llaves tras la actualización (solo_source={only_source}, solo_mirror={only_mirror})")

        # Guardar sin alterar el conjunto de columnas del mirror
        try:
            df_mirror.to_pickle(mirror_path)
            print(f"💾 Guardado: {mirror_path}")
        except Exception as e:
            print(f"❌ No se pudo guardar el mirror en '{mirror_path}': {e}")

        self.export_mirror_excel()
def main():
    folder_root = os.getcwd()
    strategy_folder = os.path.join(folder_root, "Implementación", "Estrategia")
    passwords_path = os.path.join(folder_root, "Implementación", "Info Bancaria", 'passwords.yaml')
    with open(passwords_path, 'r') as f:
        data_access = yaml.safe_load(f)
    dataframes_dict = {}
    source_url = data_access['sql_url']
    source_schema = 'banorte_lake'   
    print(f"Folder de trabajo {strategy_folder}")
    print(f"Conectando a la fuente de datos: {source_url}")
    # Crear motores de conexión (source y, si quieres, target para futuras cargas)
    try:
        src_engine = create_engine(source_url, pool_pre_ping=True)
        with src_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ Conexión a la fuente exitosa.")
    except Exception as e:
        print(f"❌ Error conectando a la fuente: {e}")
        return
    df_credito_corriente = pd.read_sql(text(f'SELECT * FROM "{source_schema}"."credito_corriente"'), src_engine)
    df_credito_cerrado = pd.read_sql(text(f'SELECT * FROM "{source_schema}"."credito_cerrado"'), src_engine)
    df_debito_corriente = pd.read_sql(text(f'SELECT * FROM "{source_schema}"."debito_corriente"'), src_engine)
    df_debito_cerrado = pd.read_sql(text(f'SELECT * FROM "{source_schema}"."debito_cerrado"'), src_engine)
    dataframes_dict['credito_corriente'] = df_credito_corriente
    dataframes_dict['credito_cerrado'] = df_credito_cerrado
    dataframes_dict['debito_corriente'] = df_debito_corriente
    dataframes_dict['debito_cerrado'] = df_debito_cerrado

    Conceptos(strategy_folder, data_access).generador_de_conceptos(dataframes_dict)            


if __name__ == "__main__":
    main()
