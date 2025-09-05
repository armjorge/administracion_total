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
        
        # Ejecutar mirror (que ahora primero sincroniza file_name/file_date por llave, luego agrega/elimina si hace falta)
        self.mirror(df_source_credito, self.mirror_credito_path, column_key_credito, update_columns)
        self.mirror(df_source_debito, self.mirror_debito_path, column_key_debito, update_columns)
        # Exportar a Excel al final
        print("📝 Exportando Mirror.xlsx tras actualizar ambos mirrors…")
        self.export_mirror_excel()

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

        side = 'CRÉDITO' if 'credito' in os.path.basename(mirror_path) else 'DÉBITO'
        
        # Get unique filenames from both dataframes
        unique_filename_source = set(df_source['file_name'].dropna().unique())
        unique_filename_mirror = set(df_mirror['file_name'].dropna().unique())
        
        # Files in both (need to update)
        files_to_update = unique_filename_source ^ unique_filename_mirror
        
        if not files_to_update:
            print(f"[{side}] No hay archivos comunes para actualizar")
            return
        
        print(f"[{side}] Archivos a actualizar: {list(files_to_update)}")
        
        # Process files to update only
        for filename in files_to_update:
            mirror_rows = df_mirror[df_mirror['file_name'] == filename].copy()
            source_rows = df_source[df_source['file_name'] == filename].copy()
            
            print(f"[{side}] Actualizando filas para archivo: {filename}")
            updated_count = 0
            
            # Update mirror rows row by row
            for mirror_idx, mirror_row in mirror_rows.iterrows():
                # Get the actual index in df_mirror
                actual_mirror_idx = df_mirror[
                    (df_mirror['file_name'] == filename) & 
                    (df_mirror[match_columns[0]] == mirror_row[match_columns[0]])
                ].index
                
                if len(actual_mirror_idx) > 0:
                    actual_idx = actual_mirror_idx[0]
                    # Create search criteria using match_columns
                    search_criteria = True
                    for col in match_columns:
                        search_criteria = search_criteria & (source_rows[col] == mirror_row[col])
                    
                    matching_source = source_rows[search_criteria]
                    
                    if not matching_source.empty:
                        # Update the row in df_mirror with values from source
                        for update_col in update_columns:
                            if update_col in matching_source.columns:
                                df_mirror.loc[actual_idx, update_col] = matching_source[update_col].iloc[0]
                                updated_count += 1
            
            if updated_count > 0:
                print(f"💡 [{side}] Sincronizadas columnas {update_columns} por llave (celdas cambiadas={updated_count})")
        
        # Save the updated mirror
        try:
            df_mirror.to_pickle(mirror_path)
            print(f"💾 Guardado: {mirror_path}")
        except Exception as e:
            print(f"❌ No se pudo guardar el mirror en '{mirror_path}': {e}")
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
