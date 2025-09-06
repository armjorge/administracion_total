import os  # Import the os module
#from utils.helpers import Helper  # Import the Helper class
import yaml
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
from glob import glob
import numpy as np
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
    def load_local_pickle(self, path, df_source):
        # Load mirror
        try:
            df_mirror = pd.read_pickle(mirror_path)
        except Exception as e:
            print(f"❌ No se pudo leer el mirror '{mirror_path}': {e}")
            return

        # Reset index to ensure unique indices (fixes concat error)
        df_mirror = df_mirror.reset_index(drop=True)
        df_source = df_source.reset_index(drop=True)

        # Normalize dtypes for match_columns (similar to mirror_file_date_file_name_from_source)
        def _normalize_string_series(ser: pd.Series) -> pd.Series:
            ser = ser.astype('string')
            ser = ser.str.replace(r"\s+", " ", regex=True).str.strip()
            return ser

        for col in match_columns:
            if pd.api.types.is_datetime64_any_dtype(df_source[col]):
                df_source[col] = pd.to_datetime(df_source[col], errors='coerce').dt.normalize()
                df_mirror[col] = pd.to_datetime(df_mirror[col], errors='coerce').dt.normalize()
            elif pd.api.types.is_numeric_dtype(df_source[col]):
                df_source[col] = pd.to_numeric(df_source[col], errors='coerce')
                df_mirror[col] = pd.to_numeric(df_mirror[col], errors='coerce')
            else:
                df_source[col] = _normalize_string_series(df_source[col])
                df_mirror[col] = _normalize_string_series(df_mirror[col])
        return df_mirror
               
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
        print("✅ Dataframes espejo ya existen localmente como pickles.")

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
        print("Nuestro espejo tiene las mismas filas y columnas que la fuente en SQL, procedemos a cargar a SQL para su minería.")
        return True
        
      

    def actualiza(self, dict_dataframes):
        print(self.helper.message_print("\n🔄 Actualizando mirrors desde archivos de Conceptos temporales…"))
        df_source_credito, df_source_debito = self.dataframes_from_source(dict_dataframes)
        column_key_credito = ['fecha', 'concepto', 'abono', 'cargo', 'tarjeta']
        column_key_debito = ['fecha', 'concepto', 'cargos', 'abonos', 'saldos']
        update_columns = ['file_date', 'file_name']
        
        # 1 Ejecutar mirror (que ahora primero sincroniza file_name/file_date por llave, luego agrega/elimina si hace falta)
        self.mirror_file_date_file_name_from_source(df_source_credito, self.mirror_credito_path, column_key_credito, update_columns)
        self.mirror_file_date_file_name_from_source(df_source_debito, self.mirror_debito_path, column_key_debito, update_columns)
        # 2 Agrega o quita filas
        self.remove_or_add_rows(df_source_debito, self.mirror_debito_path, column_key_debito, update_columns)
        self.remove_or_add_rows(df_source_credito, self.mirror_credito_path, column_key_credito, update_columns)
        # En este momento, tenemos los mimso rows en source y mirror, y file_name/file_date actualizados
        # 3 Actualizar las columnas de conceptos (beneficiario, categoria, grupo,
        update_extra_columns = False
        update_extra_columns = self.extra_columns_management()
        # Exportar a Excel al final
        print("📝 Exportando Mirror.xlsx tras actualizar ambos mirrors…")
        self.export_mirror_excel()

    def extra_columns_management(self):
        print(self.helper.message_print("\n🔄 los Pickle Locales ya tienen la misma información que la fuente SQL"))
        print("""
        Ahora vamos a agregar las columnas conceptuales (beneficiario, categoria, grupo, clave_presupuestal, concepto_procesado)
              1) Selecciona para llenar crédito y 2) para débito  
              """ )
        column_key_credito = ['fecha', 'concepto', 'abono', 'cargo', 'tarjeta']
        column_key_debito = ['fecha', 'concepto', 'cargos', 'abonos', 'saldos']       
        conceptual_columns = pd.read_excel(self.conceptos_master).columns.tolist()

        self.mirror_credito_path = os.path.join(self.strategy_folder, 'mirror_credito.pkl')
        self.mirror_debito_path = os.path.join(self.strategy_folder, 'mirror_debito.pkl')        
        self.excel_credit_conceptos
        self.excel_debito_conceptos
        # Load mirror
        try:
            df_mirror_credito = pd.read_pickle(self.mirror_credito_path)
        except Exception as e:
            print(f"❌ No se pudo leer el mirror '{self.mirror_credito_path}': {e}")
            return
        try:
            df_mirror_debito = pd.read_pickle(self.mirror_debito_path)
        except Exception as e:
            print(f"❌ No se pudo leer el mirror '{self.mirror_debito_path}': {e}")
            return
        if df_mirror_credito is not None and not df_mirror_credito.empty: 
            self.helper.message_print("\n🔄 Abriendo Excel para que agregues/edites las columnas conceptuales en CRÉDITO…")
            df_mirror_credito.to_excel(self.excel_credit_conceptos, index=False)
            self.helper.open_xlsx_file(self.excel_credit_conceptos)
            input("\nPresiona Enter cuando hayas terminado de editar el archivo de crédito...")
            self.add_conceptual_columns(self.mirror_credito_path, column_key_credito, self.excel_credit_conceptos, conceptual_columns)
        if df_mirror_debito is not None and not df_mirror_debito.empty:
            self.helper.message_print("\n🔄 Abriendo Excel para que agregues/edites las columnas conceptuales en DÉBITO…")
            df_mirror_debito.to_excel(self.excel_debito_conceptos, index=False)
            self.helper.open_xlsx_file(self.excel_debito_conceptos)
            input("\nPresiona Enter cuando hayas terminado de editar el archivo de débito...")
            self.add_conceptual_columns(self.mirror_debito_path, column_key_debito, self.excel_debito_conceptos, conceptual_columns)



    def add_conceptual_columns(self, mirror_path, match_columns, excel_with_concepts, conceptual_columns):
        """
        Adds/updates conceptual columns from excel_with_concepts to df_mirror based on match_columns.
        - Loads conceptos_df from Excel.
        - Matches rows on match_columns and updates conceptual_columns in df_mirror.
        - Adds missing columns if needed.
        - Saves the updated mirror.
        """
        print(self.helper.message_print(f"\n🔄 Agregando columnas conceptuales a mirror: {os.path.basename(mirror_path)}"))
        
        # Load mirror
        try:
            df_mirror = pd.read_pickle(mirror_path)
        except Exception as e:
            print(f"❌ No se pudo leer el mirror '{mirror_path}': {e}")
            return

        # Load conceptos_df from Excel
        try:
            conceptos_df = pd.read_excel(excel_with_concepts)
        except Exception as e:
            print(f"❌ No se pudo leer el Excel '{excel_with_concepts}': {e}")
            return

        # Reset index
        df_mirror = df_mirror.reset_index(drop=True)
        conceptos_df = conceptos_df.reset_index(drop=True)

        # Normalize dtypes for match_columns
        def _normalize_string_series(ser: pd.Series) -> pd.Series:
            ser = ser.astype('string')
            ser = ser.str.replace(r"\s+", " ", regex=True).str.strip()
            return ser

        for col in match_columns:
            if pd.api.types.is_datetime64_any_dtype(conceptos_df[col]):
                conceptos_df[col] = pd.to_datetime(conceptos_df[col], errors='coerce').dt.normalize()
                df_mirror[col] = pd.to_datetime(df_mirror[col], errors='coerce').dt.normalize()
            elif pd.api.types.is_numeric_dtype(conceptos_df[col]):
                conceptos_df[col] = pd.to_numeric(conceptos_df[col], errors='coerce')
                df_mirror[col] = pd.to_numeric(df_mirror[col], errors='coerce')
            else:
                conceptos_df[col] = _normalize_string_series(conceptos_df[col])
                df_mirror[col] = _normalize_string_series(df_mirror[col])

        # Drop duplicates in conceptos_df based on match_columns
        conceptos_df = conceptos_df.drop_duplicates(subset=match_columns, keep='first')

        # Create composite keys for matching
        df_mirror['_composite_key'] = df_mirror[match_columns].astype(str).agg(' | '.join, axis=1)
        conceptos_df['_composite_key'] = conceptos_df[match_columns].astype(str).agg(' | '.join, axis=1)

        # Add missing conceptual_columns to df_mirror
        for col in conceptual_columns:
            if col not in df_mirror.columns:
                df_mirror[col] = None

        # Merge to get conceptual values (left join on df_mirror)
        # Merge to get conceptual values (left join on df_mirror)
        merged = pd.merge(
            df_mirror,
            conceptos_df[match_columns + conceptual_columns + ['_composite_key']],
            on='_composite_key',
            how='left',
            suffixes=('', '_conceptos')
        )

        # Debug: Number of matches
        matches = merged['_composite_key'].notna().sum()
        print(f"🔍 DEBUG - Found {matches} matching rows between Mirror and Excel.")

        # Update conceptual_columns in df_mirror with non-NaN values from Excel
        updated_cells = 0
        for col in conceptual_columns:
            conceptos_col = f'{col}_conceptos'
            if conceptos_col in merged.columns:
                mask = merged[conceptos_col].notna()  # Update if Excel value is not NaN
                if mask.any():
                    df_mirror.loc[mask, col] = merged.loc[mask, conceptos_col]
                    updated_cells += mask.sum()

        # Clean up
        df_mirror = df_mirror.drop(columns=['_composite_key'], errors='ignore')

        # Debug
        print(f"🔍 DEBUG - Updated {updated_cells} conceptual cells in mirror.")

        # Save updated mirror
        df_mirror.to_pickle(mirror_path)
        print(f"✅ Columnas conceptuales agregadas/actualizadas. Guardado en {mirror_path}")

    def remove_or_add_rows(self, df_source, mirror_path, match_columns, update_columns):
        """
        Adjusts df_mirror to have exactly the same rows as df_source based on match_columns:
        - Treats rows as unique keys formed by match_columns.
        - Removes rows from df_mirror where the key is not in df_source.
        - Adds rows from df_source where the key is not in df_mirror (copying all columns).
        - Adds missing columns from df_source to df_mirror.
        - Preserves order by sorting df_mirror to match df_source's key order.
        - Includes debug/verification to confirm exact match.
        """
        print(self.helper.message_print(f"\n🔄 Eliminando y agregando filas entre source y mirror: {os.path.basename(mirror_path)}"))
        # Load mirror
        try:
            df_mirror = pd.read_pickle(mirror_path)
        except Exception as e:
            print(f"❌ No se pudo leer el mirror '{mirror_path}': {e}")
            return

        # Reset index to ensure unique indices (fixes concat error)
        df_mirror = df_mirror.reset_index(drop=True)
        df_source = df_source.reset_index(drop=True)

        # Normalize dtypes for match_columns (similar to mirror_file_date_file_name_from_source)
        def _normalize_string_series(ser: pd.Series) -> pd.Series:
            ser = ser.astype('string')
            ser = ser.str.replace(r"\s+", " ", regex=True).str.strip()
            return ser

        for col in match_columns:
            if pd.api.types.is_datetime64_any_dtype(df_source[col]):
                df_source[col] = pd.to_datetime(df_source[col], errors='coerce').dt.normalize()
                df_mirror[col] = pd.to_datetime(df_mirror[col], errors='coerce').dt.normalize()
            elif pd.api.types.is_numeric_dtype(df_source[col]):
                df_source[col] = pd.to_numeric(df_source[col], errors='coerce')
                df_mirror[col] = pd.to_numeric(df_mirror[col], errors='coerce')
            else:
                df_source[col] = _normalize_string_series(df_source[col])
                df_mirror[col] = _normalize_string_series(df_mirror[col])

        # Drop duplicates in df_source based on match_columns (keep first) and copy
        df_source_unique = df_source.drop_duplicates(subset=match_columns, keep='first').copy()

        # Create composite keys for matching
        df_source_unique['_composite_key'] = df_source_unique[match_columns].astype(str).agg(' | '.join, axis=1)
        df_mirror['_composite_key'] = df_mirror[match_columns].astype(str).agg(' | '.join, axis=1)

        # Get sets of keys
        source_keys = set(df_source_unique['_composite_key'])
        mirror_keys = set(df_mirror['_composite_key'])

        # Keys to add (in source but not mirror)
        keys_to_add = source_keys - mirror_keys
        # Keys to remove (in mirror but not source)
        keys_to_remove = mirror_keys - source_keys

        # Debug: Before changes
        print(f"🔍 DEBUG - Before: Source has {len(source_keys)} unique keys, Mirror has {len(mirror_keys)} unique keys.")
        print(f"🔍 DEBUG - Keys to add: {len(keys_to_add)}, Keys to remove: {len(keys_to_remove)}")

        # Add missing columns from df_source
        for col in df_source.columns:
            if col not in df_mirror.columns:
                df_mirror[col] = None

        # Remove rows from df_mirror
        if keys_to_remove:
            df_mirror = df_mirror[~df_mirror['_composite_key'].isin(keys_to_remove)]

        # Add rows from df_source_unique (copy all columns)
        if keys_to_add:
            rows_to_add = df_source_unique[df_source_unique['_composite_key'].isin(keys_to_add)]
            df_mirror = pd.concat([df_mirror, rows_to_add], ignore_index=True, sort=False)

        # Sort df_mirror to match the order of keys in df_source_unique
        key_order = df_source_unique['_composite_key'].tolist()
        df_mirror['_sort_order'] = df_mirror['_composite_key'].map({key: i for i, key in enumerate(key_order)})
        df_mirror = df_mirror.sort_values('_sort_order').drop(columns=['_composite_key', '_sort_order'])

        # Verification/Debug: After changes
        df_mirror['_composite_key'] = df_mirror[match_columns].astype(str).agg(' | '.join, axis=1)
        mirror_keys_after = set(df_mirror['_composite_key'])
        df_mirror = df_mirror.drop(columns=['_composite_key'])

        if mirror_keys_after == source_keys:
            print(f"✅ DEBUG - Verification passed: Mirror now has exactly {len(mirror_keys_after)} unique keys matching Source.")
        else:
            print(f"❌ DEBUG - Verification failed: Mirror has {len(mirror_keys_after)} keys, Source has {len(source_keys)}.")
            extra_in_mirror = mirror_keys_after - source_keys
            extra_in_source = source_keys - mirror_keys_after
            if extra_in_mirror:
                print(f"🔍 DEBUG - Extra keys in Mirror: {list(extra_in_mirror)[:5]}...")  # Show first 5
            if extra_in_source:
                print(f"🔍 DEBUG - Missing keys in Mirror: {list(extra_in_source)[:5]}...")

        # Save updated mirror
        df_mirror.to_pickle(mirror_path)
        print(f"✅ Mirror actualizado: {len(keys_to_add)} filas agregadas, {len(keys_to_remove)} filas eliminadas. Guardado en {mirror_path}")

    def mirror_file_date_file_name_from_source(self, df_source, mirror_path, match_columns, update_columns, prefer='source_last', save=True):
        """
        Actualiza en el mirror (pickle) las columnas de `update_columns` (p.ej. file_name, file_date)
        solo cuando exista una fila equivalente en df_source definida por `match_columns`.

        Reglas:
        - No agrega ni elimina filas.
        - Si hay duplicados en df_source por la llave, se resuelven con `prefer`:
            - 'source_last'  -> conserva la última aparición (útil si file_date crece)
            - 'source_first' -> conserva la primera
        - Es vectorizado (merge + np.where) y reporta celdas/filas cambiadas.
        """

        # ===== Cargar mirror =====
        try:
            df_mirror = pd.read_pickle(mirror_path)
        except Exception as e:
            print(f"❌ No se pudo leer el mirror '{mirror_path}': {e}")
            return

        # ===== Validaciones =====
        if not isinstance(df_source, pd.DataFrame):
            print("⚠️ df_source no es un DataFrame válido")
            return
        #df_mirror.info()
        #df_source.info()
        
        missing_m_in_mirror = [c for c in match_columns if c not in df_mirror.columns]
        missing_m_in_source = [c for c in match_columns if c not in df_source.columns]
        missing_u_in_mirror  = [c for c in update_columns if c not in df_mirror.columns]
        missing_u_in_source  = [c for c in update_columns if c not in df_source.columns]

        if missing_m_in_mirror:
            print("⚠️ Columnas de llave faltantes en mirror:", missing_m_in_mirror); return
        if missing_m_in_source:
            print("⚠️ Columnas de llave faltantes en source:", missing_m_in_source); return
        if missing_u_in_mirror:
            print("⚠️ Columnas a actualizar faltantes en mirror:", missing_u_in_mirror); return
        if missing_u_in_source:
            print("⚠️ Columnas a actualizar faltantes en source:", missing_u_in_source); return

        side = 'CRÉDITO' if 'credito' in os.path.basename(mirror_path).lower() else 'DÉBITO'
        updated_cells = 0
        updated_rows = 0
        # ===== Normalizar dtypes para llaves (evitar object vs datetime, etc.) =====
        def _normalize_string_series(ser: pd.Series) -> pd.Series:
            ser = ser.astype('string')  # dtype de cadenas que preserva NA
            ser = ser.str.replace(r"\s+", " ", regex=True).str.strip()
            return ser

        for col in match_columns:
            # Si en el SOURCE es datetime, forzamos ambos a datetime y normalizamos fecha (sin hora)
            if pd.api.types.is_datetime64_any_dtype(df_source[col]):
                df_source[col] = pd.to_datetime(df_source[col], errors='coerce').dt.normalize()
                df_mirror[col] = pd.to_datetime(df_mirror[col], errors='coerce').dt.normalize()
            # Si en el SOURCE es numérico, forzamos ambos a numéricos
            elif pd.api.types.is_numeric_dtype(df_source[col]):
                df_source[col] = pd.to_numeric(df_source[col], errors='coerce')
                df_mirror[col] = pd.to_numeric(df_mirror[col], errors='coerce')
            # En caso contrario lo tratamos como texto (normalizamos espacios)
            else:
                df_source[col] = _normalize_string_series(df_source[col])
                df_mirror[col] = _normalize_string_series(df_mirror[col])

        # ===== Actualizar filas con matching match_columns (incluyendo todas, no solo files_to_update) =====
        unique_filename_source = set(df_source['file_name'].dropna().unique())
        unique_filename_mirror = set(df_mirror['file_name'].dropna().unique())
        files_to_update = unique_filename_source ^ unique_filename_mirror

        if not files_to_update:
            print(f"ℹ️ [{side}] No hay file_name desalineados entre source y mirror. Nada que actualizar por filename.")
        else:
            # Use all rows for matching (not filtered to files_to_update) to catch rows with same key but different file_name
            mirror_rows = df_mirror.copy()
            source_rows = df_source.copy()
            
            # Create composite key for matching (concatenate match_columns)
            mirror_rows['_composite_key'] = mirror_rows[match_columns].astype(str).agg(' | '.join, axis=1)
            source_rows['_composite_key'] = source_rows[match_columns].astype(str).agg(' | '.join, axis=1)
            
            # Debug: Before merge
            print(f"🔍 DEBUG [{side}] - Mirror rows: {len(mirror_rows)}, Source rows: {len(source_rows)}")
            
            # Merge on composite key (left join to update all mirror rows with matches in source)
            merged = pd.merge(
                mirror_rows.reset_index(),
                source_rows[match_columns + update_columns + ['_composite_key']],
                on='_composite_key',
                how='left',
                suffixes=('', '_source')
            )
            
            # Filter to rows with a match in source
            merged = merged[merged['_composite_key'].notna()]
            
            # Debug: After merge
            print(f"🔍 DEBUG [{side}] - Merged rows with matches: {len(merged)}")
            
            if merged.empty:
                print(f"ℹ️ [{side}] No matches found for update.")
            else:
                # Update df_mirror in place for update_columns (always update to source values for matched rows)
                for upd_col in update_columns:
                    source_col = f'{upd_col}_source'
                    if source_col in merged.columns:
                        df_mirror.loc[merged['index'], upd_col] = merged[source_col]
                        updated_cells += len(merged)
                
                updated_rows = len(merged['index'].unique())
                
                # Debug: Update summary
                print(f"🔍 DEBUG [{side}] - Updated cells: {updated_cells}, Updated rows: {updated_rows}")
                
                if updated_cells > 0 and save:
                    df_mirror.to_pickle(mirror_path)
                    print(f"💾 [{side}] Guardado mirror: {mirror_path}")

            ##########
            ##########
            ##########
            #df_mirror.to_pickle(mirror_path)
            df_mirror = pd.read_pickle(mirror_path) 
            
            # Get unique file_name values from both dataframes
            mirror_filenames = set(df_mirror['file_name'].dropna().unique())
            source_filenames = set(df_source['file_name'].dropna().unique())
            
            # Confirma que los grupos por file_name son los mismos
            if mirror_filenames != source_filenames:
                print(f"❌ Los file_name del espejo y la fuente no coinciden. No se puede continuar, para este punto ya esperábamos tener actualizado todo lo actualizable")
                
                # Debug: Show what didn't match
                only_in_mirror = mirror_filenames - source_filenames
                only_in_source = source_filenames - mirror_filenames
                
                print(f"🔍 DEBUG - Archivos solo en MIRROR: {sorted(only_in_mirror)}")
                print(f"🔍 DEBUG - Archivos solo en SOURCE: {sorted(only_in_source)}")
                
                # Let's also see if there are matching rows between these different filenames
                if only_in_mirror and only_in_source:
                    print(f"🔍 DEBUG - Comparando contenido entre archivos diferentes...")
                    for mirror_file in only_in_mirror:
                        mirror_subset = df_mirror[df_mirror['file_name'] == mirror_file][match_columns]
                        print(f"     Mirror file '{mirror_file}': {len(mirror_subset)} filas")
                        
                        for source_file in only_in_source:
                            source_subset = df_source[df_source['file_name'] == source_file][match_columns]
                            print(f"     Source file '{source_file}': {len(source_subset)} filas")
                            
                            # Check if they have the same content (excluding file_name)
                            if len(mirror_subset) == len(source_subset):
                                # Create composite keys for comparison
                                mirror_keys = mirror_subset.apply(tuple, axis=1).sort_values()
                                source_keys = source_subset.apply(tuple, axis=1).sort_values()
                                
                                if mirror_keys.reset_index(drop=True).equals(source_keys.reset_index(drop=True)):
                                    print(f"     ✅ CONTENIDO IDÉNTICO entre '{mirror_file}' y '{source_file}' - El update debería haber funcionado!")
                                else:
                                    print(f"     ❌ Contenido diferente entre '{mirror_file}' y '{source_file}'")
                
                print(f"Mirror filenames: {sorted(mirror_filenames)}")
                print(f"Source filenames: {sorted(source_filenames)}")

                # ===== Resumen =====
                print("—" * 60)
                print(f"✅ [{side}] Resumen actualización por llave {match_columns}:")
                #print(f"   • Celdas cambiadas: {cells_changed}")
                #print(f"   • Filas con al menos un cambio: {rows_changed}")
                print(f"   • Filas mirror sin match en source: {len(only_in_mirror)}")
                print(f"   • Filas source sin match en mirror: {len(only_in_source)}")
                print("—" * 60)            
                return

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

    if Conceptos(strategy_folder, data_access).generador_de_conceptos(dataframes_dict):
        print("✅ Proceso de generación de conceptos finalizado exitosamente.")
    return True


if __name__ == "__main__":
    main()
