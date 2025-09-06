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

    def safe_deduplicate_mirror(self, mirror_path: str, df_source: pd.DataFrame, match_columns: list):
            """
            Elimina SOLO duplicados en el MIRROR cuando **la misma llave NO está duplicada en SOURCE**.
            - Respeta columnas no compartidas (etiquetas) y solo quita filas espejo redundantes.
            - No toca SOURCE; no deduplica SOURCE.
            - Mantiene la primera ocurrencia en el mirror para cada llave única en SOURCE.
            """
            print(self.helper.message_print(f"\n🧹 SAFE DEDUPE en mirror (solo donde SOURCE no tiene duplicados): {os.path.basename(mirror_path)}"))
            try:
                df_mirror = pd.read_pickle(mirror_path)
            except Exception as e:
                print(f"❌ No se pudo leer el mirror '{mirror_path}': {e}")
                return

            if df_mirror is None or df_mirror.empty or df_source is None or df_source.empty:
                print("ℹ️ Nada que deduplicar (alguno de los dataframes está vacío).")
                return

            # Normalizar tipos para formar llave compuesta de forma consistente
            def _normalize_string_series(ser: pd.Series) -> pd.Series:
                ser = ser.astype('string')
                ser = ser.str.replace(r"\s+", " ", regex=True).str.strip()
                return ser

            for col in match_columns:
                if col in df_source.columns and col in df_mirror.columns:
                    if pd.api.types.is_datetime64_any_dtype(df_source[col]) or pd.api.types.is_datetime64_any_dtype(df_mirror[col]):
                        df_source[col] = pd.to_datetime(df_source[col], errors='coerce').dt.normalize()
                        df_mirror[col] = pd.to_datetime(df_mirror[col], errors='coerce').dt.normalize()
                    elif pd.api.types.is_numeric_dtype(df_source[col]) or pd.api.types.is_numeric_dtype(df_mirror[col]):
                        df_source[col] = pd.to_numeric(df_source[col], errors='coerce')
                        df_mirror[col] = pd.to_numeric(df_mirror[col], errors='coerce')
                    else:
                        df_source[col] = _normalize_string_series(df_source[col])
                        df_mirror[col] = _normalize_string_series(df_mirror[col])

            # Claves compuestas
            df_source['_key'] = df_source[match_columns].astype(str).agg(' | '.join, axis=1)
            df_mirror['_key'] = df_mirror[match_columns].astype(str).agg(' | '.join, axis=1)

            # Identificar llaves que NO están duplicadas en SOURCE
            src_dup_mask = df_source.duplicated(subset=match_columns, keep=False)
            src_unique_keys = set(df_source.loc[~src_dup_mask, '_key'])

            # En mirror, para esas llaves únicas en SOURCE, eliminar duplicados (mantener primera)
            before = len(df_mirror)
            drop_mask = df_mirror['_key'].isin(src_unique_keys) & df_mirror.duplicated(subset=match_columns, keep='first')
            removed = int(drop_mask.sum())
            if removed > 0:
                df_mirror = df_mirror.loc[~drop_mask].copy()
                df_mirror = df_mirror.drop(columns=['_key'], errors='ignore')
                df_mirror.to_pickle(mirror_path)
                print(self.helper.message_print(f"✅ SAFE DEDUPE: se eliminaron {removed} filas duplicadas SOLO en mirror. ({before} → {len(df_mirror)})"))
            else:
                df_mirror = df_mirror.drop(columns=['_key'], errors='ignore')
                print("ℹ️ SAFE DEDUPE: no se encontraron duplicados a eliminar bajo esta regla.")

    def remove_or_add_rows_safe(self, df_source, mirror_path, match_columns, update_columns, side: str):
            """
            Igual que `remove_or_add_rows` pero con **zona de seguridad**:
            - Si hay claves en el MIRROR que no están en el SOURCE, exporta esas filas a Excel
            (self.excel_credito_perdidos / self.excel_debito_perdidos), abre el archivo y **regresa** sin borrar.
            - Si no hay faltantes en SOURCE, sincroniza: agrega filas nuevas y elimina las sobrantes.
            - Mantiene las columnas no compartidas (etiquetas) del mirror.
            """
            print(self.helper.message_print(f"\n🔄 SAFE SYNC (agregar/quitar con seguridad): {os.path.basename(mirror_path)}"))
            # Cargar mirror
            try:
                df_mirror = pd.read_pickle(mirror_path)
            except Exception as e:
                print(f"❌ No se pudo leer el mirror '{mirror_path}': {e}")
                return

            # Reset index y normalización para llaves
            df_mirror = df_mirror.reset_index(drop=True)
            df_source = df_source.reset_index(drop=True)

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

            # NO desduplicar SOURCE: preserva duplicados
            df_source_unique = df_source.copy()

            # Claves compuestas
            df_source_unique['_composite_key'] = df_source_unique[match_columns].astype(str).agg(' | '.join, axis=1)
            df_mirror['_composite_key'] = df_mirror[match_columns].astype(str).agg(' | '.join, axis=1)

            source_keys = set(df_source_unique['_composite_key'])
            mirror_keys = set(df_mirror['_composite_key'])

            keys_to_add = source_keys - mirror_keys
            keys_to_remove = mirror_keys - source_keys

            print(f"🔍 DEBUG SAFE - Source keys: {len(source_keys)} | Mirror keys: {len(mirror_keys)}")
            print(f"🔍 DEBUG SAFE - Add: {len(keys_to_add)} | Remove: {len(keys_to_remove)}")

            # Zona de seguridad: si SOURCE carece de claves que sí están en MIRROR → exportar y salir
            if keys_to_remove:
                missing_rows = df_mirror[df_mirror['_composite_key'].isin(keys_to_remove)].copy()
                # Ruta de exportación según lado
                if side.lower().startswith('cred'):
                    out_path = self.excel_credito_perdidos
                else:
                    out_path = self.excel_debito_perdidos
                # Conservar columnas útiles para diagnóstico
                cols_export = list(dict.fromkeys(match_columns + ['concepto', 'file_name', 'file_date']))
                for c in cols_export:
                    if c not in missing_rows.columns:
                        missing_rows[c] = None
                missing_rows[cols_export].to_excel(out_path, index=False)
                print(self.helper.message_print(
                    f"⚠️ Zona de seguridad activa: hay {len(keys_to_remove)} claves en MIRROR que no están en SOURCE. "
                    f"Se exportaron a: {out_path}. Revisa el archivo. No se borró nada."
                ))
                try:
                    self.helper.open_xlsx_file(out_path)
                except Exception:
                    pass
                # Salir SIN borrar nada
                return

            # Si no hay faltantes en SOURCE, proceder con sincronización completa
            # Asegurar columnas del source existan en el mirror
            for col in df_source.columns:
                if col not in df_mirror.columns:
                    df_mirror[col] = None

            # Agregar filas nuevas
            if keys_to_add:
                rows_to_add = df_source_unique[df_source_unique['_composite_key'].isin(keys_to_add)].copy()
                rows_to_add = rows_to_add.reindex(columns=df_mirror.columns)
                df_mirror = pd.concat([df_mirror, rows_to_add], ignore_index=True, sort=False)

            # Ordenar según el orden de aparición en SOURCE (opcional)
            key_order = df_source_unique['_composite_key'].tolist()
            pos_map = {k: i for i, k in enumerate(key_order)}
            df_mirror['_sort_order'] = df_mirror['_composite_key'].map(pos_map)
            df_mirror = df_mirror.sort_values('_sort_order').drop(columns=['_composite_key', '_sort_order'])

            # Guardar
            df_mirror.to_pickle(mirror_path)
            print(self.helper.message_print("✅ SAFE SYNC: filas agregadas y espejo mantenido. (No hubo borrados peligrosos)"))


    def export_mirror_excel(self):
        # Cargar pickles (si no existen, usar DF vacío)
        df_credito = pd.read_pickle(self.mirror_credito_path)
        df_debito = pd.read_pickle(self.mirror_debito_path)
        
        print(f"📝 Escribiendo mirrors a Excel: {self.mirror_excel_path}")
        with pd.ExcelWriter(self.mirror_excel_path, engine='openpyxl') as writer:
            # Según especificación: hoja debito <= mirror_credito; hoja credito <= mirror_debito
            df_debito = df_debito.sort_values(by='file_name', ascending=False)
            df_credito = df_credito.sort_values(by='file_name', ascending=False)
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
        
    def _add_numeric_concept_column(self, df: pd.DataFrame, concepto_col: str = 'concepto', new_col: str = 'numeric_concept') -> pd.DataFrame:
        """
        Extrae todos los dígitos del texto en `concepto` (de principio a fin) y los concatena en `numeric_concept`.
        Si no hay dígitos, deja NA. No modifica el DF original (devuelve copia).
        """
        df = df.copy()
        if concepto_col not in df.columns:
            df[new_col] = pd.NA
            return df
        def _extract_digits(x):
            if pd.isna(x):
                return pd.NA
            s = re.sub(r'\D+', '', str(x))
            return s if s else pd.NA
        df[new_col] = df[concepto_col].apply(_extract_digits)
        return df

    def drop_exact_dupes_in_pickle(self, pickle_path: str) -> pd.DataFrame:
        """
        Carga un pickle de mirror, elimina duplicados **idénticos** considerando **todas las columnas**,
        guarda de nuevo y devuelve el DataFrame resultante. Mantiene la **primera** ocurrencia.
        """
        try:
            df = pd.read_pickle(pickle_path)
        except Exception as e:
            print(f"❌ No se pudo leer el mirror '{pickle_path}': {e}")
            return pd.DataFrame()
        if df is None or df.empty:
            print(f"ℹ️ No hay filas para deduplicar en {os.path.basename(pickle_path)}")
            return df
        before = len(df)
        df = df.drop_duplicates(keep='first')  # todas las columnas
        removed = before - len(df)
        if removed > 0:
            print(self.helper.message_print(
                f"🧹 DEDUPE EXACTO: se eliminaron {removed} filas idénticas en {os.path.basename(pickle_path)} ({before} → {len(df)})"
            ))
            df.to_pickle(pickle_path)
        else:
            print(f"ℹ️ DEDUPE EXACTO: {os.path.basename(pickle_path)} no contenía duplicados idénticos.")
        return df

    def funcion_alternativa_update_mirror(self, dict_dataframes):
        """
        Estrategia segura para sincronizar el mirror evitando usar `concepto` puro como llave.
        1) Extrae `numeric_concept` en SOURCE y MIRROR.
        2) Define llaves estrictas con `numeric_concept`:
           - crédito: ['fecha','abono','cargo','numeric_concept']
           - débito : ['fecha','cargos','abonos','numeric_concept']
        3) No elimina duplicados en SOURCE. Si hay duplicados por llave, **solo alerta** y exporta un XLSX de diagnóstico.
        4) Fase 1: Actualiza SOLO columnas descriptivas (file_date, file_name) en el mirror donde las llaves coinciden (sin tocar etiquetas/otras columnas).
        5) Fase 2: Agrega/elimina filas con zona de seguridad: si hay llaves en MIRROR que no aparecen en SOURCE, **exporta** esas filas a Excel (…_perdidos.xlsx), abre el archivo y **regresa** sin borrar nada.
        """
        # 1) Cargar dataframes de source
        df_source_credito, df_source_debito = self.dataframes_from_source(dict_dataframes)

        # 1a) Agregar columna numeric_concept (extrae todos los dígitos de `concepto`)
        df_source_credito = self._add_numeric_concept_column(df_source_credito, concepto_col='concepto', new_col='numeric_concept')
        df_source_debito  = self._add_numeric_concept_column(df_source_debito,  concepto_col='concepto', new_col='numeric_concept')

        # 1b) Asegurar que los MIRROR también tengan numeric_concept
        try:
            df_mirror_credito = pd.read_pickle(self.mirror_credito_path)
        except Exception:
            df_mirror_credito = pd.DataFrame(columns=df_source_credito.columns)
        try:
            df_mirror_debito = pd.read_pickle(self.mirror_debito_path)
        except Exception:
            df_mirror_debito = pd.DataFrame(columns=df_source_debito.columns)

        # 1b.1) Limpieza previa: eliminar duplicados **idénticos** en mirrors (todas las columnas)
        if os.path.exists(self.mirror_credito_path):
            df_mirror_credito = self.drop_exact_dupes_in_pickle(self.mirror_credito_path)
        if os.path.exists(self.mirror_debito_path):
            df_mirror_debito = self.drop_exact_dupes_in_pickle(self.mirror_debito_path)

        df_mirror_credito = self._add_numeric_concept_column(df_mirror_credito, 'concepto', 'numeric_concept')
        df_mirror_debito  = self._add_numeric_concept_column(df_mirror_debito,  'concepto', 'numeric_concept')
        df_mirror_credito.to_pickle(self.mirror_credito_path)
        df_mirror_debito.to_pickle(self.mirror_debito_path)

        # 2) Definir llaves y columnas de actualización
        key_credito = ['fecha', 'abono', 'cargo', 'numeric_concept']
        key_debito  = ['fecha', 'cargos', 'abonos', 'numeric_concept']
        update_columns = ['file_date', 'file_name']

        # 3) Diagnóstico de duplicados **en SOURCE** (no aborta)
        dup_mask_cred = df_source_credito.duplicated(subset=key_credito, keep=False)
        dup_mask_deb  = df_source_debito.duplicated(subset=key_debito,  keep=False)
        ndup_cred = int(dup_mask_cred.sum())
        ndup_deb  = int(dup_mask_deb.sum())
        if ndup_cred > 0:
            duplicated_path = os.path.join(self.strategy_folder, 'credito_duplicado_con_numeric_concept.xlsx')
            print(self.helper.message_print(
                f"⚠️ CRÉDITO: {ndup_cred} filas duplicadas por llave {key_credito}. Exportando diagnóstico → {duplicated_path}"
            ))
            cols_export = list(dict.fromkeys(key_credito + ['concepto', 'tarjeta', 'file_date', 'file_name']))
            df_source_credito.loc[dup_mask_cred, cols_export].to_excel(duplicated_path, index=False)
            try:
                self.helper.open_xlsx_file(duplicated_path)
            except Exception:
                pass
        if ndup_deb > 0:
            duplicated_path = os.path.join(self.strategy_folder, 'debito_duplicado_con_numeric_concept.xlsx')
            print(self.helper.message_print(
                f"⚠️ DÉBITO: {ndup_deb} filas duplicadas por llave {key_debito}. Exportando diagnóstico → {duplicated_path}"
            ))
            cols_export = list(dict.fromkeys(key_debito + ['concepto', 'saldos', 'file_date', 'file_name']))
            df_source_debito.loc[dup_mask_deb, cols_export].to_excel(duplicated_path, index=False)
            try:
                self.helper.open_xlsx_file(duplicated_path)
            except Exception:
                pass

        # 4) Normalización mínima antes de sincronizar (fechas a día y numéricos)
        def _norm(df, fecha_col, nums):
            if fecha_col in df.columns:
                df[fecha_col] = pd.to_datetime(df[fecha_col], errors='coerce').dt.normalize()
            for c in nums:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors='coerce')
            return df
        df_source_credito = _norm(df_source_credito, 'fecha', ['abono', 'cargo'])
        df_source_debito  = _norm(df_source_debito,  'fecha', ['cargos', 'abonos'])

        print(self.helper.message_print("✅ Llaves con numeric_concept listas. Procediendo a sincronización en dos fases."))

        # 5) Fase 1: Actualizar columnas descriptivas (sin agregar/quitar filas)
        self.mirror_file_date_file_name_from_source(
            df_source=df_source_credito,
            mirror_path=self.mirror_credito_path,
            match_columns=key_credito,
            update_columns=update_columns,
            prefer='source_last',
            save=True,
        )
        self.mirror_file_date_file_name_from_source(
            df_source=df_source_debito,
            mirror_path=self.mirror_debito_path,
            match_columns=key_debito,
            update_columns=update_columns,
            prefer='source_last',
            save=True,
        )

        # 6) Fase 2: Agregar/eliminar filas con zona de seguridad
        self.remove_or_add_rows_safe(
            df_source=df_source_credito,
            mirror_path=self.mirror_credito_path,
            match_columns=key_credito,
            update_columns=update_columns,
            side='credito'
        )
        self.remove_or_add_rows_safe(
            df_source=df_source_debito,
            mirror_path=self.mirror_debito_path,
            match_columns=key_debito,
            update_columns=update_columns,
            side='debito'
        )

        # 6.1) Limpieza opcional: si en SOURCE la llave es única, deduplicar mirror para esa llave
        self.safe_deduplicate_mirror(self.mirror_credito_path, df_source_credito, key_credito)
        self.safe_deduplicate_mirror(self.mirror_debito_path, df_source_debito, key_debito)

        print(self.helper.message_print("🎉 Mirror sincronizado usando numeric_concept como parte de la llave (sin perder etiquetas)."))


    def actualiza(self, dict_dataframes):
        print(self.helper.message_print("\n🔄 Actualizando mirrors desde archivos de Conceptos temporales…"))
        df_source_credito, df_source_debito = self.dataframes_from_source(dict_dataframes)
        column_key_credito = ['fecha', 'concepto', 'abono', 'cargo', 'tarjeta']
        column_key_debito = ['fecha', 'concepto', 'cargos', 'abonos', 'saldos']
        update_columns = ['file_date', 'file_name']
        self.funcion_alternativa_update_mirror(dict_dataframes)        
        # 1 Ejecutar mirror (que ahora primero sincroniza file_name/file_date por llave, luego agrega/elimina si hace falta)
        #self.mirror_file_date_file_name_from_source(df_source_credito, self.mirror_credito_path, column_key_credito, update_columns)
        #self.mirror_file_date_file_name_from_source(df_source_debito, self.mirror_debito_path, column_key_debito, update_columns)
        # 2 Agrega o quita filas
        ##self.remove_or_add_rows(df_source_debito, self.mirror_debito_path, column_key_debito, update_columns)
        #self.remove_or_add_rows(df_source_credito, self.mirror_credito_path, column_key_credito, update_columns)
        # En este momento, tenemos los mimso rows en source y mirror, y file_name/file_date actualizados
        # 3 Actualizar las columnas de conceptos (beneficiario, categoria, grupo,
        update_extra_columns = False
        update_extra_columns = self.extra_columns_management()
        # Exportar a Excel al final
        print("📝 Exportando Mirror.xlsx tras actualizar ambos mirrors…")
        self.export_mirror_excel()
        return True
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

        # ALERTA: duplicados en el Excel de conceptos por la llave → exportar diagnóstico
        dup_mask_excel = conceptos_df.duplicated(subset=match_columns, keep=False)
        if int(dup_mask_excel.sum()) > 0:
            side = 'credito' if 'credito' in os.path.basename(mirror_path).lower() else 'debito'
            dup_path = os.path.join(self.strategy_folder, f'duplicados_en_excel_{side}.xlsx')
            print(self.helper.message_print(
                f"⚠️ Se detectaron {int(dup_mask_excel.sum())} filas duplicadas por llave en el Excel de conceptos. Exportando → {dup_path}"
            ))
            conceptos_df.loc[dup_mask_excel, match_columns + [c for c in conceptos_df.columns if c not in match_columns]].to_excel(dup_path, index=False)
            try:
                self.helper.open_xlsx_file(dup_path)
            except Exception:
                pass
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
        (LEGACY: sincronización SIN zona de seguridad, puede eliminar filas del mirror)
        Adjusts df_mirror to have exactly the same rows as df_source based on match_columns:
        - Treats rows as unique keys formed by match_columns.
        - Removes rows from df_mirror where the key is not in df_source.
        - Adds rows from df_source where the key is not in df_mirror (copying all columns).
        - Adds missing columns from df_source to df_mirror.
        - Preserves order by sorting df_mirror to match df_source's key order.
        - Includes debug/verification to confirm exact match.
        NOTE: Does not touch conceptual columns already present in mirror; new rows get those columns as None.
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

        # Normalize dtypes for match_columns
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

        # Keys to add/remove
        keys_to_add = source_keys - mirror_keys
        keys_to_remove = mirror_keys - source_keys

        # Debug: Before changes
        print(f"🔍 DEBUG - Before: Source has {len(source_keys)} unique keys, Mirror has {len(mirror_keys)} unique keys.")
        print(f"🔍 DEBUG - Keys to add: {len(keys_to_add)}, Keys to remove: {len(keys_to_remove)}")

        # Ensure mirror has all source columns; preserve existing conceptual columns
        for col in df_source.columns:
            if col not in df_mirror.columns:
                df_mirror[col] = None

        # Remove rows from df_mirror
        if keys_to_remove:
            df_mirror = df_mirror[~df_mirror['_composite_key'].isin(keys_to_remove)]

        # Add rows from df_source_unique (copy all columns)
        if keys_to_add:
            rows_to_add = df_source_unique[df_source_unique['_composite_key'].isin(keys_to_add)].copy()
            # Align to mirror's columns so conceptual columns exist (as None) for new rows
            rows_to_add = rows_to_add.reindex(columns=df_mirror.columns)
            df_mirror = pd.concat([df_mirror, rows_to_add], ignore_index=True, sort=False)

        # Sort df_mirror to match the order of keys in df_source_unique
        key_order = df_source_unique['_composite_key'].tolist()
        pos_map = {key: i for i, key in enumerate(key_order)}
        df_mirror['_sort_order'] = df_mirror['_composite_key'].map(pos_map)
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
                print(f"🔍 DEBUG - Extra keys in Mirror: {list(extra_in_mirror)[:5]}...")
            if extra_in_source:
                print(f"🔍 DEBUG - Missing keys in Mirror: {list(extra_in_source)[:5]}...")

        # Save updated mirror
        df_mirror.to_pickle(mirror_path)
        print(f"✅ Mirror actualizado: {len(keys_to_add)} filas agregadas, {len(keys_to_remove)} filas eliminadas. Guardado en {mirror_path}")

    def mirror_file_date_file_name_from_source(self, df_source, mirror_path, match_columns, update_columns, prefer='source_last', save=True):
        """
        Sincroniza en el mirror las columnas de `update_columns` para las filas que hagan match por `match_columns`.
        No agrega ni elimina filas: solo ACTUALIZA valores en columnas descriptivas (p.ej. file_name, file_date).
        La preferencia de duplicados en el SOURCE se resuelve con `prefer` ('source_last' | 'source_first').
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

        # ===== Normalizar dtypes para llaves =====
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

        # ===== Resolver duplicados en SOURCE por llave =====
        src = df_source.copy()
        # Si existe file_date, ordenar por file_date y luego por índice para estabilidad
        if 'file_date' in src.columns:
            src = src.sort_values(['file_date']).reset_index(drop=True)
        else:
            src = src.reset_index(drop=True)

        keep = 'last' if prefer == 'source_last' else 'first'
        src_dedup = src.drop_duplicates(subset=match_columns, keep=keep)

        # ===== Hacer merge para obtener valores de actualización =====
        merged = pd.merge(
            df_mirror.reset_index(),
            src_dedup[match_columns + update_columns],
            on=match_columns,
            how='left',
            suffixes=('', '_src')
        )

        if merged.empty:
            print(f"ℹ️ [{side}] No hubo filas con match para actualizar.")
            return

        # ===== Aplicar actualizaciones solo donde haya valor en source y haya cambio =====
        updated_rows = 0
        updated_cells = 0
        for upd_col in update_columns:
            src_col = f"{upd_col}_src"
            if src_col not in merged.columns:
                continue
            mask = merged[src_col].notna() & (merged[upd_col] != merged[src_col])
            if mask.any():
                df_mirror.loc[merged.loc[mask, 'index'], upd_col] = merged.loc[mask, src_col]
                updated_cells += int(mask.sum())
        updated_rows = merged[update_columns].ne(merged[[f"{c}_src" for c in update_columns]].values).any(axis=1).sum()

        print(f"🔍 DEBUG [{side}] - Updated cells: {updated_cells}, Updated rows (any col): {int(updated_rows)}")

        if save and updated_cells > 0:
            df_mirror.to_pickle(mirror_path)
            print(f"💾 [{side}] Guardado mirror: {mirror_path}")

        # ===== (Opcional) Informe de filenames sólo como diagnóstico; no bloquea el flujo =====
        try:
            mirror_filenames = set(df_mirror['file_name'].dropna().unique()) if 'file_name' in df_mirror.columns else set()
            source_filenames = set(src_dedup['file_name'].dropna().unique()) if 'file_name' in src_dedup.columns else set()
            if mirror_filenames != source_filenames:
                only_in_mirror = mirror_filenames - source_filenames
                only_in_source = source_filenames - mirror_filenames
                print(f"ℹ️ [{side}] Aviso: conjuntos de file_name difieren tras actualizar (esto es esperado si aún no se agregan/eliminan filas).")
                print(f"   • Solo en MIRROR: {sorted(only_in_mirror)}")
                print(f"   • Solo en SOURCE: {sorted(only_in_source)}")
        except Exception:
            pass

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

    