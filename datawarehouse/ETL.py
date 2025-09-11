import os  # Import the os module
#from utils.helpers import Helper  # Import the Helper class
import yaml
from sqlalchemy import all_, create_engine, text, true
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



class ETL:
    def __init__(self, folder_root):
        self.folder_root = folder_root
        self.strategy_folder = os.path.join(folder_root, "Implementación", "Estrategia")
        self.helper = Helper()
        self.mirror_credito_path = os.path.join(self.strategy_folder, 'mirror_credito.pkl')
        self.mirror_debito_path = os.path.join(self.strategy_folder, 'mirror_debito.pkl')
        self.conceptos_master = os.path.join(self.strategy_folder, 'Conceptos.xlsx')
        self.mirror_excel_path = os.path.join(self.strategy_folder, 'Mirror.xlsx')
        self.source_excel = os.path.join(self.strategy_folder, 'Source_ETL.xlsx')
        
        self.conceptos_temporal_path = os.path.join(self.strategy_folder, 'Conceptos temporales')
        self.excel_credit_conceptos = os.path.join(self.conceptos_temporal_path, "credito","credito_corriente.xlsx")
        self.excel_debito_conceptos = os.path.join(self.conceptos_temporal_path, "debito", "debito_corriente.xlsx")
        self.data_access = None
        self.dataframes_dict = {}
        self.estates_per_year = {} #_cerrado o _corriente por año, por ejemplo 2023: ['cerrado', 'corriente']
        self.all_slices = {} # Diccionario con todos los slices generados en transformación.
        self.columns_to_upload = ['fecha', 'concepto', 'cargo', 'abono', 'saldo', 'file_date', 'file_name', 'estado', 'cuenta', 'unic_concept']
   

    def extraction(self):
        dataframes_dict = {}
        self.source_url = self.data_access['sql_url']
        self.source_schema = 'banorte_lake' 
        print(f"\tFolder de trabajo {os.path.basename(self.strategy_folder)}")
        print(f"\tConectando a la fuente de datos SOURCE {self.source_schema}")
        # Crear motores de conexión (source y, si quieres, target para futuras cargas)
        try:
            src_engine = create_engine(self.source_url, pool_pre_ping=True)
            with src_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("\t✅ Conexión a la fuente exitosa.")
        except Exception as e:
            print(f"\t❌ Error conectando a la fuente: {e}")
            return
        df_credito_corriente = pd.read_sql(text(f'SELECT * FROM "{self.source_schema}"."credito_corriente"'), src_engine)
        df_credito_cerrado = pd.read_sql(text(f'SELECT * FROM "{self.source_schema}"."credito_cerrado"'), src_engine)
        df_debito_corriente = pd.read_sql(text(f'SELECT * FROM "{self.source_schema}"."debito_corriente"'), src_engine)
        df_debito_cerrado = pd.read_sql(text(f'SELECT * FROM "{self.source_schema}"."debito_cerrado"'), src_engine)
        dataframes_dict['credito_corriente'] = df_credito_corriente
        dataframes_dict['credito_cerrado'] = df_credito_cerrado
        dataframes_dict['debito_corriente'] = df_debito_corriente
        dataframes_dict['debito_cerrado'] = df_debito_cerrado
        self.dataframes_dict = dataframes_dict
        print(f"\t✅ DataFrames extraídos: {list(self.dataframes_dict.keys())}")
        return True

    def transformation(self):
        # Función para generar slices por año y estado
        def generador_filtros(df, type_account):
            # Fragmenta el dataframe completo de crédito y débito por año y estado.
            # Agregar columna 'year'
            df = df.copy()  # Evitar modificar el original
            df['year'] = df['fecha'].dt.year
            
            # Obtener estados y años únicos
            estados = df['estado'].unique().tolist()
            year_range = df['year'].unique().tolist()
            
            # Inicializar diccionario para slices
            slices_to_upload = {}
            
            for year in year_range:
                df_per_year = df[df['year'] == year]
                
                # Filtrar por estado
                df_per_year_cerrado = df_per_year[df_per_year['estado'] == 'cerrado']
                df_per_year_corriente = df_per_year[df_per_year['estado'] == 'corriente']
                
                # Agregar al diccionario (solo si no están vacíos)
                if not df_per_year_cerrado.empty:
                    key_cerrado = f"{type_account}_{year}_cerrado"
                    slices_to_upload[key_cerrado] = df_per_year_cerrado
                if not df_per_year_corriente.empty:
                    key_corriente = f"{type_account}_{year}_corriente"
                    slices_to_upload[key_corriente] = df_per_year_corriente
            
            for table_name, df_año in slices_to_upload.items():
                print(f"\tSubiendo {table_name} con {df_año.shape[0]} filas...")
            return slices_to_upload
        # Generador del concepto único 
        def unic_concept_generator(concepto: str) -> str:
            if pd.isna(concepto) or concepto == '':
                return ''
            # Extraer todos los números
            numbers = re.findall(r'\d+', concepto)
            if numbers:
                # Concatenar todos los números encontrados
                return ''.join(numbers)
            else:
                # Si no hay números, limpiar a solo letras minúsculas
                concepto = concepto.lower()
                concepto = re.sub(r'[^a-z]', '', concepto)
                return concepto
        # Correcciones de fechas, generador de concepto único, estado y saldo, 
        for nombre_dataframe, dataframe in self.dataframes_dict.items(): 
            # Verificación del tamaño del dataframe. 
            print(f"\tDataframe {nombre_dataframe} tiene {dataframe.shape[0]} filas y {dataframe.shape[1]} columnas.")
            date_columns = ['fecha', 'file_date']
            for c in date_columns:
                dataframe[c] = pd.to_datetime(dataframe[c], format='%Y-%m-%d', errors='coerce')
            # Loop para aplicar el estado según venga de cerrado o corriente. 
            dataframe['unic_concept'] = dataframe['concepto'].apply(unic_concept_generator)
            if 'cerrado' in nombre_dataframe:
                dataframe['estado'] = 'cerrado'
            elif 'corriente' in nombre_dataframe:
                dataframe['estado'] = 'corriente'
            else:
                dataframe['estado'] = 'desconocido'
            print(f"\t✅ Columna 'estado' agregada a {nombre_dataframe}: {dataframe['estado'].unique()}")  # Verificación
            if 'debito' in nombre_dataframe:
                dataframe['cuenta'] = '0639 y 0640'
                dataframe.rename(columns = {'cargos': 'cargo', 'abonos': 'abono', 'saldos': 'saldo'}, inplace=True)
            if 'credito' in nombre_dataframe:
                dataframe['cuenta'] = dataframe['tarjeta'].str.replace(r'\D', '', regex=True)
                dataframe['saldo'] = pd.NA
        # Calcular rangos de años
        df_debito_combined = pd.concat([self.dataframes_dict['debito_corriente'], self.dataframes_dict['debito_cerrado']], ignore_index=True)
        df_credito_combined = pd.concat([self.dataframes_dict['credito_corriente'], self.dataframes_dict['credito_cerrado']], ignore_index=True)
        
        year_debit_min = df_debito_combined['fecha'].dt.year.min()
        year_debit_max = df_debito_combined['fecha'].dt.year.max()
        year_debit_range = list(range(year_debit_min, year_debit_max + 1))
        year_debit_range = ['debito_' + str(item) for item in year_debit_range]

        year_credit_min = df_credito_combined['fecha'].dt.year.min()
        year_credit_max = df_credito_combined['fecha'].dt.year.max()
        year_credit_range = list(range(year_credit_min, year_credit_max + 1))
        year_credit_range = ['credito_' + str(item) for item in year_credit_range]

        print(f"\tRango años débito: {year_debit_range}")
        print(f"\tRango años crédito: {year_credit_range}")
        # años debito_2025  | años crédito: ['credito_2024', 'credito_2025']
        # Primero obtenemos cuántos años cubre nuestro dataframe para enrutar a la tabla correspondiente. 
        
        all_years = year_debit_range + year_credit_range
        # all_year=s = ['debito_2023', 'debito_2024', 'debito_2025', 'credito_2024', 'credito_2025']
        #self.estates_per_year = {} # Lo declaramos en init. 
        # A partir del año en el periodo, generamos nuestro primer segmentación por año: 
        for item in all_years:
            year = int(item.replace('debito_', '').replace('credito_', ''))
            if 'debito' in item:
                df = df_debito_combined
            elif 'credito' in item:
                df = df_credito_combined
            else:
                continue
            # Filtrar por año y obtener estados únicos
            state_series = df[df['fecha'].dt.year == year]['estado']
            self.estates_per_year[item] = state_series.unique().tolist()  
            #print("Estates per year")
        print(self.estates_per_year)
        # estates_per_year  = {'debito_2025': ['corriente', 'cerrado'], 'credito_2024': ['cerrado'], 'credito_2025': ['corriente', 'cerrado']}

        # Poblar slices para débito y crédito
        slices_debito = generador_filtros(df_debito_combined, 'debito')
        slices_credito =generador_filtros(df_credito_combined, 'credito')

        # Combinar todos los slices
        #self.all_slices = {**slices_debito, **slices_credito}
        
        for nombre_dataframe, dataframe in list(self.dataframes_dict.items()):  
        # Iterar sobre una copia para evitar RuntimeError
            if dataframe is None or dataframe.empty:
                del self.dataframes_dict[nombre_dataframe]
        self.export_dataframes(self.dataframes_dict, self.source_excel)

        return True

      

    def load(self):
        #En caso de que aún no existan esquemas ni tablas, esta función la genera. 
        def first_load(tgt_engine,list_tables = None):
            # Verificar y crear esquema si no existe
            try:
                with tgt_engine.connect() as conn:
                    # Usar IF NOT EXISTS para evitar errores
                    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.target_schema}"))
                    conn.commit()  # Confirmar la transacción DDL
                    print(f"\t✅ Esquema '{self.target_schema}' verificado/creado.")
            except Exception as e:
                print(f"\t❌ Error creando esquema '{self.target_schema}': {e}")
                return False
            
            # Nuevo: Verificar si ya existen tablas en el esquema
            try:
                with tgt_engine.connect() as conn:
                    # Consulta para contar tablas en el esquema
                    table_count_query = text(f"""
                        SELECT COUNT(*) 
                        FROM information_schema.tables 
                        WHERE table_schema = '{self.target_schema}'
                    """)
                    result = conn.execute(table_count_query).scalar()
                    if result > 0:
                        print("\t\tTablas en esquema existentes.")
                        return False  # Saltar el resto del proceso de carga
            except Exception as e:
                print(f"\t❌ Error verificando tablas existentes: {e}")
                return False
            
            #####
            # Generar y crear tablas basadas en estates_per_year
            #####
            tables_to_create = []
            if list_tables is not None:
                # Usar las tablas proporcionadas, reemplazando 'corriente' por 'abierto' para simplificar
                for name in list_tables:
                    tables_to_create.append(name)
            else:
                # Vía compleja: basadas en estates_per_year
                print("\tGenerando tablas basadas en estados por año: credito_2025_cerrado, debito_2024_corriente, etc.")
                for table_key, estados in self.estates_per_year.items():
                    year = int(table_key.replace('debito_', '').replace('credito_', ''))
                    tipo = 'debito' if 'debito' in table_key else 'credito'                
                    for estado in estados:
                        table_name = f"{table_key}_{estado}"
                        tables_to_create.append(table_name)

            print(f"\tTablas a crear: {tables_to_create}")


            for table_name in tables_to_create:
                    try:
                        with tgt_engine.connect() as conn:
                            # Crear tabla con columnas estándar
                            create_query = f"""
                            CREATE TABLE IF NOT EXISTS {self.target_schema}.{table_name} (
                                fecha DATE,
                                concepto TEXT,
                                cargo NUMERIC,
                                abono NUMERIC,
                                saldo NUMERIC,
                                file_date DATE,
                                file_name TEXT,
                                estado TEXT,
                                cuenta TEXT,
                                unic_concept TEXT,
                                PRIMARY KEY (fecha, unic_concept, cargo, abono)
                            )
                            """
                            conn.execute(text(create_query))
                            conn.commit()
                            print(f"\t✅ Tabla {table_name} creada/verificada.")
                    except Exception as e:
                        print(f"\t❌ Error creando tabla {table_name}: {e}")
                        continue  

        # Sección para cargar                               
        print("\tCargando datos transformados...")
        self.target_url = self.data_access['sql_url']
        self.target_schema = 'banorte_load' 
        if self.target_schema == self.source_schema:
            print("\t⚠️ El esquema fuente y el target son iguales. Verifica las URLs.")
            return False
        print(f"\tConectando a la fuente de datos TARGET {self.target_schema}")
        # Crear motores de conexión (source y, si quieres, target para futuras cargas)
        try:
            tgt_engine = create_engine(self.target_url, pool_pre_ping=True)
            with tgt_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("\t✅ Conexión a la base target exitosa.")
        except Exception as e:
            print(f"\t❌ Error conectando a la fuente target: {e}")
            return    
        # Crear esquema, generar y poblar tablas si no existen
        tablas_destino = list(self.dataframes_dict.keys())  # Convertir a lista para usar como nombres de tablas
        first_load(tgt_engine, tablas_destino)
        print("\tCargando dataframes...")
        self.upload_dataframes_dict(tgt_engine)
        print("\n\tGenerando y enriqueciendo tablas de conceptos...\n")
        self.create_conceptos(tgt_engine)
        return True
    
    def create_conceptos(self, tgt_engine):
        # Construye/actualiza credito_conceptos y debito_conceptos desde las tablas en la BD
        try:
            with tgt_engine.connect() as conn:
                # Crear tablas conceptos si no existen
                for t in ['credito_conceptos', 'debito_conceptos']:
                    conn.execute(text(f"""
                        CREATE TABLE IF NOT EXISTS {self.target_schema}.{t} (
                            fecha DATE,
                            unic_concept TEXT,
                            cargo NUMERIC,
                            abono NUMERIC,
                            concepto TEXT,
                            beneficiario TEXT,
                            categoria TEXT,
                            grupo TEXT,
                            id_presupuesto TEXT,
                            concepto_procesado TEXT,
                            ubicacion TEXT,
                            PRIMARY KEY (fecha, unic_concept, cargo, abono)
                        )
                    """))
                # Upsert desde crédito (cerrado + corriente). Preferencia a 'cerrado' si coincidiera llave
                conn.execute(text(f"""
                    INSERT INTO {self.target_schema}.credito_conceptos
                        (fecha, unic_concept, cargo, abono, concepto, beneficiario, categoria, grupo, id_presupuesto, concepto_procesado, ubicacion)
                    SELECT DISTINCT ON (fecha, unic_concept, cargo, abono)
                        fecha, unic_concept, cargo, abono, concepto,
                        NULL::text AS beneficiario, NULL::text AS categoria, NULL::text AS grupo,
                        NULL::text AS id_presupuesto, NULL::text AS concepto_procesado, NULL::text AS ubicacion
                    FROM (
                        SELECT fecha, unic_concept, cargo, abono, concepto, 0 AS ord
                        FROM {self.target_schema}.credito_cerrado
                        UNION ALL
                        SELECT fecha, unic_concept, cargo, abono, concepto, 1 AS ord
                        FROM {self.target_schema}.credito_corriente
                    ) s
                    ORDER BY fecha, unic_concept, cargo, abono, ord
                    ON CONFLICT (fecha, unic_concept, cargo, abono) DO NOTHING
                """))
                # Upsert desde débito (cerrado + corriente)
                conn.execute(text(f"""
                    INSERT INTO {self.target_schema}.debito_conceptos
                        (fecha, unic_concept, cargo, abono, concepto, beneficiario, categoria, grupo, id_presupuesto, concepto_procesado, ubicacion)
                    SELECT DISTINCT ON (fecha, unic_concept, cargo, abono)
                        fecha, unic_concept, cargo, abono, concepto,
                        NULL::text AS beneficiario, NULL::text AS categoria, NULL::text AS grupo,
                        NULL::text AS id_presupuesto, NULL::text AS concepto_procesado, NULL::text AS ubicacion
                    FROM (
                        SELECT fecha, unic_concept, cargo, abono, concepto, 0 AS ord
                        FROM {self.target_schema}.debito_cerrado
                        UNION ALL
                        SELECT fecha, unic_concept, cargo, abono, concepto, 1 AS ord
                        FROM {self.target_schema}.debito_corriente
                    ) s
                    ORDER BY fecha, unic_concept, cargo, abono, ord
                    ON CONFLICT (fecha, unic_concept, cargo, abono) DO NOTHING
                """))
                conn.commit()
                print(f"\t✅ Tablas de conceptos actualizadas desde la base de datos.")
        except Exception as e:
            print(f"\t❌ Error creando/actualizando conceptos: {e}")
        
        
    def upload_dataframes_dict(self, tgt_engine):
        # Usar tal cual los dataframes transformados y columnas esperadas
        columns = self.columns_to_upload
        pk_cols = ['fecha', 'unic_concept', 'cargo', 'abono']

        # Primero corrientes (reemplazo total), luego cerrados (solo nuevas)
        order = ['debito_corriente', 'credito_corriente', 'debito_cerrado', 'credito_cerrado']

        for table in order:
            if table not in self.dataframes_dict:
                print(f"\t⚠️ {table}: no presente en dataframes_dict, omitiendo.")
                continue
            df = self.dataframes_dict[table]
            if df is None or df.empty:
                print(f"\t⚠️ {table}: fuente vacía, omitiendo.")
                continue

            work = df[columns]

            try:
                with tgt_engine.connect() as conn:
                    full_table = f"{self.target_schema}.{table}"
                    if table.endswith('_corriente'):
                        # Reemplazo total de snapshot
                        conn.execute(text(f"TRUNCATE TABLE {full_table}"))
                        conn.commit()
                        work.to_sql(table, tgt_engine, schema=self.target_schema, if_exists='append', index=False)
                        print(f"\t✅ {table}: snapshot reemplazada con {len(work)} filas.")
                    else:
                        # Insertar todo con protección de PK: ON CONFLICT DO NOTHING
                        # Evita que un duplicado corte el proceso y omite duplicadas internas
                        before = len(work)
                        work = work.drop_duplicates(subset=pk_cols, keep='first')
                        dropped_internal = before - len(work)
                        insert_sql = text(
                            f"""
                            INSERT INTO {full_table}
                            (fecha, concepto, cargo, abono, saldo, file_date, file_name, estado, cuenta, unic_concept)
                            VALUES (:fecha, :concepto, :cargo, :abono, :saldo, :file_date, :file_name, :estado, :cuenta, :unic_concept)
                            ON CONFLICT (fecha, unic_concept, cargo, abono) DO NOTHING
                            """
                        )
                        # Convertir NaN -> None para evitar problemas con psycopg2
                        clean = work.where(pd.notnull(work), None)
                        records = clean.to_dict(orient='records')
                        # Ejecutar en chunks para no saturar el driver
                        chunk = 1000
                        total = 0
                        for i in range(0, len(records), chunk):
                            batch = records[i:i+chunk]
                            if not batch:
                                continue
                            conn.execute(insert_sql, batch)
                            total += len(batch)
                        conn.commit()
                        if dropped_internal:
                            print(f"\tℹ️ {table}: {dropped_internal} duplicadas internas omitidas.")
                        print(f"\t✅ {table}: upsert con DO NOTHING completado ({len(work)} filas evaluadas).")
            except Exception as e:
                print(f"\t❌ Error procesando {table}: {e}")

        # Actualizar tablas de conceptos (solo nuevas filas)
        feed_cols = ['fecha', 'unic_concept', 'cargo', 'abono', 'concepto']
        new_cols = ['beneficiario', 'categoria', 'grupo', 'id_presupuesto', 'concepto_procesado', 'ubicacion']
        concepts_pk = ['fecha', 'unic_concept', 'cargo', 'abono']

        def ensure_concepts_table(conn, table_name: str):
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.target_schema}.{table_name} (
                fecha DATE,
                unic_concept TEXT,
                cargo NUMERIC,
                abono NUMERIC,
                concepto TEXT,
                beneficiario TEXT,
                categoria TEXT,
                grupo TEXT,
                id_presupuesto TEXT,
                concepto_procesado TEXT,
                ubicacion TEXT,
                PRIMARY KEY (fecha, unic_concept, cargo, abono)
            )
            """
            conn.execute(text(create_sql))

        def upsert_concepts(conn, table_name: str, df_a: pd.DataFrame, df_b: pd.DataFrame):
            if (df_a is None or df_a.empty) and (df_b is None or df_b.empty):
                print(f"\tℹ️ {table_name}: sin fuentes para conceptos.")
                return
            ensure_concepts_table(conn, table_name)
            # Unir fuentes disponibles
            parts = []
            if df_a is not None and not df_a.empty:
                parts.append(df_a)
            if df_b is not None and not df_b.empty:
                parts.append(df_b)
            combined = pd.concat(parts, axis=0, ignore_index=True)
            # Seleccionar columnas y completar faltantes sin re-formatear
            cols_all = feed_cols + new_cols
            work = combined[[c for c in cols_all if c in combined.columns]].copy()
            for c in cols_all:
                if c not in work.columns:
                    work[c] = pd.NA
            # Deduplicar por PK para no cargar duplicadas internas
            work = work.drop_duplicates(subset=concepts_pk, keep='first')
            # Preparar inserción segura
            insert_sql = text(
                f"""
                INSERT INTO {self.target_schema}.{table_name}
                (fecha, unic_concept, cargo, abono, concepto, beneficiario, categoria, grupo, id_presupuesto, concepto_procesado, ubicacion)
                VALUES (:fecha, :unic_concept, :cargo, :abono, :concepto, :beneficiario, :categoria, :grupo, :id_presupuesto, :concepto_procesado, :ubicacion)
                ON CONFLICT (fecha, unic_concept, cargo, abono) DO NOTHING
                """
            )
            clean = work.where(pd.notnull(work), None)
            records = clean.to_dict(orient='records')
            chunk = 2000
            inserted_total = 0
            for i in range(0, len(records), chunk):
                batch = records[i:i+chunk]
                if not batch:
                    continue
                conn.execute(insert_sql, batch)
                inserted_total += len(batch)
            conn.commit()
            print(f"\t✅ {table_name}: conceptos evaluados {len(work)} (solo nuevas insertadas).")



    def export_dataframes(self, dictionary, file_path):            
        print(f"\t📝 Escribiendo la fuente SQL Source a Excel:")
        df_debito_corriente = dictionary['debito_corriente']
        df_credito_corriente = dictionary['credito_corriente']
        df_debito_cerrado = dictionary['debito_cerrado']
        df_credito_cerrado = dictionary['credito_cerrado']
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            # Según especificación: hoja debito <= mirror_credito; hoja credito <= mirror_debito
            df_debito_corriente = df_debito_corriente.sort_values(by='file_name', ascending=False)
            df_credito_corriente = df_credito_corriente.sort_values(by='file_name', ascending=False)
            df_debito_cerrado = df_debito_cerrado.sort_values(by='file_name', ascending=False)
            df_credito_cerrado = df_credito_cerrado.sort_values(by='file_name', ascending=False)
            df_debito_corriente.to_excel(writer, sheet_name='debito_corriente', index=False)
            df_credito_corriente.to_excel(writer, sheet_name='credito_corriente', index=False)
            df_debito_cerrado.to_excel(writer, sheet_name='debito_cerrado', index=False)
            df_credito_cerrado.to_excel(writer, sheet_name='credito_cerrado', index=False)
        print(f"\t✅ {os.path.basename(file_path)}.")

    
    def main(self):
        passwords_path = os.path.join(self.folder_root, "Implementación", "Info Bancaria", 'passwords.yaml')
        with open(passwords_path, 'r') as f:
            self.data_access = yaml.safe_load(f)

        print("🚀 Iniciando proceso de extracción...")
        if self.extraction():
            print("✅ Proceso de extracción de datos finalizado exitosamente.")
        print("🚀 Iniciando proceso de transformación...")
        if self.transformation():
            print("✅ Proceso de transformación de conceptos finalizado exitosamente.")
        print("🚀 Iniciando proceso de carga...")
        if self.load():
            print("✅ Proceso de carga finalizado exitosamente.")


if __name__ == "__main__":
    folder_root = os.getcwd()
    ETL(folder_root).main()

    
