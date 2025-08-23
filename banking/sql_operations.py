from sqlalchemy import create_engine, text
from utils.helpers import message_print

class SQL_Operations:
    def __init__(self, data_access):
        self.sql_url = data_access['SQL_PATH']
        self.scheme_name = 'datawarehouse'
        self.engine = self._create_engine()
        self.connection = None
        if self.engine:
            # Solo probar la conexión y luego cerrarla
            test_connection = self.engine.connect()
            test_connection.close()
            print("✅ Conexión exitosa a la base de datos.")    

    
    def _create_engine(self):
        """Crea el motor de conexión a la base de datos."""
        try:
            engine = create_engine(self.sql_url)
            return engine
        except Exception as e:
            print(f"❌ Error al crear el motor de base de datos: {e}")
            return None
    
    def __del__(self):
        """Cierra el motor al destruir el objeto."""
        if self.engine:
            self.engine.dispose()
    def sql_business_mining(self):
        """Verifica el esquema y realiza comparaciones de datos."""
        try:
            # Crear conexión específica para esta función
            with self.engine.connect() as connection:
                # Verificar si el esquema existe
                result = connection.execute(text(f"""
                    SELECT schema_name 
                    FROM information_schema.schemata 
                    WHERE schema_name = '{self.scheme_name}';
                """)).fetchone()
                
                if not result:
                    # Llamar a la función para crear el esquema y las tablas
                    self.new_feeding()
                    print("Tenemos que llenar manualmente los conceptos. Vamos a descargar slides de datos y luego cargar de nuevo la info para sustituir información. Ahora sí podrás tener control por categorías y dimensiones.")
                    return
                
                print("Primera verificación:")
                self.primera_verificacion(connection)
                
        except Exception as e:
            print(f"❌ Error durante la minería de datos empresariales: {e}")

    def new_feeding(self):
        """Crea el esquema y las tablas necesarias."""
        try:
            # Crear conexión específica para esta función
            with self.engine.connect() as connection:
                with connection.begin() as trans:
                    try:
                        # Verificar si el esquema existe
                        schema_exists = connection.execute(text(f"""
                            SELECT schema_name 
                            FROM information_schema.schemata 
                            WHERE schema_name = '{self.scheme_name}';
                        """)).fetchone()
                        
                        if not schema_exists:
                            # Crear esquema
                            connection.execute(text(f"CREATE SCHEMA {self.scheme_name};"))
                            print(f"✅ Esquema '{self.scheme_name}' creado exitosamente.")
                        else:
                            print(f"⚠️ El esquema '{self.scheme_name}' ya existe.")

                        # Verificar tablas existentes
                        existing_tables = connection.execute(text(f"""
                            SELECT table_name 
                            FROM information_schema.tables 
                            WHERE table_schema = '{self.scheme_name}';
                        """)).fetchall()
                        existing_tables = [row[0] for row in existing_tables]
                        
                        # Crear tablas si no existen
                        if 'credit' not in existing_tables:
                            connection.execute(text(f"""
                                CREATE TABLE {self.scheme_name}.credit (
                                    fecha TIMESTAMP,
                                    concepto TEXT,
                                    abono DOUBLE PRECISION,
                                    cargo DOUBLE PRECISION,
                                    tarjeta TEXT,
                                    file_name TEXT
                                );
                            """))
                            print("✅ Tabla 'credit' creada exitosamente.")
                        else:
                            print("⚠️ Tabla 'credit' ya existe.")
                        
                        if 'debit' not in existing_tables:
                            connection.execute(text(f"""
                                CREATE TABLE {self.scheme_name}.debit (
                                    fecha TIMESTAMP,
                                    concepto TEXT,
                                    cargos DOUBLE PRECISION,
                                    abonos DOUBLE PRECISION,
                                    saldos DOUBLE PRECISION,
                                    file_name TEXT
                                );
                            """))
                            print("✅ Tabla 'debit' creada exitosamente.")
                        else:
                            print("⚠️ Tabla 'debit' ya existe.")
                        
                        if 'manual_columns' not in existing_tables:
                            connection.execute(text(f"""
                                CREATE TABLE {self.scheme_name}.manual_columns (
                                    concepto TEXT,
                                    categoria TEXT,
                                    estado_financiero TEXT
                                );
                            """))
                            print("✅ Tabla 'manual_columns' creada exitosamente.")
                        else:
                            print("⚠️ Tabla 'manual_columns' ya existe.")
                        
                        # Llenar tablas INCLUYENDO TODOS LOS REGISTROS (sin filtros)
                        if 'credit' not in existing_tables or not schema_exists:
                            credit_count = connection.execute(text(f"""
                                SELECT COUNT(*) FROM {self.scheme_name}.credit;
                            """)).fetchone()[0]
                            
                            if credit_count == 0:
                                # INCLUIR TODOS LOS REGISTROS sin filtrar
                                connection.execute(text(f"""
                                    INSERT INTO {self.scheme_name}.credit (fecha, concepto, abono, cargo, tarjeta, file_name)
                                    SELECT fecha, concepto, abono, cargo, tarjeta, file_name
                                    FROM public.base_credito_cerrado
                                    UNION ALL
                                    SELECT fecha, concepto, abono, cargo, tarjeta, file_name
                                    FROM public.base_credito_corriente;
                                """))
                                print("✅ Tabla 'credit' llenada exitosamente (todos los registros).")
                            else:
                                print("⚠️ Tabla 'credit' ya tiene datos.")
                        
                        if 'debit' not in existing_tables or not schema_exists:
                            debit_count = connection.execute(text(f"""
                                SELECT COUNT(*) FROM {self.scheme_name}.debit;
                            """)).fetchone()[0]
                            
                            if debit_count == 0:
                                # INCLUIR TODOS LOS REGISTROS sin filtrar
                                connection.execute(text(f"""
                                    INSERT INTO {self.scheme_name}.debit (fecha, concepto, cargos, abonos, saldos, file_name)
                                    SELECT fecha, concepto, cargos, abonos, saldos, file_name
                                    FROM public.base_debito_cerrado
                                    UNION ALL
                                    SELECT fecha, concepto, cargos, abonos, saldos, file_name
                                    FROM public.base_debito_corriente;
                                """))
                                print("✅ Tabla 'debit' llenada exitosamente (todos los registros).")
                            else:
                                print("⚠️ Tabla 'debit' ya tiene datos.")
                        
                        if 'manual_columns' not in existing_tables or not schema_exists:
                            manual_count = connection.execute(text(f"""
                                SELECT COUNT(*) FROM {self.scheme_name}.manual_columns;
                            """)).fetchone()[0]
                            
                            if manual_count == 0:
                                connection.execute(text(f"""
                                    INSERT INTO {self.scheme_name}.manual_columns (concepto, categoria, estado_financiero)
                                    VALUES 
                                        (NULL, 'Categ1', 'Ingreso'),
                                        (NULL, 'Categ2', 'Gasto operativo'),
                                        (NULL, 'Categ3', 'Impuestos'),
                                        (NULL, 'Categ4', 'Intereses');
                                """))
                                print("✅ Tabla 'manual_columns' llenada exitosamente.")
                            else:
                                print("⚠️ Tabla 'manual_columns' ya tiene datos.")
                        
                        print("🎉 Esquema y tablas verificados/creados exitosamente.")
                        
                    except Exception as e:
                        print(f"❌ Error durante la creación del esquema y las tablas: {e}")
                        raise e
                        
        except Exception as e:
            print(f"❌ Error durante la creación del esquema y las tablas: {e}")
    def sql_mirroring(self):
        """Sincroniza los datos entre las tablas públicas y el esquema business."""
        print("Vamos a asegurarnos de que la base sea un espejo de la información fuente en las columnas que hacen únicos a los renglones")
        
        try:
            # Crear conexión específica para esta función
            with self.engine.connect() as connection:
                print("Iniciando el mirroring para la información de crédito")
                columnas_credito = ["fecha", "concepto", "abono", "cargo", "tarjeta"]
                
                # Query para obtener datos de crédito de las tablas públicas
                input_query_credit = """
                    SELECT fecha, concepto, abono, cargo, tarjeta
                    FROM public.base_credito_cerrado
                    UNION ALL
                    SELECT fecha, concepto, abono, cargo, tarjeta
                    FROM public.base_credito_corriente
                """
                
                target_table_credit = f"{self.scheme_name}.credit"
                
                # Sincronizar créditos
                self._sync_tables(connection, input_query_credit, target_table_credit, columnas_credito, "crédito")
                
                print("Iniciando el mirroring para la información de débito")
                columnas_debito = ["fecha", "concepto", "cargos", "abonos", "saldos"]
                
                # Query para obtener datos de débito de las tablas públicas
                input_query_debit = """
                    SELECT fecha, concepto, cargos, abonos, saldos
                    FROM public.base_debito_cerrado
                    UNION ALL
                    SELECT fecha, concepto, cargos, abonos, saldos
                    FROM public.base_debito_corriente
                """
                
                target_table_debit = f"{self.scheme_name}.debit"
                
                # Sincronizar débitos
                self._sync_tables(connection, input_query_debit, target_table_debit, columnas_debito, "débito")
                
                print("🎉 Mirroring completado exitosamente.")
                
        except Exception as e:
            print(f"❌ Error durante el mirroring: {e}")

    def sql_menu(self):
        print("1 para actualizar datos de Public (raw) a Business")
        print("2 para diagnosticar diferencias entre Public (raw) y Business")
        while True:
            try:
                input_user = int(input("Selecciona una opción: "))
                break
            except ValueError:
                print("Por favor, ingresa un número válido.")
            
        if input_user == 1:
            self.sql_mirroring()
        elif input_user == 2:
            self.diagnostico_datos()
        else:
            print("Desarrollando")

    def primera_verificacion(self, connection):
        # Comparaciones de datos
        # Créditos públicos - SUMA TOTAL de ambas tablas
        abonos_public_credit = connection.execute(text(f"""
            SELECT SUM(abono) FROM (
                SELECT abono FROM public.base_credito_cerrado
                UNION ALL
                SELECT abono FROM public.base_credito_corriente
            ) AS combined_credit;
        """)).fetchone()[0]
        
        cargos_public_credit = connection.execute(text(f"""
            SELECT SUM(cargo) FROM (
                SELECT cargo FROM public.base_credito_cerrado
                UNION ALL
                SELECT cargo FROM public.base_credito_corriente
            ) AS combined_credit;
        """)).fetchone()[0]
        
        # Créditos empresariales
        abonos_business_credit = connection.execute(text(f"""
            SELECT SUM(abono) 
            FROM {self.scheme_name}.credit;
        """)).fetchone()[0]
        
        cargos_business_credit = connection.execute(text(f"""
            SELECT SUM(cargo) 
            FROM {self.scheme_name}.credit;
        """)).fetchone()[0]
        
        print("Explicación de la comparación de crédito:")
        print(f"ABONOS Públicos: {abonos_public_credit}, ABONOS {self.scheme_name}: {abonos_business_credit}")
        print(f"CARGOS Públicos: {cargos_public_credit}, CARGOS {self.scheme_name}: {cargos_business_credit}")
        print(message_print(f"Diferencia Crédito \nCargos {cargos_public_credit - cargos_business_credit} \nAbonos {abonos_public_credit - abonos_business_credit}"))

        # Débitos públicos - SUMA TOTAL de ambas tablas
        abonos_public_debit = connection.execute(text(f"""
            SELECT SUM(abonos) FROM (
                SELECT abonos FROM public.base_debito_cerrado
                UNION ALL
                SELECT abonos FROM public.base_debito_corriente
            ) AS combined_debit;
        """)).fetchone()[0]
        
        cargos_public_debit = connection.execute(text(f"""
            SELECT SUM(cargos) FROM (
                SELECT cargos FROM public.base_debito_cerrado
                UNION ALL
                SELECT cargos FROM public.base_debito_corriente
            ) AS combined_debit;
        """)).fetchone()[0]
        
        # Débitos empresariales
        abonos_business_debit = connection.execute(text(f"""
            SELECT SUM(abonos) 
            FROM {self.scheme_name}.debit;
        """)).fetchone()[0]
        
        cargos_business_debit = connection.execute(text(f"""
            SELECT SUM(cargos) 
            FROM {self.scheme_name}.debit;
        """)).fetchone()[0]
        
        print("Explicación y resultado de la comparación:")
        print(f"ABONOS Públicos: {abonos_public_debit}, ABONOS Empresariales: {abonos_business_debit}")
        print(f"CARGOS Públicos: {cargos_public_debit}, CARGOS Empresariales: {cargos_business_debit}")
        print(message_print(f"Diferencia Débito \nCargos {abonos_public_debit - abonos_business_debit} \nAbonos {cargos_public_debit - cargos_business_debit}"))


    def _sync_tables(self, connection, input_query, target_table, columnas, table_type):
        """
        Función genérica para sincronizar una tabla input con una tabla target.
        """
        with connection.begin() as trans:
            try:
                # Obtener TODOS los datos de la fuente (input) - SIN FILTROS
                input_data = connection.execute(text(input_query)).fetchall()
                
                # Obtener TODOS los datos del target (solo las columnas principales)
                target_data = connection.execute(text(f"""
                    SELECT {', '.join(columnas)} FROM {target_table}
                """)).fetchall()
                
                # Convertir a sets para comparación eficiente
                input_set = set(input_data)
                target_set = set(target_data)
                
                # Encontrar diferencias
                to_insert = input_set - target_set  # Registros que faltan en target
                to_delete = target_set - input_set  # Registros que sobran en target
                
                # ELIMINAR registros que no están en input
                if to_delete:
                    print(f"Eliminando {len(to_delete)} registros de {table_type}:")
                    for record in to_delete:
                        # Crear condiciones WHERE para las columnas principales
                        where_conditions = " AND ".join([f"{col} = :{col}" for col in columnas])
                        values_dict = {col: record[i] for i, col in enumerate(columnas)}
                        
                        # ELIMINAR registros que coincidan con estas columnas principales
                        delete_query = f"""
                            DELETE FROM {target_table} 
                            WHERE {where_conditions}
                        """
                        
                        result = connection.execute(text(delete_query), values_dict)
                        print(f"❌ Eliminados {result.rowcount} registros de {table_type}: {record}")
                else:
                    print(f"✅ No hay registros para eliminar en {table_type}")
                
                # INSERTAR registros que faltan
                if to_insert:
                    print(f"Insertando {len(to_insert)} registros en {table_type}:")
                    for record in to_insert:
                        # Crear placeholders para los valores
                        placeholders = ", ".join([f":{col}" for col in columnas])
                        
                        # Crear el diccionario de valores
                        values_dict = {col: record[i] for i, col in enumerate(columnas)}
                        values_dict['file_name'] = 'mirroring'  # Agregar file_name
                        
                        # Construir query de inserción
                        all_columns = columnas + ['file_name']
                        all_placeholders = placeholders + ", :file_name"
                        insert_query = f"""
                            INSERT INTO {target_table} ({", ".join(all_columns)})
                            VALUES ({all_placeholders})
                        """
                        
                        connection.execute(text(insert_query), values_dict)
                        print(f"\t✅ Agregado a {table_type}: {record}")
                else:
                    print(f"✅ No hay registros nuevos para insertar en {table_type}")
                
                # VERIFICACIÓN FINAL
                final_count = connection.execute(text(f"SELECT COUNT(*) FROM {target_table}")).fetchone()[0]
                input_count = len(input_data)
                
                print(f"📊 Resultado final en {table_type}: {final_count} registros en target, {input_count} en source")
                
                if final_count != input_count:
                    print(f"⚠️ Discrepancia: target tiene {final_count}, source tiene {input_count}")
                
                print(f"🎉 Sincronización de {table_type} completada exitosamente.")
                
            except Exception as e:
                print(f"❌ Error durante la sincronización de {table_type}: {e}")
                raise e
            
    def diagnostico_datos(self):
        """Diagnóstica las diferencias entre las tablas públicas y datawarehouse."""
        try:
            with self.engine.connect() as connection:
                print("=== DIAGNÓSTICO DE DATOS ===")
                
                # Contar registros en cada tabla
                print("\n📊 CONTEO DE REGISTROS:")
                
                # Tablas públicas
                count_credito_cerrado = connection.execute(text("SELECT COUNT(*) FROM public.base_credito_cerrado")).fetchone()[0]
                count_credito_corriente = connection.execute(text("SELECT COUNT(*) FROM public.base_credito_corriente")).fetchone()[0]
                count_debito_cerrado = connection.execute(text("SELECT COUNT(*) FROM public.base_debito_cerrado")).fetchone()[0]
                count_debito_corriente = connection.execute(text("SELECT COUNT(*) FROM public.base_debito_corriente")).fetchone()[0]
                
                print(f"base_credito_cerrado: {count_credito_cerrado}")
                print(f"base_credito_corriente: {count_credito_corriente}")
                print(f"base_debito_cerrado: {count_debito_cerrado}")
                print(f"base_debito_corriente: {count_debito_corriente}")
                
                # Tablas datawarehouse
                count_credit_dw = connection.execute(text(f"SELECT COUNT(*) FROM {self.scheme_name}.credit")).fetchone()[0]
                count_debit_dw = connection.execute(text(f"SELECT COUNT(*) FROM {self.scheme_name}.debit")).fetchone()[0]
                
                print(f"datawarehouse.credit: {count_credit_dw}")
                print(f"datawarehouse.debit: {count_debit_dw}")
                
                print(f"\n🔍 TOTAL ESPERADO vs REAL:")
                total_credito_esperado = count_credito_cerrado + count_credito_corriente
                total_debito_esperado = count_debito_cerrado + count_debito_corriente
                
                print(f"Crédito - Esperado: {total_credito_esperado}, Real: {count_credit_dw}")
                print(f"Débito - Esperado: {total_debito_esperado}, Real: {count_debit_dw}")
                
                # Verificar duplicados en datawarehouse usando subconsultas
                print(f"\n🔍 VERIFICANDO DUPLICADOS EN DATAWAREHOUSE:")
                
                # Forma correcta: usar subconsulta para contar únicos
                total_credit_records = connection.execute(text(f"""
                    SELECT COUNT(*) FROM {self.scheme_name}.credit
                """)).fetchone()[0]
                
                unique_credit_records = connection.execute(text(f"""
                    SELECT COUNT(*) FROM (
                        SELECT DISTINCT fecha, concepto, abono, cargo, tarjeta 
                        FROM {self.scheme_name}.credit
                    ) AS unique_records
                """)).fetchone()[0]
                
                duplicados_credit = total_credit_records - unique_credit_records
                
                total_debit_records = connection.execute(text(f"""
                    SELECT COUNT(*) FROM {self.scheme_name}.debit
                """)).fetchone()[0]
                
                unique_debit_records = connection.execute(text(f"""
                    SELECT COUNT(*) FROM (
                        SELECT DISTINCT fecha, concepto, cargos, abonos, saldos 
                        FROM {self.scheme_name}.debit
                    ) AS unique_records
                """)).fetchone()[0]
                
                duplicados_debit = total_debit_records - unique_debit_records
                
                print(f"Duplicados en credit: {duplicados_credit}")
                print(f"Duplicados en debit: {duplicados_debit}")
                
                # Verificar duplicados en tablas públicas
                print(f"\n🔍 VERIFICANDO DUPLICADOS EN PÚBLICAS:")
                
                total_public_credit = connection.execute(text("""
                    SELECT COUNT(*) FROM (
                        SELECT fecha, concepto, abono, cargo, tarjeta FROM public.base_credito_cerrado
                        UNION ALL
                        SELECT fecha, concepto, abono, cargo, tarjeta FROM public.base_credito_corriente
                    ) AS combined
                """)).fetchone()[0]
                
                unique_public_credit = connection.execute(text("""
                    SELECT COUNT(*) FROM (
                        SELECT DISTINCT fecha, concepto, abono, cargo, tarjeta FROM (
                            SELECT fecha, concepto, abono, cargo, tarjeta FROM public.base_credito_cerrado
                            UNION ALL
                            SELECT fecha, concepto, abono, cargo, tarjeta FROM public.base_credito_corriente
                        ) AS combined
                    ) AS unique_records
                """)).fetchone()[0]
                
                duplicados_public_credit = total_public_credit - unique_public_credit
                
                total_public_debit = connection.execute(text("""
                    SELECT COUNT(*) FROM (
                        SELECT fecha, concepto, cargos, abonos, saldos FROM public.base_debito_cerrado
                        UNION ALL
                        SELECT fecha, concepto, cargos, abonos, saldos FROM public.base_debito_corriente
                    ) AS combined
                """)).fetchone()[0]
                
                unique_public_debit = connection.execute(text("""
                    SELECT COUNT(*) FROM (
                        SELECT DISTINCT fecha, concepto, cargos, abonos, saldos FROM (
                            SELECT fecha, concepto, cargos, abonos, saldos FROM public.base_debito_cerrado
                            UNION ALL
                            SELECT fecha, concepto, cargos, abonos, saldos FROM public.base_debito_corriente
                        ) AS combined
                    ) AS unique_records
                """)).fetchone()[0]
                
                duplicados_public_debit = total_public_debit - unique_public_debit
                
                print(f"Total registros públicos crédito: {total_public_credit}")
                print(f"Registros únicos públicos crédito: {unique_public_credit}")
                print(f"Duplicados en públicas crédito: {duplicados_public_credit}")
                
                print(f"Total registros públicos débito: {total_public_debit}")
                print(f"Registros únicos públicos débito: {unique_public_debit}")
                print(f"Duplicados en públicas débito: {duplicados_public_debit}")
                
                # Mostrar resumen del problema
                print(f"\n🎯 RESUMEN DEL PROBLEMA:")
                registros_faltantes_credit = total_credito_esperado - count_credit_dw
                registros_faltantes_debit = total_debito_esperado - count_debit_dw
                
                if registros_faltantes_credit > 0:
                    print(f"❌ Faltan {registros_faltantes_credit} registros de crédito")
                elif registros_faltantes_credit < 0:
                    print(f"⚠️ Hay {abs(registros_faltantes_credit)} registros de más en crédito")
                else:
                    print(f"✅ Registros de crédito coinciden")
                    
                if registros_faltantes_debit > 0:
                    print(f"❌ Faltan {registros_faltantes_debit} registros de débito")
                elif registros_faltantes_debit < 0:
                    print(f"⚠️ Hay {abs(registros_faltantes_debit)} registros de más en débito")
                else:
                    print(f"✅ Registros de débito coinciden")
                
                if duplicados_credit > 0:
                    print(f"⚠️ Hay {duplicados_credit} duplicados en datawarehouse.credit")
                if duplicados_debit > 0:
                    print(f"⚠️ Hay {duplicados_debit} duplicados en datawarehouse.debit")
                if duplicados_public_credit > 0:
                    print(f"⚠️ Hay {duplicados_public_credit} duplicados en las tablas públicas de crédito")
                if duplicados_public_debit > 0:
                    print(f"⚠️ Hay {duplicados_public_debit} duplicados en las tablas públicas de débito")
                    
        except Exception as e:
            print(f"❌ Error en diagnóstico: {e}")