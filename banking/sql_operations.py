from sqlalchemy import create_engine, text
from utils.helpers import message_print

class SQL_Operations:
    def __init__(self, data_access):
        self.sql_url = data_access['SQL_PATH']
        self.scheme_name = 'business'  # Definir aquí una sola vez
        # SQL_PATH: "postgresql://armjorge:*****@ep-shy-darkness-10211313-pooler.us-east-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

    def sql_connection(self):
        """Establece una conexión a la base de datos y devuelve el motor."""
        try:
            # Crear el motor de conexión
            engine = create_engine(self.sql_url)
            # Probar la conexión
            with engine.connect() as connection:
                print("✅ Conexión exitosa a la base de datos.")
            return engine
        except Exception as e:
            print(f"❌ Error al conectar a la base de datos: {e}")
            return None

    def sql_business_mining(self):
        """Verifica el esquema y realiza comparaciones de datos."""
        try:
            # Establecer conexión a la base de datos
            engine = self.sql_connection()
            if not engine:
                print("❌ No se pudo establecer la conexión con la base de datos.")
                return
            
            with engine.connect() as connection:
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
                # Modelos para alimentar con nueva data
                
        except Exception as e:
            print(f"❌ Error durante la minería de datos empresariales: {e}")

    def new_feeding(self):
        """Crea el esquema y las tablas necesarias."""
        try:
            # Establecer conexión a la base de datos
            engine = self.sql_connection()
            if not engine:
                print("❌ No se pudo establecer la conexión con la base de datos.")
                return
            
            with engine.connect() as connection:
                # Iniciar transacción explícita
                transaction = connection.begin()
                try:
                    # Crear esquema
                    connection.execute(text(f"CREATE SCHEMA {self.scheme_name};"))
                    print(f"✅ Esquema '{self.scheme_name}' creado exitosamente.")
                except Exception as e:
                    if "DuplicateSchema" in str(e):
                        print(f"⚠️ El esquema '{self.scheme_name}' ya existe. Continuando con las operaciones...")
                    else:
                        raise e
                
                # Crear tablas
                tables = ['CREDIT', 'DEBIT', 'MANUAL_COLUMNS']
                existing_tables = connection.execute(text(f"""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = '{self.scheme_name}';
                """)).fetchall()
                existing_tables = [row[0] for row in existing_tables]
                
                if 'CREDIT' not in existing_tables:
                    connection.execute(text(f"""
                        CREATE TABLE {self.scheme_name}.CREDIT (
                            fecha TIMESTAMP,
                            concepto TEXT,
                            abono DOUBLE PRECISION,
                            cargo DOUBLE PRECISION,
                            tarjeta TEXT,
                            file_name TEXT
                        );
                    """))
                    print("✅ Tabla 'CREDIT' creada exitosamente.")
                
                if 'DEBIT' not in existing_tables:
                    connection.execute(text(f"""
                        CREATE TABLE {self.scheme_name}.DEBIT (
                            fecha TIMESTAMP,
                            concepto TEXT,
                            cargos DOUBLE PRECISION,
                            abonos DOUBLE PRECISION,
                            saldos DOUBLE PRECISION,
                            file_name TEXT
                        );
                    """))
                    print("✅ Tabla 'DEBIT' creada exitosamente.")
                
                if 'MANUAL_COLUMNS' not in existing_tables:
                    connection.execute(text(f"""
                        CREATE TABLE {self.scheme_name}.MANUAL_COLUMNS (
                            concepto TEXT,
                            categoria TEXT,
                            estado_financiero TEXT
                        );
                    """))
                    print("✅ Tabla 'MANUAL_COLUMNS' creada exitosamente.")
                
                # Llenar tablas
                connection.execute(text(f"""
                    INSERT INTO {self.scheme_name}.CREDIT (fecha, concepto, abono, cargo, tarjeta, file_name)
                    SELECT fecha, concepto, abono, cargo, tarjeta, file_name
                    FROM public.base_credito_cerrado
                    UNION ALL
                    SELECT fecha, concepto, abono, cargo, tarjeta, file_name
                    FROM public.base_credito_corriente;
                """))
                print("✅ Tabla 'CREDIT' llenada exitosamente.")
                
                connection.execute(text(f"""
                    INSERT INTO {self.scheme_name}.DEBIT (fecha, concepto, cargos, abonos, saldos, file_name)
                    SELECT fecha, concepto, cargos, abonos, saldos, file_name
                    FROM public.base_debito_cerrado
                    UNION ALL
                    SELECT fecha, concepto, cargos, abonos, saldos, file_name
                    FROM public.base_debito_corriente;
                """))
                print("✅ Tabla 'DEBIT' llenada exitosamente.")
                
                connection.execute(text(f"""
                    INSERT INTO {self.scheme_name}.MANUAL_COLUMNS (concepto, categoria, estado_financiero)
                    VALUES 
                        (NULL, 'Categ1', 'Ingreso'),
                        (NULL, 'Categ2', 'Gasto operativo'),
                        (NULL, 'Categ3', 'Impuestos'),
                        (NULL, 'Categ4', 'Intereses');
                """))
                print("✅ Tabla 'MANUAL_COLUMNS' llenada exitosamente.")
                
                # Confirmar la transacción
                transaction.commit()
                print("🎉 Esquema y tablas creados exitosamente.")
                
        except Exception as e:
            if 'transaction' in locals():
                transaction.rollback()
            print(f"❌ Error durante la creación del esquema y las tablas: {e}")


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
        print(f"ABONOS Públicos: {abonos_public_credit}, ABONOS Empresariales: {abonos_business_credit}")
        print(f"CARGOS Públicos: {cargos_public_credit}, CARGOS Empresariales: {cargos_business_credit}")
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

