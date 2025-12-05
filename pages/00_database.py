import sys
import streamlit as st
import os
from dotenv import load_dotenv

st.title("Agregar base de datos ⚙️")
st.write("Necesitarás un link a tu base en PostgreSQL y a MongoDB. Te recomiendo abrir cuentas gratuitas aquí: ")
st.page_link("https://www.mongodb.com", label="MongoDB", icon="🍃")
st.page_link("https://neon.tech", label="Neon", icon="🗄️")

st.write("Los links de conexión tienen este formato:")
st.code("mongodb+srv://user:password@database.xhwn9wa.mongodb.net/?appName=cluster")
st.code("postgresql://user:password@ep-mute-sunset-a84equmn-pooler.eastus2.azure.neon.tech/database")

# ================== CONFIG RUTAS / .env ==================
BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_PATH not in sys.path:
    sys.path.insert(0, BASE_PATH)

env_file = os.path.join(BASE_PATH, ".env")
postgresql_key = "sql_workflow"
mongo_db_key = "MONGO_URI"

# Crear .env vacío si no existe
if not os.path.exists(env_file):
    with open(env_file, "w") as f:
        f.write("")  # opcional: puedes poner un comentario inicial
    st.info("Se creó un archivo `.env` vacío en la raíz del proyecto.")

# Cargar variables desde .env
load_dotenv(dotenv_path=env_file, override=True)

existing_pg = os.getenv(postgresql_key, "") or ""
existing_mongo = os.getenv(mongo_db_key, "") or ""

data_access = {}

# ================== FUNCIONES AUXILIARES ==================
def mask_uri(uri: str) -> str:
    """Enmascara la contraseña en una URI tipo scheme://user:pass@host/..."""
    if not uri:
        return ""
    try:
        # scheme://rest
        if "://" not in uri:
            return uri
        scheme, rest = uri.split("://", 1)
        # rest: user:pass@host/...
        if "@" not in rest:
            return uri
        creds, host_part = rest.split("@", 1)
        # creds: user:pass o user
        if ":" not in creds:
            # no password, solo user
            return f"{scheme}://{creds}@{host_part}"
        user, _ = creds.split(":", 1)
        return f"{scheme}://{user}:***@{host_part}"
    except Exception:
        # Si algo falla, regresamos el original (mejor que romper)
        return uri

# ================== UI: CAPTURA / EDICIÓN ==================
st.subheader("Configurar conexiones a bases de datos")

col_pg, col_mongo = st.columns(2)

# ---- PostgreSQL ----
with col_pg:
    st.markdown("### PostgreSQL")

    new_pg = ""  # para capturar cambios
    if existing_pg:
        st.write("Conexión actual (enmascarada):")
        st.code(mask_uri(existing_pg), language="text")
        edit_pg = st.checkbox("Editar URL de PostgreSQL", key="edit_pg")
        if edit_pg:
            new_pg = st.text_input(
                "Nueva URL de PostgreSQL",
                type="password",
                help="Pega aquí la URL completa si quieres actualizarla.",
            )
    else:
        st.info("No se ha configurado todavía la URL de PostgreSQL.")
        new_pg = st.text_input(
            "URL de PostgreSQL",
            placeholder="postgresql://user:password@host:puerto/database",
            type="password",
        )

# ---- MongoDB ----
with col_mongo:
    st.markdown("### MongoDB")

    new_mongo = ""
    if existing_mongo:
        st.write("Conexión actual (enmascarada):")
        st.code(mask_uri(existing_mongo), language="text")
        edit_mongo = st.checkbox("Editar URL de MongoDB", key="edit_mongo")
        if edit_mongo:
            new_mongo = st.text_input(
                "Nueva URL de MongoDB",
                type="password",
                help="Pega aquí la URL completa si quieres actualizarla.",
            )
    else:
        st.info("No se ha configurado todavía la URL de MongoDB.")
        new_mongo = st.text_input(
            "URL de MongoDB",
            placeholder="mongodb+srv://user:password@cluster.mongodb.net/dbname",
            type="password",
        )

# ================== GUARDAR CAMBIOS ==================
if st.button("Guardar configuración"):
    pg_to_save = existing_pg
    mongo_to_save = existing_mongo

    if new_pg.strip():
        pg_to_save = new_pg.strip()
    if new_mongo.strip():
        mongo_to_save = new_mongo.strip()

    # Leemos líneas actuales del .env
    lines = []
    if os.path.exists(env_file):
        with open(env_file, "r") as f:
            lines = f.read().splitlines()

    # Filtramos las líneas anteriores de esas keys
    filtered = [
        line
        for line in lines
        if not line.startswith(f"{postgresql_key}=")
        and not line.startswith(f"{mongo_db_key}=")
    ]

    # Agregamos las nuevas si existen
    if pg_to_save:
        filtered.append(f"{postgresql_key}={pg_to_save}")
        os.environ[postgresql_key] = pg_to_save
        data_access[postgresql_key] = pg_to_save

    if mongo_to_save:
        filtered.append(f"{mongo_db_key}={mongo_to_save}")
        os.environ[mongo_db_key] = mongo_to_save
        data_access[mongo_db_key] = mongo_to_save

    # Escribimos el .env actualizado
    with open(env_file, "w") as f:
        f.write("\n".join(filtered) + ("\n" if filtered else ""))

    st.success("Configuración guardada correctamente ✅")

# Si ya había valores y no se dio clic en guardar, igual los ponemos en data_access
if existing_pg and postgresql_key not in data_access:
    data_access[postgresql_key] = existing_pg
if existing_mongo and mongo_db_key not in data_access:
    data_access[mongo_db_key] = existing_mongo

st.write("Estado actual de `data_access` (solo para depuración):")
st.json({k: mask_uri(v) for k, v in data_access.items()})