import os
import glob
import pandas as pd
import hashlib
from collections import defaultdict


def export_pickle(pickle_file, output_tc_al_corte):
    print("📦 Cargando archivo pickle...")
    try:
        df = pd.read_pickle(pickle_file)

        # 🔁 Asegura formato correcto en 'Fecha'
        if 'Fecha' in df.columns:
            df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce')
            df['Fecha'] = df['Fecha'].dt.strftime('%d/%m/%Y')

        df.to_excel(output_tc_al_corte, index=False)
        print(f"✅ Información exportada exitosamente a: {output_tc_al_corte}")
    except Exception as e:
        print(f"❌ Error al exportar el archivo: {e}")

def hash_file_content(file_path):
    """Returns the SHA256 hash of a file's content."""
    with open(file_path, 'rb') as f:
        return hashlib.sha256(f.read()).hexdigest()

def read_create_pickle(pickle_file, columns): 
    if os.path.exists(pickle_file):
        df_tc_closed = pd.read_pickle(pickle_file)
        print("✅ Archivo con información cargado.")
    else:
        df_empty = pd.DataFrame(columns=columns + ['file_date', 'file_name'])
        df_empty.to_pickle(pickle_file)
        df_tc_closed = df_empty
        print("📁 Archivo no localizado, creando nuevo archivo.")
    return df_tc_closed

def process_closed_credit_accounts(folder_TC_al_corte, columns):
    csv_files = glob.glob(os.path.join(folder_TC_al_corte, '*.csv'))
    pickle_file = os.path.join(folder_TC_al_corte, 'pickle_database.pkl')
    df_readed = read_create_pickle(pickle_file, columns)
    
    # Step 1: Build hash for all CSVs and detect internal duplicates
    hash_to_files = defaultdict(list)
    file_hashes = {}

    for csv_file in csv_files:
        file_hash = hash_file_content(csv_file)
        file_hashes[csv_file] = file_hash
        hash_to_files[file_hash].append(csv_file)

    # Highlight duplicates among CSV files (even with different names)
    for file_hash, files in hash_to_files.items():
        if len(files) > 1:
            print(f"\n*********\n⚠️ Archivos con contenido duplicado entre sí: {[os.path.basename(f) for f in files]}\n*********\n")

    # Extract previously recorded hashes from pickle
    existing_hashes = set(df_readed['file_name'].apply(lambda x: x.split("__HASH__")[-1]) if not df_readed.empty else [])

    updated = False
    files_processed = 0

    for csv_file in csv_files:
        file_hash = file_hashes[csv_file]
        if file_hash in existing_hashes:
            #print(f"🔁 Archivo ya registrado previamente: {os.path.basename(csv_file)}")
            continue
        if len(hash_to_files[file_hash]) > 1 and hash_to_files[file_hash][0] != csv_file:
            # Skip all but the first occurrence of duplicated content
            print(f"🚫 Contenido duplicado ignorado: {os.path.basename(csv_file)}")
            continue

        try:
            df_csv_try = pd.read_csv(csv_file)
            if not all(col in df_csv_try.columns for col in columns):
                print(f"❌ Archivo excluido por encabezados incorrectos: {os.path.basename(csv_file)}")
                continue
        except Exception as e:
            print(f"⚠️ Error al leer {os.path.basename(csv_file)}: {e}")
            continue

        try:
            file_date = pd.to_datetime(df_csv_try['Fecha'].iloc[0], dayfirst=True)
        except Exception:
            file_date = pd.to_datetime(os.path.getmtime(csv_file), unit='s')
        df_csv['Fecha'] = pd.to_datetime(df_csv['Fecha'], dayfirst=True, errors='coerce')
        df_csv = df_csv_try[columns].copy()
        df_csv['file_date'] = file_date.strftime('%d/%m/%Y')
        df_csv['file_name'] = f"{os.path.basename(csv_file)}__HASH__{file_hash}"

        df_readed = pd.concat([df_readed, df_csv], ignore_index=True)
        existing_hashes.add(file_hash)
        updated = True
        files_processed += 1
        print(f"✅ Archivo nuevo agregado: {os.path.basename(csv_file)}")

    if updated:
        df_readed['Fecha'] = pd.to_datetime(df_readed['Fecha'], dayfirst=True, errors='coerce')
        df_readed.sort_values(by='file_date', ascending=False, inplace=True)
        df_readed.to_pickle(pickle_file)
        print("💾 Cambios guardados en el archivo pickle.")
    else:
        print("📭 No hay archivos nuevos para registrar.")

    #print("\n📋 Vista previa de los primeros registros:")
    #print(df_readed.head(10))

    #return df_readed