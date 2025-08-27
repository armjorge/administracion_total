import os
import pickle
import subprocess


class Helper:
    @staticmethod
    def message_print(message):
        """Formatea mensajes con asteriscos para destacarlos"""
        message_highlights = '*' * len(message)
        return f'\n{message_highlights}\n{message}\n{message_highlights}\n'

    @staticmethod
    def create_directory_if_not_exists(path_or_paths):
        """Crea directorios si no existen"""
        if isinstance(path_or_paths, str):
            paths = [path_or_paths]
        else:
            paths = path_or_paths

        for path in paths:
            if not os.path.exists(path):
                print(f"\tCreando directorio: {os.path.basename(path)}")
                os.makedirs(path)
            else:
                print(f"\tDirectorio encontrado: {os.path.basename(path)}")

    @staticmethod
    def add_to_gitignore(root_directory, path_to_add):
        """Añade una ruta al archivo .gitignore"""
        gitignore_path = os.path.join(root_directory, ".gitignore")
        relative_output = f"{os.path.basename(path_to_add)}/"

        if os.path.exists(gitignore_path):
            with open(gitignore_path, 'r') as f:
                lines = f.read().splitlines()
        else:
            lines = []

        if relative_output not in lines:
            with open(gitignore_path, 'a') as f:
                f.write(f"\n{relative_output}\n")
            print(f"'{relative_output}' agregado a .gitignore.")

    @staticmethod
    def open_folder(os_path):
        """Abre una carpeta en el explorador de archivos"""
        try:
            if os.name == 'nt':  # Windows
                os.startfile(os_path)
            elif os.name == 'posix':  # macOS o Linux
                if "darwin" in os.uname().sysname.lower():  # macOS
                    subprocess.run(["open", os_path])
                else:  # Linux
                    subprocess.run(["xdg-open", os_path])
        except Exception as e:
            print(f"Error opening folder: {e}")

    @staticmethod
    def load_pickle_as_dataframe(file_path):
        """Carga un archivo pickle como DataFrame si existe y es válido"""
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            return None
        try:
            with open(file_path, 'rb') as f:
                df = pickle.load(f)
                return df
        except Exception as e:
            print(f"Error loading pickle file: {e}")
            return None
    @staticmethod
    def get_files_in_directory(directory):
        return [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
    @staticmethod
    def get_file_headers(file_path):
        import pandas as pd
        try:
            df = pd.read_csv(file_path, nrows=1)
            return df.columns.tolist()
        except Exception as e:
            print(f"Error al leer el archivo {file_path}: {e}")
            return []
    @staticmethod  
    def merge_files(file_paths):
        import pandas as pd
        try:
            dfs = [pd.read_csv(file) for file in file_paths]
            merged_df = pd.concat(dfs, ignore_index=True)
            merged_file_path = os.path.join(os.path.dirname(file_paths[0]), "merged_file.csv")
            merged_df.to_csv(merged_file_path, index=False)
            return merged_file_path
        except Exception as e:
            print(f"Error al fusionar archivos: {e}")
            return None
    @staticmethod
    def move_file(source, destination):
        import shutil
        try:
            shutil.move(source, destination)
            print(f"Archivo movido de {source} a {destination}")
        except Exception as e:
            print(f"Error al mover el archivo {source}: {e}")