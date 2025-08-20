import os
import subprocess

def message_print(message):
    """Formatea mensajes con asteriscos para destacarlos"""
    message_highlights = '*' * len(message)
    return f'\n{message_highlights}\n{message}\n{message_highlights}\n'

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