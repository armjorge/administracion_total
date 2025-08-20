#!/usr/bin/env python3
import os
import subprocess
import sys
from pathlib import Path
from shutil import which

def find_plantuml_cmd():
    """
    Busca cómo ejecutar PlantUML:
      1) 'plantuml' en PATH
      2) 'java -jar plantuml.jar' si existe en el repo
    """
    if which("plantuml"):
        return ["plantuml"]
    for candidate in [Path("plantuml.jar"), Path.cwd() / "plantuml.jar"]:
        if candidate.exists():
            return ["java", "-jar", str(candidate)]
    return None

def newer_than(src: Path, dst: Path) -> bool:
    """True si dst existe y es más nuevo que src."""
    return dst.exists() and dst.stat().st_mtime >= src.stat().st_mtime

def render_puml(cmd, puml_file: Path, out_dir: Path, fmt: str):
    """Renderiza un archivo .puml a formato png/svg."""
    out_dir.mkdir(parents=True, exist_ok=True)
    args = cmd + [f"-t{fmt}", "-o", str(out_dir.resolve()), str(puml_file.name)]
    subprocess.run(
        args, cwd=puml_file.parent, check=True,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )

def diagram_generator(diagram_folder: str, out_dir: str = None, formats=("svg",)):
    """
    Busca todos los .puml en diagram_folder y genera imágenes en out_dir.
    
    Args:
        diagram_folder: Directorio donde buscar archivos .puml
        out_dir: Directorio de salida (por defecto: mismo directorio del script)
        formats: Tupla de formatos a generar. Opciones:
                - ("svg",) - Solo SVG (por defecto)
                - ("png",) - Solo PNG  
                - ("svg", "png") - Ambos formatos
    """
    cmd = find_plantuml_cmd()
    if not cmd:
        print("⚠️ No encontré PlantUML. Instala con:")
        print("   brew install plantuml graphviz   (macOS)")
        print("   sudo apt-get install plantuml graphviz   (Linux)")
        sys.exit(1)

    src_dir = Path(diagram_folder)
    
    # Si no se especifica out_dir, usar el mismo directorio del script
    if out_dir is None:
        dst_dir = Path(__file__).parent
    else:
        dst_dir = Path(out_dir)

    if not src_dir.exists():
        print(f"⚠️ El directorio {src_dir} no existe.")
        return

    puml_files = list(src_dir.rglob("*.puml"))
    if not puml_files:
        print(f"ℹ️ No se encontraron .puml en {src_dir}")
        return

    total_rendered = 0
    for puml in puml_files:
        for fmt in formats:
            out_file = dst_dir / f"{puml.stem}.{fmt}"
            if newer_than(puml, out_file):
                continue
            print(f"🧩 Render: {puml} -> {out_file.name}")
            render_puml(cmd, puml, dst_dir, fmt)
            total_rendered += 1

    print(f"✅ Listo. {total_rendered} imágenes generadas/actualizadas.")
    print(f"📁 Salida: {dst_dir.resolve()}")

if __name__ == "__main__":
    # Usar el directorio donde está este script como fuente y destino
    script_dir = Path(__file__).parent
    
    # Puedes cambiar los formatos aquí según necesites:
    # - ("svg",) para solo SVG (por defecto)
    # - ("png",) para solo PNG
    # - ("svg", "png") para ambos formatos
    
    diagram_generator(str(script_dir), formats=("svg",))