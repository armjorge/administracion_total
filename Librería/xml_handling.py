import os
import re
import xml.etree.ElementTree as ET


def pretty_xml(xml_string):
    # Add line breaks between tags (e.g., > followed by <)
    xml_string = re.sub(r'>\s*<', '>\n<', xml_string)
    return xml_string

def pretty_xml_files(directory):
    for filename in os.listdir(directory):
        if filename.endswith('.xml'):
            filepath = os.path.join(directory, filename)
            with open(filepath, 'r', encoding='utf-8') as file:
                content = file.read()
                pretty_content = pretty_xml(content)

            with open(filepath, 'w', encoding='utf-8') as file:
                file.write(pretty_content)
            print(f"XML estructurado: {filename}")





def extract_total_and_fecha_pago(xml_folder):
    total_data = []

    for xml_file in os.listdir(xml_folder):
        if not xml_file.endswith('.xml'):
            continue

        path = os.path.join(xml_folder, xml_file)

        try:
            tree = ET.parse(path)
            root = tree.getroot()

            ns = {
                'cfdi': 'http://www.sat.gob.mx/cfd/4',
                'nomina12': 'http://www.sat.gob.mx/nomina12'
            }

            if root.tag.endswith('Comprobante'):
                total = root.attrib.get('Total') or root.attrib.get('total')

                # Buscar el nodo <cfdi:Complemento>/<nomina12:Nomina>
                complemento = root.find('cfdi:Complemento', ns)
                fecha_pago = None

                if complemento is not None:
                    nomina = complemento.find('nomina12:Nomina', ns)
                    if nomina is not None:
                        fecha_pago = nomina.attrib.get('FechaPago')

                total_data.append({
                    'filename': os.path.basename(xml_file),
                    'Total': float(total) if total else None,
                    'FechaPago': fecha_pago
                })

            else:
                print(f"\t⚠️ Nodo raíz no es Comprobante en: {xml_file}")

        except ET.ParseError as e:
            print(f"\t❌ Error al parsear {xml_file}: {e}")

    # Mostrar resultados
    print("\n📄 Datos extraídos:")
    for item in total_data:
        print(f"\t{item['filename']} → Total: {item['Total']}, FechaPago: {item['FechaPago']}")

    return total_data