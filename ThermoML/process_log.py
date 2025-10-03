"""Module to process RegNum errors from ThermoML XML files"""

import re
import xml.etree.ElementTree as ET
from pathlib import Path

REGNUM_PATTERN = re.compile(
    r"^Error parsing (.+?\.xml) \[UnrecognizedContentError\]: Invalid content.*\}RegNum"
)


def collect_regnum_error_files(log_path: Path):
    "collect regnum error files"
    files = []
    if not log_path.exists():
        print(f"error log não encontrado: {log_path}")
        return files
    with log_path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            m = REGNUM_PATTERN.match(line.strip())
            if m:
                files.append(Path(m.group(1)))
    # remover duplicados preservando ordem
    seen = set()
    unique = []
    for p in files:
        if p not in seen:
            seen.add(p)
            unique.append(p)
    return unique


NS_URI = "http://www.iupac.org/namespaces/ThermoML"
REGNUM_QNAME = f"{{{NS_URI}}}RegNum"


def remove_empty_regnum(xml_path: Path):
    "remove empty regnum"
    try:
        ET.register_namespace("", NS_URI)  # preservar namespace default
        tree = ET.parse(xml_path)
        root = tree.getroot()
    except ET.ParseError as e:
        return {
            "file": xml_path,
            "status": "parse_error",
            "detail": str(e),
            "removed": 0,
        }

    to_remove = []
    for elem in root.iter(REGNUM_QNAME):
        text_ok = (elem.text is None) or (elem.text.strip() == "")
        no_children = len(list(elem)) == 0
        if text_ok and no_children:
            parent = _find_parent(root, elem)
            if parent is not None:
                to_remove.append((parent, elem))

    for parent, child in to_remove:
        parent.remove(child)

    if to_remove:
        backup = xml_path.with_suffix(xml_path.suffix + ".bak")
        if not backup.exists():
            backup.write_bytes(xml_path.read_bytes())
        tree.write(xml_path, encoding="utf-8", xml_declaration=True)

    return {"file": xml_path, "status": "ok", "removed": len(to_remove)}


def _find_parent(root, target):
    # xml.etree não tem getparent; busca manual
    for parent in root.iter():
        for child in list(parent):
            if child is target:
                return parent
    return None


def process_all(error_log: Path):
    "process all files"
    files = collect_regnum_error_files(error_log)
    print(f"Arquivos com erro RegNum: {len(files)}")
    results = []
    total_removed = 0
    for f in files:
        if not f.exists():
            results.append({"file": f, "status": "missing", "removed": 0})
            continue
        r = remove_empty_regnum(f)
        results.append(r)
        total_removed += r.get("removed", 0)
    # resumo
    ok = sum(1 for r in results if r["status"] == "ok")
    skipped = sum(1 for r in results if r["status"] != "ok")
    print(
        f"Processados OK: {ok} | Skipped: {skipped} | RegNum vazios removidos: {total_removed}"
    )
    # listar somente os que removeram algo
    for r in results:
        if r["removed"]:
            print(f"- {r['file']}: {r['removed']} removidos")
    return results
