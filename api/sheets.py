"""Spreadsheets: the drawings a cell reader never sees.

A spreadsheet's cells go through tabular.py, exactly and deterministically. Charts, images,
shapes and text boxes live outside the cells, and only a renderer shows them. LibreOffice
(through Gotenberg) renders a workbook with one page per sheet; on that page PDFium can list
what is drawn, and pdf_engine.drawn_regions renders each drawing alone at a proper scale, so
a chart on a 20,000-row sheet comes out legible while the cells are never rendered at all.

Two preparations make that work. has_drawings() tells, from the file's own structure,
whether there is anything to render, so a plain table costs no conversion. strip_formatting()
removes cell formatting before the conversion: fills, borders, conditional formatting and
the like are drawn as thousands of small shapes and would turn every coloured table into a
"drawing"; the cells' values, and the drawings themselves, are left untouched. Only OOXML
(.xlsx) is handled here; the cell route serves every spreadsheet format.
"""
import io
import re
import zipfile
import xml.etree.ElementTree as ET

_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
_RELS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
_DRAWING_PARTS = ("xl/drawings/", "xl/charts/", "xl/media/")
_SHEET_XML = re.compile(r"^xl/worksheets/sheet\d+\.xml$")


def is_ooxml_workbook(data: bytes) -> bool:
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as z:
            return "xl/workbook.xml" in z.namelist()
    except zipfile.BadZipFile:
        return False


def has_drawings(data: bytes) -> bool:
    """Whether the workbook carries anything drawn - a chart, a picture, a shape - which is
    visible in its zip structure before anything is parsed."""
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        return any(n.startswith(_DRAWING_PARTS) for n in z.namelist())


def visible_sheets(data: bytes) -> list[str]:
    """Sheet names in workbook order, hidden sheets left out: LibreOffice exports one page per
    visible sheet, in this order."""
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        root = ET.fromstring(z.read("xl/workbook.xml"))
    sheets = root.find(f"{{{_NS}}}sheets")
    if sheets is None:
        return []
    return [s.get("name", "") for s in sheets if s.get("state", "visible") == "visible"]


def strip_formatting(data: bytes) -> bytes:
    """The workbook with every cell fill set to none, every border emptied, conditional
    formatting and its differential styles removed, and print areas dropped (a print area
    could leave a chart outside the exported page). Cell values, sheet layout and all
    drawings are byte-for-byte what they were."""
    src = zipfile.ZipFile(io.BytesIO(data))
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as dst:
        for info in src.infolist():
            part = src.read(info.filename)
            if info.filename == "xl/styles.xml":
                part = _strip_styles(part)
            elif info.filename == "xl/workbook.xml":
                part = _strip_print_areas(part)
            elif _SHEET_XML.match(info.filename):
                part = _strip_sheet(part)
            dst.writestr(info, part)
    return out.getvalue()


def _strip_styles(xml: bytes) -> bytes:
    ET.register_namespace("", _NS)
    root = ET.fromstring(xml)
    fills = root.find(f"{{{_NS}}}fills")
    if fills is not None:
        for fill in list(fills):
            for child in list(fill):
                fill.remove(child)
            ET.SubElement(fill, f"{{{_NS}}}patternFill", patternType="none")
    borders = root.find(f"{{{_NS}}}borders")
    if borders is not None:
        for border in list(borders):
            for child in list(border):
                border.remove(child)
            for side in ("left", "right", "top", "bottom", "diagonal"):
                ET.SubElement(border, f"{{{_NS}}}{side}")
    dxfs = root.find(f"{{{_NS}}}dxfs")
    if dxfs is not None:
        for dxf in list(dxfs):
            for child in list(dxf):
                dxf.remove(child)
    return ET.tostring(root, xml_declaration=True, encoding="UTF-8")


def _strip_sheet(xml: bytes) -> bytes:
    text = xml.decode("utf-8")
    text = re.sub(r"<conditionalFormatting\b.*?</conditionalFormatting>", "", text, flags=re.S)
    text = re.sub(r"<extLst\b.*?</extLst>", "", text, flags=re.S)   # x14 conditional formats, sparklines
    return text.encode("utf-8")


def _strip_print_areas(xml: bytes) -> bytes:
    text = xml.decode("utf-8")
    text = re.sub(r"<definedName\b[^>]*name=\"_xlnm\.Print_Area\"[^>]*>.*?</definedName>", "", text, flags=re.S)
    return text.encode("utf-8")
