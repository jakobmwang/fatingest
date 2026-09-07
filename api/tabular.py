"""Tabular data (spreadsheets, parquet, json, ndjson, csv) to GFM pipe-table chunks.

Spreadsheets are tabular containers, not documents: converting them to PDF yields
arbitrary page breaks and mangled columns, so they take this route instead. Each
sheet becomes its own section; chunking batches rows by character budget with the
header repeated per chunk, so no chunk ever mixes sheets or loses its column context.
Raises on anything that is not confidently tabular (the router treats that as
"try the next route").
"""
import io

import clevercsv
import polars as pl

SPREADSHEET_EXTENSIONS = ("xlsx", "xls", "ods")
CHUNK_CHAR_BUDGET = 3500
MIN_ROWS, MIN_COLS = 5, 2   # below this, "csv" is more likely prose with commas


def tabular_to_chunks(data: bytes, extension: str | None = None) -> tuple[list[dict], dict]:
    """[{markdown, meta}] and the file meta. Each chunk's meta says which sheet it comes from
    (when the format has sheets) and which rows, as RowIDs, so a reader can place it."""
    sheets, meta = _read(data, extension)
    chunks: list[dict] = []
    total_rows = 0
    for name, df in sheets.items():
        df = _flatten(df)
        df = df.with_columns(pl.int_range(1, df.height + 1).alias("RowID")).select(
            ["RowID"] + df.columns)
        df = df.with_columns([
            pl.col(c).cast(pl.Utf8, strict=False).fill_null("")
              .str.replace_all(r"\s+", " ").str.replace_all(r"\|", "¦")
              .str.strip_chars().alias(c)
            for c in df.columns])
        total_rows += df.height
        heading = f"## {name}\n\n" if name else ""
        header = "| " + " | ".join(df.columns) + " |\n|" + "---|" * len(df.columns) + "\n"
        rows = ["| " + " | ".join("" if v is None else str(v) for v in row) + " |"
                for row in df.iter_rows()]

        def emit(batch: list[str], first: int, last: int):
            cmeta = {"status": "ok", "rows": [first, last]}
            if name:
                cmeta["sheet"] = name
            chunks.append({"markdown": heading + header + "\n".join(batch), "meta": cmeta})

        batch: list[str] = []
        size = len(heading) + len(header)
        first = 1
        for i, row in enumerate(rows, start=1):
            if batch and size + len(row) > CHUNK_CHAR_BUDGET:
                emit(batch, first, i - 1)
                batch, size, first = [], len(heading) + len(header), i
            batch.append(row)
            size += len(row) + 1
        if batch:
            emit(batch, first, len(rows))
    meta |= {"num_rows": total_rows, "num_sheets": len(sheets)}
    return chunks, meta


def _read(data: bytes, extension: str | None) -> tuple[dict[str, pl.DataFrame], dict]:
    if extension in SPREADSHEET_EXTENSIONS:
        sheets = pl.read_excel(io.BytesIO(data), sheet_id=0)   # 0 = all sheets
        if isinstance(sheets, pl.DataFrame):
            sheets = {"": sheets}
        return sheets, {"kind": "spreadsheet", "content_type":
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"}
    try:
        return {"": pl.read_parquet(io.BytesIO(data))}, \
               {"kind": "tabular", "content_type": "application/vnd.apache.parquet"}
    except Exception:
        pass
    try:
        return {"": pl.read_json(io.BytesIO(data))}, \
               {"kind": "tabular", "content_type": "application/json"}
    except Exception:
        pass
    try:
        return {"": pl.read_ndjson(io.BytesIO(data))}, \
               {"kind": "tabular", "content_type": "application/json"}
    except Exception:
        pass
    text = data.decode("utf-8", errors="replace")
    dialect = clevercsv.Sniffer().sniff(text, verbose=False)
    if dialect is None or not dialect.delimiter:
        raise ValueError("not tabular")
    df = pl.read_csv(io.BytesIO(data), separator=dialect.delimiter,
                     quote_char=dialect.quotechar or None,
                     has_header=clevercsv.Sniffer().has_header(text),
                     infer_schema_length=10000, ignore_errors=True,
                     truncate_ragged_lines=True)
    if df.height < MIN_ROWS or df.width < MIN_COLS:
        raise ValueError("not confidently tabular")
    return {"": df}, {"kind": "tabular", "content_type": "text/csv"}


def _flatten(df: pl.DataFrame) -> pl.DataFrame:
    while any(isinstance(t, (pl.Struct, pl.List)) for t in df.dtypes):
        for col in df.columns:
            if isinstance(df[col].dtype, pl.Struct):
                un = df[col].struct.unnest()
                df = df.drop(col).hstack(un.rename({c: f"{col}.{c}" for c in un.columns}))
            elif isinstance(df[col].dtype, pl.List):
                df = df.with_columns(pl.col(col).map_elements(
                    lambda v: "" if v is None else ", ".join(
                        "" if x is None else str(x) for x in v),
                    return_dtype=pl.Utf8).alias(col))
    return df
