FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends fonts-dejavu \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir "fastapi" "uvicorn[standard]" "psycopg[binary]" "pillow" \
    "filetype" "httpx" "polars" "fastexcel" "clevercsv" \
    "pypdfium2==5.10.1" "liteparse==2.14.3"
# Match the uid to the owner of the bind-mounted store/ directory on the host.
ARG APP_UID=1001
RUN useradd -m -u ${APP_UID} fatingest
USER fatingest
WORKDIR /app
COPY app.py models.py store.py archive_to_files.py unpack.py refs.py pdf_engine.py \
     text_utils.py tabular.py routing.py sheets.py prompt.md ./
EXPOSE 8000
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
