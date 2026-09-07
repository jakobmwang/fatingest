# Test image: the product image plus pymupdf, which the suites use to BUILD fixture PDFs
# (pages, links, form fields, annotations). pymupdf is AGPL-licensed and is a test-only
# dependency: it is never part of the product image.
ARG BASE=fatingest-api:dev
FROM ${BASE}
USER root
RUN pip install --no-cache-dir pymupdf openpyxl
USER fatingest
