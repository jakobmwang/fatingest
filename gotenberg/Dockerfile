# Gotenberg with deployment-specific fonts baked in. LibreOffice substitutes missing
# fonts, which changes layout AND pagination - and pages are our chunk unit, so font
# fidelity matters. Put your fonts in fonts/ (not committed; often license-restricted).
# Pinned by version AND digest: conversion output feeds render hashes, so a Gotenberg
# upgrade is a deliberate re-index event, never a side effect of a rebuild.
FROM gotenberg/gotenberg:8.36.0@sha256:87c16b9f364279d321bc9772d31fa58aa6abe036423c270698bd636c3a8e9466

USER root
COPY fonts/ /usr/share/fonts/truetype/custom/
RUN fc-cache -f
USER gotenberg
