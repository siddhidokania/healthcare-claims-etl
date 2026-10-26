# Runner + openpyxl + LibreOffice Calc, used to build and recalculate the Excel workbook.
FROM claims-etl-runner:latest

RUN apt-get update \
    && apt-get install -y --no-install-recommends libreoffice-calc-nogui \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir openpyxl==3.1.5
