FROM python:3.9.1

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY ingest_hw.py ingest_hw.py 

ENTRYPOINT [ "python", "ingest_hw.py"]