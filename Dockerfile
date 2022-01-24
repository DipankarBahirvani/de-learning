FROM python:3.9
RUN pip install pandas
RUN pip install sqlalchemy
RUN pip install psycopg2-binary
WORKDIR /ingestion
COPY ingestion/ingest_data.py  ingest_data.py
RUN mkdir -p test_data
ENTRYPOINT ["python","ingest_data.py"]