FROM python:latest
RUN apt-get update && \
    apt-get install -y postgresql-client && \
    pip install psycopg2 requests
COPY /scripts/etl_script.py .
CMD ["python", "-u", "etl_script.py"]
