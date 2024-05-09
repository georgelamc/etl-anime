FROM python:latest
RUN apt-get update && apt-get install -y postgresql-client
COPY /scripts/etl_script.py .
CMD ["python", "etl_script.py"]
