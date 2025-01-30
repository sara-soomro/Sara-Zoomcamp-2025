
FROM python:3.9.1

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY upload-data-green.py upload-data-green.py 

ENTRYPOINT [ "python", "upload-data-green.py" ]
