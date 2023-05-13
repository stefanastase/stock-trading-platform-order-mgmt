# syntax=docker/dockerfile:1
FROM python:3.7-slim-buster
WORKDIR /code
RUN python -m pip install --upgrade pip
RUN apt-get update && apt-get -y install libpq-dev gcc

ENV FLASK_APP=app.py
ENV FLASK_RUN_HOST=0.0.0.0

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt 

EXPOSE 5000

COPY . .

CMD ["flask", "run"]
