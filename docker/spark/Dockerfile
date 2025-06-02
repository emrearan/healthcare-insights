FROM bitnami/spark:3.5.0

USER root

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk curl && \
    mkdir -p /usr/share/java && \
    curl -o /usr/share/java/postgresql.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar


RUN pip3 install pipenv
COPY ./Pipfile Pipfile.lock /app/
WORKDIR /app
RUN pipenv install --system --deploy

ENV PYSPARK_PYTHON=python3
