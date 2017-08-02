FROM ubuntu:16.04

COPY . /tapiriik

RUN apt-get update \
    && apt-get install -y \
        git \
        python3-pip \
        libxslt-dev \
        libxml2-dev \
        python3-lxml \
        python3-crypto \
    && cd /tapiriik \
    && pip3 install -r requirements.txt \
    && cp tapiriik/local_settings.py.example tapiriik/local_settings.py \
    && python3 credentialstore_keygen.py >> tapiriik/local_settings.py

WORKDIR /tapiriik

RUN echo "" >> tapiriik/local_settings.py \
    && echo 'MONGO_HOST = "mongodb"' >> tapiriik/local_settings.py \
    && echo 'REDIS_HOST = "redis"' >> tapiriik/local_settings.py \
    && echo 'RABBITMQ_BROKER_URL = "amqp://guest@rabbitmq//"' >> tapiriik/local_settings.py \
    && echo -n "\n\nHow fun is that?\n\n\n"

ENTRYPOINT ["python3","/tapiriik/manage.py","runserver"]
CMD ["0.0.0.0:8000"]

EXPOSE 8000
