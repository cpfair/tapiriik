FROM ubuntu:latest

ENV TZ=Europe/Riga
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Install python
RUN apt-get update \
  && apt-get install -y python3-pip python3-dev git \
  && cd /usr/local/bin \
  && ln -s /usr/bin/python3 python \
  && pip3 install --upgrade pip

# Install  libs
RUN apt-get -y install git libxslt-dev libxml2-dev python3-lxml python3-crypto

WORKDIR /tapiriik

# Copy requirements for build and only rebuild if requirements have changed
COPY requirements.txt /tapiriik
RUN pip3 install -r requirements.txt

# Copy code
COPY . /tapiriik

ENTRYPOINT "/bin/bash -c '/tapiriik/bootstrap-docker-compose.sh && python3 manage.py runserver 0.0.0.0:8000'"
