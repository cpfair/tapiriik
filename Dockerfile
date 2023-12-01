FROM ubuntu:latest

# install python
RUN apt-get update \
  && apt-get install -y python3-pip python3-dev \
  && cd /usr/local/bin \
  && ln -s /usr/bin/python3 python \
  && pip3 install --upgrade pip

# copy project
COPY . /

# install  libs
RUN apt-get -y install git libxslt-dev libxml2-dev python3-lxml python3-crypto

# install requirements 
RUN pip3 install -r requirements.txt

# rename settings example com
RUN cp tapiriik/local_settings.py.example tapiriik/local_settings.py 

# generate keys
RUN python3 credentialstore_keygen.py >> tapiriik/local_settings.py

# run server, worker and scheduler
ENTRYPOINT python3 manage.py runserver 0.0.0.0:8000 && python3 sync_worker.py && python3 sync_scheduler.py