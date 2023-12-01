#!/usr/bin/env bash

# Install system requirements
sudo apt-get update
sudo apt-get install -y python3-pip libxml2-dev libxslt-dev zlib1g-dev git redis-server rabbitmq-server mongodb-server

# upgrade pip
pip3 install --upgrade pip

# Fix the default python and pip instance
update-alternatives --install /usr/bin/python python /usr/bin/python2.7 1
update-alternatives --install /usr/bin/python python /usr/bin/python3.6 2
update-alternatives --install /usr/bin/pip pip /usr/local/bin/pip3 1
update-alternatives --force --install /usr/bin/pip3 pip3 /usr/local/bin/pip3 1

# Install app requirements
pip install --upgrade -r /vagrant/requirements.txt

# Put in a default local_settings.py (if one doesn't exist)
if [ ! -f /vagrant/tapiriik/local_settings.py ]; then
    cp /vagrant/tapiriik/local_settings.py.example /vagrant/tapiriik/local_settings.py
    # Generate credential storage keys
    python /vagrant/credentialstore_keygen.py >> /vagrant/tapiriik/local_settings.py
fi

