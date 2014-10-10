#!/usr/bin/env bash

# Install system requirements
sudo apt-get update
sudo apt-get install -y python3-pip libxml2-dev libxslt-dev zlib1g-dev mongodb git redis-server rabbitmq-server

# Fix pip
pip3 install --upgrade pip

# Install app requirements
pip install -r /vagrant/requirements.txt

# Fix the default python instance
sudo rm `which python`
sudo ln -s /usr/bin/python3.3 /usr/bin/python

# Put in a default local_settings.py
cp /vagrant/tapiriik/local_settings.py.example /vagrant/tapiriik/local_settings.py
