#!/bin/bash

ln -s /usr/bin/python3 /usr/bin/python &&
add-apt-repository ppa:deadsnakes/ppa
apt update && apt install python3.9
apt install python3-pip && pip3 install -r requirements.txt
apt install python3-tk
