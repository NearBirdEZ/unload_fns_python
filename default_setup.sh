#!/bin/bash
add-apt-repository ppa:deadsnakes/ppa
apt update && apt install python3.9
update-alternatives --install /usr/bin/python3 python /usr/bin/python3.9 2
apt install python3-pip && pip3 install -r requirements.txt
apt install python3-tk
