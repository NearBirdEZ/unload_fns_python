#!/bin/bash
add-apt-repository ppa:deadsnakes/ppa
apt update && apt install python3.9
ln -s /usr/bin/python3 /usr/bin/python
echo "alias python3='python3.9'" >> ~/.bashrc
apt install python3-pip && pip3 install -r requirements.txt
apt install python3-tk
