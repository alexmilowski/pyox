#!/bin/bash

conda create -n fernet-key

source activate fernet-key

pip install cryptography

python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode('utf-8'))"
