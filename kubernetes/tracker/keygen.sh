#!/bin/bash

conda create -n fernet-key

source activate fernet-key

pip install cryptography

python -c "from cryptography.fernet import Fernet, base64; print(base64.b64encode(Fernet.generate_key()).decode('utf-8'))"
