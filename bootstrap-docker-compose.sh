#!/usr/bin/env bash

# Put in a default local_settings.py (if one doesn't exist)
if [ ! -f tapiriik/local_settings.py ]; then
    cp tapiriik/local_settings.py.local tapiriik/local_settings.py

    # Generate credential storage keys
    if [ ! -f persistent/credentialstore_key.py ]; then
        python3 credentialstore_keygen.py > persistent/credentialstore_key.py
    fi
    cat persistent/credentialstore_key.py >> tapiriik/local_settings.py
fi
