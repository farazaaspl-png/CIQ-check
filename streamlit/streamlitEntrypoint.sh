#!/bin/sh
python get_config_v2.py

exec python -m streamlit run main.py