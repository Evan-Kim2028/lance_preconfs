#!/bin/bash

source /home/ubuntu/lance_preconfs/.venv/bin/activate
python /home/ubuntu/lance_preconfs/read_db/query_commitments.py > /home/ubuntu/lance_preconfs/query_commitments.log 2>&1 &
python /home/ubuntu/lance_preconfs/read_db/query_mev_boost.py > /home/ubuntu/lance_preconfs/query_mev_boost.log 2>&1 &
/home/ubuntu/lance_preconfs/.venv/bin/marimo run preconf_analytics_app.py --host 0.0.0.0 --port 5008 > /home/ubuntu/lance_preconfs/marimo.log 2>&1
