#!/usr/bin/env bash
# End-to-end local run: produce the sample stream, run the medallion
# pipeline, then read the Delta tables back.
# Prereqs: Python 3.10+, Java 17 (JAVA_HOME set), `pip install -r requirements.txt`
set -euo pipefail
cd "$(dirname "$0")"

rm -rf output data/stream_landing   # clean slate so reruns are deterministic
python src/producer.py --delay 0
python src/pipeline.py
python src/show_outputs.py
