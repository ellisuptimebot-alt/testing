#!/bin/bash
apt-get update -qq && apt-get install -y -qq ffmpeg 2>/dev/null
pip install -r requirements.txt
