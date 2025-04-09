#!/bin/bash

echo "Searching and deleting _pychache_ (not including venv folder)"

find . -type d -name "_pycache_" ! -path "/venv/" -exec rm -rf {} +

echo "Delete success."