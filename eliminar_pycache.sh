#!/bin/bash

echo "Buscando y eliminando carpetas _pycache_ (excepto las de venv)..."

find . -type d -name "_pycache_" ! -path "/venv/" -exec rm -rf {} +

echo "Eliminación completada."