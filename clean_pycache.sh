#!/bin/bash

# Script para eliminar todos los directorios __pycache__ y archivos .pyc
# desde el directorio actual y sus subdirectorios.

# Navegar al directorio raíz del proyecto (donde se ejecuta este script)
# Esto es opcional si siempre ejecutas el script desde la raíz.
# SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
# cd "$SCRIPT_DIR"

echo "Buscando y eliminando directorios __pycache__..."
find . -type d -name "__pycache__" -print0 | while IFS= read -r -d $'\0' dir; do
    echo "Eliminando: $dir"
    rm -rf "$dir"
done

echo ""
echo "Buscando y eliminando archivos .pyc fuera de __pycache__ (si existen)..."
find . -type f -name "*.pyc" -print0 | while IFS= read -r -d $'\0' file; do
    echo "Eliminando: $file"
    rm -f "$file"
done

echo ""
echo "Limpieza de __pycache__ y .pyc completada."

# Adicional: Si tienes archivos .py antiguos en airflow/dags/ que ya no usas
# (como task_etl.py o dag_nuevo.py si dag.py es el único activo),
# deberías eliminarlos manualmente. Este script no borra archivos .py.
# Ejemplo (¡CUIDADO! Descomenta y ajusta solo si estás seguro):
# echo ""
# echo "Recordatorio: Elimina manualmente archivos .py obsoletos de airflow/dags/ si es necesario."
# echo "Por ejemplo:"
# echo "rm -f airflow/dags/task_etl.py"
# echo "rm -f airflow/dags/dag_nuevo.py"