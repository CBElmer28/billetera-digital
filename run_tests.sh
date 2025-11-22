#!/usr/bin/env bash
# run_tests.sh
# Este script ejecuta la suite completa de pruebas automatizadas usando pytest.

echo "Ejecutando la suite de pruebas de Pixel Money..."

# Activa la detención del script si cualquier comando falla
set -e

# Ejecuta pytest
# -v (verbose): Muestra el nombre de cada prueba que se ejecuta (más útil que -q)
# tests/: La carpeta donde se encuentran todas nuestras pruebas
pytest -v tests/

echo "¡Todas las pruebas pasaron exitosamente!"