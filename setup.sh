#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$SCRIPT_DIR/venv"

# Vérifier Python
echo "[1/4] Vérification de Python..."
if command -v python3 &> /dev/null; then
    PYTHON_CMD="python3"
elif command -v python &> /dev/null; then
    PYTHON_CMD="python"
else
    echo "ERREUR: Python n'est pas installé."
    echo "Installez Python 3.9+ depuis https://www.python.org/downloads/"
    exit 1
fi

PYTHON_VERSION=$($PYTHON_CMD -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
echo "      Python $PYTHON_VERSION détecté"

# Vérifier version minimale (3.9)
MIN_VERSION="3.9"
if [ "$(printf '%s\n' "$MIN_VERSION" "$PYTHON_VERSION" | sort -V | head -n1)" != "$MIN_VERSION" ]; then
    echo "ERREUR: Python 3.9+ requis (version actuelle: $PYTHON_VERSION)"
    exit 1
fi

# Créer l'environnement virtuel
echo ""
echo "[2/4] Création de l'environnement virtuel..."
if [ -d "$VENV_DIR" ]; then
    echo "      Environnement existant détecté, suppression..."
    rm -rf "$VENV_DIR"
fi

$PYTHON_CMD -m venv "$VENV_DIR"
echo "      Environnement créé dans: $VENV_DIR"

# Activer et installer
echo ""
echo "[3/4] Installation des dépendances..."
source "$VENV_DIR/bin/activate"

# Mise à jour pip
pip install --upgrade pip --quiet

# Installation des dépendances
pip install -r "$SCRIPT_DIR/requirements.txt" --quiet

echo "      Dépendances installées avec succès"

# Créer le script de lancement
echo ""
echo "[4/4] Création du script de lancement..."

cat > "$SCRIPT_DIR/run.sh" << 'LAUNCHER'
#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/venv/bin/activate"
python "$SCRIPT_DIR/scraper.py" "$@"
LAUNCHER

chmod +x "$SCRIPT_DIR/run.sh"

echo ""
echo "============================================================"
echo "  INSTALLATION TERMINÉE"
echo "============================================================"
echo ""
echo "  Pour lancer le scraper:"
echo ""
echo "    ./run.sh                     # Mode rapide"
echo "    ./run.sh --detailed          # Mode détaillé (plus précis)"
echo "    ./run.sh --limit 50          # Test avec 50 résultats"
echo "    ./run.sh --help              # Aide complète"
echo ""
echo "  Les résultats seront dans le dossier 'output/'"
echo ""
echo "============================================================"
