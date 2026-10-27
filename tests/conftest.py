"""tests/conftest.py — Configuración compartida de pytest."""
import sys
from pathlib import Path

# Añadir el directorio raíz al path para importaciones
sys.path.insert(0, str(Path(__file__).parent.parent))
