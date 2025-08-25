import sys
from pathlib import Path

def setup_path():
    project_root = Path(__file__).resolve().parents[0]
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))