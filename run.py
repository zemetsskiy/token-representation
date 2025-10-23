import sys
import os
from pathlib import Path
project_root = Path(__file__).parent
src_path = project_root / 'src'
sys.path.insert(0, str(src_path))
from src.core.main import main
if __name__ == '__main__':
    main()