import os
import argparse
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent


DEVICE = os.getenv("DEVICE", "cpu")


WEIGHTS_DIR = BASE_DIR / "weights"

