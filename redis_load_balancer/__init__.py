import os
import argparse
from pathlib import Path
from .module.utils.timer import Timer

BASE_DIR = Path(__file__).parent.parent
DEVICE = os.getenv("DEVICE", "cpu")

if isinstance(DEVICE, str):
    if DEVICE.isdigit():
        DEVICE = int(DEVICE)

    elif DEVICE.lower() in ["gpu", "cuda"]:
        import paddle
        paddle.set_device("gpu")
        
    elif DEVICE.lower() in ["cpu"]:
        os.environ["CUDA_VISIBLE_DEVICES"] = ""
        import paddle
        paddle.set_device("cpu")
        
    elif len(DEVICE.split(",")) > 1:
        os.environ["CUDA_VISIBLE_DEVICES"] = DEVICE
        import paddle
        paddle.set_device("gpu")
        
if isinstance(DEVICE, int):
    os.environ["CUDA_VISIBLE_DEVICES"] = str(DEVICE)
    import paddle
    paddle.set_device(f"gpu")

WEIGHTS_DIR = BASE_DIR / "weights"

CACHE_FOLDER = BASE_DIR / "cache"