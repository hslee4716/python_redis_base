import os, sys
import json
import socket
import asyncio
from time import perf_counter
from pathlib import Path
from redis_load_balancer import BASE_DIR, CACHE_FOLDER, Timer
from redis_load_balancer.redis_config import (
    REDIS_HOST_OCR,
    REDIS_PORT_OCR,
    REIDS_STREAM_REQ_OCR,
    REIDS_STREAM_RESP_OCR,
    REDIS_GROUP_OCR,
)
from redis_load_balancer.module.redis.redis_worker import RedisWorker
from redis_load_balancer.module.utils.files import save_pdf_to_images

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from worker_info import OCR_input_streams, OCR_result_streams
from glob import glob

CONSUMER_NAME = os.getenv("CONSUMER_NAME", socket.gethostname())
STREAM_REQUEST = REIDS_STREAM_REQ_OCR
STREAM_RESPONSE = REIDS_STREAM_RESP_OCR
GROUP = REDIS_GROUP_OCR
WORKER_ID = os.getenv("WORKER_ID", socket.gethostname())



def make_job_from_pdf(pdf_path):
    cache_path = CACHE_FOLDER / Path(pdf_path).stem
    if not cache_path.exists():
        os.makedirs(cache_path, exist_ok=True)
    
    image_sav_pths = save_pdf_to_images(pdf_path, cache_path)
        
    return image_sav_pths

def main():
    pdf_path = BASE_DIR / "sample_image" / "guide2.pdf"
    start = perf_counter()
    image_save_paths = make_job_from_pdf(pdf_path)
    end = perf_counter()
    print(f"image save time: {end - start:.2f}s")
    
    worker = RedisWorker(
        host=REDIS_HOST_OCR,
        port=REDIS_PORT_OCR,
        input_streams=OCR_result_streams,
        output_streams=OCR_input_streams,
        worker_id = "PROD"
    )
    
    payloads = [
        {
            "image_path": image,
            "page_number": 1,
            "total_pages": 1,
        }
        for image in image_save_paths
    ]
    
    job_ids = worker.write_and_wait_for_response(payloads, verbose=True)
    end2 = perf_counter()
    print(f"pdf to image save time: {end - start:.2f}s | total {len(image_save_paths)} images")
    print(f"message send and receive time: {end2 - end:.2f}s")

if __name__ == "__main__":
    main()
