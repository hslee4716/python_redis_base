import os, sys
import json
import socket

DEVICE = os.getenv("DEVICE", "cpu")
if DEVICE.isdigit():
    DEVICE = int(DEVICE)
if isinstance(DEVICE, str) and DEVICE.lower() in ["gpu", "cuda"]:
    import paddle
    paddle.set_device("gpu")
elif isinstance(DEVICE, str) and DEVICE.lower() in ["cpu"]:
    os.environ["CUDA_VISIBLE_DEVICES"] = ""
    import paddle
    paddle.set_device("cpu")
elif isinstance(DEVICE, int):
    os.environ["CUDA_VISIBLE_DEVICES"] = str(DEVICE)
    import paddle
    paddle.set_device(f"gpu")
else:
    raise ValueError(f"Invalid device: {DEVICE}")

from redis_load_balancer.module.models.ocr import OCRProcessor
from redis_load_balancer.redis_config import (
    REDIS_HOST_OCR,
    REDIS_PORT_OCR,
    REIDS_STREAM_REQ_OCR,
    REIDS_STREAM_RESP_OCR,
    REDIS_GROUP_OCR,
)


from redis_load_balancer.module.redis.redis_worker import RedisWorker

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from worker_info import OCR_input_streams, OCR_result_streams


class RedisPaddleWorker(RedisWorker):
    def __init__(self,
                 host:str,
                 port:int,
                 input_streams:list[dict],
                 output_streams:list[dict],
                 worker_id:str = None
                 ):
        super().__init__(host, port, input_streams, output_streams, worker_id)
        self.set_processor()
        
    def set_processor(self):
        self.processor = OCRProcessor()
    
    def process(self, resp: bytes) -> dict:
        '''
        resp:{
            "stream": str,
            "messages": list[dict],
        }
        
        payload : {
            "image_path": "path/to/image.jpg",
            "page_number": int,
            "total_pages": int,
        }
        '''
        payloads = []
        messages = []
        for stream_id, msgs in resp:
            for msg_id, fields in msgs:
                payload = json.loads(fields[b"payload"])
                messages.append({
                    "stream_id": stream_id.decode(),
                    "msg_id": msg_id.decode(),
                    "job_id": fields[b"job_id"].decode(),
                    "payload": payload,
                    "timestamp": fields[b"timestamp"].decode(),
                })
                payloads.append(payload)
        
        image_paths = [message["payload"]["image_path"] for message in messages]
        # test
        from pathlib import Path
        image_paths = [f"/home/hslee/workdir/test/redis_test/sample_image/{Path(p).name}" for p in image_paths]
        # test
        
        results = self.processor.ocr(image_paths)
        for message, result in zip(messages, results):
            message["payload"]["result"] = result
        return messages

CONSUMER_NAME = os.getenv("CONSUMER_NAME", socket.gethostname())
STREAM_REQUEST = REIDS_STREAM_REQ_OCR
STREAM_RESPONSE = REIDS_STREAM_RESP_OCR
GROUP = REDIS_GROUP_OCR
WORKER_ID = os.getenv("WORKER_ID", socket.gethostname())

def main():
    worker = RedisPaddleWorker(
        host=REDIS_HOST_OCR,
        port=REDIS_PORT_OCR,
        input_streams=OCR_input_streams,
        output_streams=OCR_result_streams,
        worker_id=WORKER_ID
    )
    worker.listen()

if __name__ == "__main__":
    main()
