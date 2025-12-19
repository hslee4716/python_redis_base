import os

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

import redis
import json
import socket
from concurrent.futures import ThreadPoolExecutor, as_completed

def process(payload_bytes: bytes) -> dict:
    '''
    payload : {
        "image_path": "path/to/image.jpg",
        "page_number": int,
        "total_pages": int,
    }
    '''
    payload = json.loads(payload_bytes)
    image_path = payload["image_path"]
    
    result = processor.ocr(image_path)
    
    
    return {"payload": payload, "result": result}

def claim_pending_if_any(min_idle_time_ms=100000, count=10):
    """
    처리 지연된 오래된 메시지를 claim.
    min_idle_time_ms: idle time 기준
    """
    pending_info = r.xpending_range(STREAM_REQUEST, GROUP, '-', '+', count, None)
    # pending_info: [(id, consumer, idle, delivered_count), ...]
    print("@@@@@@@@@@@",pending_info)
    for info in pending_info:
        '''
        info : {'message_id': b'1765947573618-1', 'consumer': b'f13bc2c7cbb2', 'time_since_delivered': 64401, 'times_delivered': 1}
        '''
        msg_id, consumer, idle, delivered_count = info["message_id"], info["consumer"], info["time_since_delivered"], info["times_delivered"]
        if int(idle) >= min_idle_time_ms:
            r.xclaim(STREAM_REQUEST, GROUP, CONSUMER_NAME, min_idle_time_ms, [msg_id])
            print(f"[{CONSUMER_NAME}] claimed pending msg {msg_id} from {consumer}")

CONSUMER_NAME = os.getenv("CONSUMER_NAME", socket.gethostname())
STREAM_REQUEST = REIDS_STREAM_REQ_OCR
STREAM_RESPONSE = REIDS_STREAM_RESP_OCR
GROUP = REDIS_GROUP_OCR
WORKER_ID = os.getenv("WORKER_ID", socket.gethostname())

processor = OCRProcessor()

r = redis.Redis(host=REDIS_HOST_OCR, port=REDIS_PORT_OCR, decode_responses=False)
print(f"@@@@@@@@@@@[{CONSUMER_NAME}] worker_id = {WORKER_ID} up!@@@@@@@@@@@ device = {paddle.device.get_device()}")

try:
    r.xgroup_create(STREAM_REQUEST, GROUP, id='0-0', mkstream=True)
    print(f"[{CONSUMER_NAME}] created group '{GROUP}'")
except redis.exceptions.ResponseError as e:
    # group already exists
    if "BUSYGROUP" in str(e):
        print(f"[{CONSUMER_NAME}] group already exists")
    else:
        raise


while True:
    # read new messages for this consumer group ('>' means new msgs not delivered to any consumer)
    resp = r.xreadgroup(GROUP, CONSUMER_NAME, {STREAM_REQUEST: ">"}, block=5000, count=1)
    if len(resp) == 0:
        claim_pending_if_any()
        continue

    for stream, messages in resp:
        for msg_id, fields in messages:
            print("@@@@@ for ack ::", f"stream: {stream}, msg_id: {msg_id}, fields: {fields}")
            try:
                job_id = fields[b"job_id"].decode()
                payload = fields[b"payload"]  # bytes
                result = process(payload)
                result.update({"worker_id": WORKER_ID})
                r.xadd(STREAM_RESPONSE, {"job_id": fields[b"job_id"], "result": json.dumps(result, ensure_ascii=False).encode()})
                r.xack(STREAM_REQUEST, GROUP, msg_id)
                # print(f"[{CONSUMER_NAME}] finished job {job_id}, acked and wrote result")
            except Exception as e:
                # print(f"[{CONSUMER_NAME}] error processing msg {msg_id}: {e}")
                pass