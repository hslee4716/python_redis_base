import os

# 설정
REDIS_HOST_OCR = os.getenv("REDIS_HOST_OCR", "localhost")
REDIS_PORT_OCR = os.getenv("REDIS_PORT_OCR", 6379)
REDIS_GROUP_OCR = "OCR_consumers"

REIDS_STREAM_REQ_OCR = "OCR:requests"
REIDS_STREAM_RESP_OCR = "OCR:responses"