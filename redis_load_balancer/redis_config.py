import os

# 설정
REDIS_HOST_OCR = os.getenv("REDIS_HOST_OCR", "localhost")
REDIS_PORT_OCR = os.getenv("REDIS_PORT_OCR", 6379)
REDIS_GROUP_OCR = "OCR_consumers"

REIDS_STREAM_REQ_OCR = "OCR:requests"
REIDS_STREAM_RESP_OCR = "OCR:responses"

REDIS_STREAM_MAX_LEN = os.getenv("REDIS_STREAM_MAX_LEN", 3000)

message_fields = {
    "stream_log": list[str], # [stream_name]
    "job_id": str,
    "payload": dict, # data or result
    "timestamp": list[str], # [request_timestamp, response_timestamp]
}
