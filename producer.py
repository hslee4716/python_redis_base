# import time
# import uuid
# import redis
# import os

# r = redis.Redis(host="redis", port=6379, decode_responses=False)

# STREAM_REQUEST = "model:requests"
# STREAM_RESPONSE = "model:responses"

# def send_job(job_id: str, payload: bytes):
#     # store job_id and raw payload (bytes) as fields
#     fields = {
#         "job_id": job_id.encode(),
#         "payload": payload,
#         "timestamp": str(time.time()).encode(),
#     }
#     msg_id = r.xadd(STREAM_REQUEST, fields)
#     print(f"[producer] sent job {job_id} -> msg_id {msg_id}")
#     return msg_id

# def read_results(last_ids):
#     # last_ids: dict stream->last_id
#     # block for up to 5s
#     res = r.xread({STREAM_RESPONSE: last_ids.get(STREAM_RESPONSE, "0-0")}, block=5000, count=10)
#     return res
