#!/usr/bin/env python3
"""
push_and_wait.py (fixed)

- 이전 실행의 responses를 섞어 읽는 문제 수정:
  -> 대기 시작 전에 model:responses의 마지막 msg_id를 구해서
     그 다음 메시지들만 읽도록 함.
"""

import redis
import uuid
import time
import argparse
from redis_load_balancer.redis_config import (
    REDIS_HOST_OCR, 
    REDIS_PORT_OCR, 
    REIDS_STREAM_REQ_OCR, 
    REIDS_STREAM_RESP_OCR,
    REDIS_GROUP_OCR,
)
from concurrent.futures import ThreadPoolExecutor, as_completed

# 설정
REDIS_HOST = REDIS_HOST_OCR
REDIS_PORT = REDIS_PORT_OCR
STREAM_REQ = REIDS_STREAM_REQ_OCR
STREAM_RESP = REIDS_STREAM_RESP_OCR

# Redis 연결 (binary mode)
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=False)


def get_last_resp_id():
    """
    model:responses에 이미 있는 마지막 메시지 ID 반환.
    없으면 "0-0" 반환.
    """
    try:
        last = r.xrevrange(STREAM_RESP, count=1)  # [(id, fields), ...]
        if not last:
            return "0-0"
        last_id = last[0][0]
        # ensure type is str for xread usage
        if isinstance(last_id, bytes):
            last_id = last_id.decode()
        return last_id
    except Exception as e:
        print("[get_last_resp_id] error:", e)
        return "0-0"

def send_one(payload: bytes) -> str:
    job_id = str(uuid.uuid4())
    fields = {
        b"job_id": job_id.encode(),
        b"payload": payload,
        b"timestamp": str(time.time()).encode(),
    }
    msg_id = r.xadd(STREAM_REQ, fields)
    print(f"[send] job_id={job_id} msg_id={msg_id}")
    return job_id

def push_many(payloads, max_workers=8):
    job_ids = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(send_one, p): idx for idx, p in enumerate(payloads)}
        for fut in as_completed(futures):
            jid = fut.result()
            job_ids.append(jid)
    return job_ids

def wait_for_results(expected_job_ids, timeout=60, start_after_id="0-0"):
    expected = set(expected_job_ids)
    seen = {}
    # we want to read only messages strictly after start_after_id
    last_id = start_after_id

    start = time.time()
    print(f"[wait] waiting for {len(expected)} results (timeout={timeout}s), reading after {start_after_id}")
    while time.time() - start < timeout and len(seen) < len(expected):
        try:
            out = r.xread({STREAM_RESP: last_id}, block=2000, count=50)
        except Exception as e:
            print("[wait] xread error:", e)
            time.sleep(0.5)
            continue

        if not out:
            continue

        for stream_name, messages in out:
            for msg_id, fields in messages:
                # update last_id so next read continues from here
                # xread expects id of the last read message; to avoid re-reading same message,
                # we set last_id to msg_id
                if isinstance(msg_id, bytes):
                    last_id = msg_id.decode()
                else:
                    last_id = msg_id

                try:
                    job_id_val = fields.get(b"job_id") or fields.get("job_id")
                    result = fields.get(b"result") or fields.get("result")
                    if job_id_val is None:
                        continue
                    job_id = job_id_val.decode() if isinstance(job_id_val, (bytes, bytearray)) else str(job_id_val)
                    seen[job_id] = result
                    print(f"[wait] got result for {job_id} (msg {last_id})")
                except Exception as e:
                    print("[wait] parse result error:", e)
    return seen

def main(num_jobs=20, concurrency=8, timeout=60, payload_template=b"payload-%d"):
    # find last response id BEFORE sending jobs
    start_after_id = get_last_resp_id()
    print(f"[main] start_after_id (responses) = {start_after_id}")

    payloads = [payload_template % i for i in range(num_jobs)]
    print(f"[main] pushing {num_jobs} jobs with concurrency={concurrency}")
    job_ids = push_many(payloads, max_workers=concurrency)
    print(f"[main] pushed {len(job_ids)} jobs. waiting for results...")

    results = wait_for_results(job_ids, timeout=timeout, start_after_id=start_after_id)
    got = len(results)
    print(f"[main] got {got}/{len(job_ids)} results")
    missing = set(job_ids) - set(results.keys())
    if missing:
        print("[main] missing job ids:", list(missing)[:20], "...")
    for i, jid in enumerate(job_ids):
        if jid in results:
            res = results[jid]
            out = res.decode() if isinstance(res, (bytes, bytearray)) else str(res)
            out_key = ["payload", "worker_id"]
            out = {k: v for k, v in out.items() if k in out_key}
            
            print(f"  {i+1}. {jid} -> {out}")
        else:
            print(f"  {i+1}. {jid} -> (no result)")
    return results

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Push multiple jobs to Redis Streams and wait for results (fixed)")
    parser.add_argument("--num", type=int, default=20)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--host", type=str, default=REDIS_HOST)
    parser.add_argument("--port", type=int, default=REDIS_PORT)
    args = parser.parse_args()

    r = redis.Redis(host=args.host, port=args.port, decode_responses=False)
    main(num_jobs=args.num, concurrency=args.concurrency, timeout=args.timeout)
