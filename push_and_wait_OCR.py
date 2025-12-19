#!/usr/bin/env python3
"""
push_and_wait.py (fixed)

- 이전 실행의 responses를 섞어 읽는 문제 수정:
  -> 대기 시작 전에 model:responses의 마지막 msg_id를 구해서
     그 다음 메시지들만 읽도록 함.
"""
from time import perf_counter
import json
import redis
import uuid
import time
import argparse
from redis_load_balancer.redis_config import (
    REDIS_HOST_OCR, 
    REDIS_PORT_OCR, 
    REIDS_STREAM_REQ_OCR, 
    REIDS_STREAM_RESP_OCR,
)
from concurrent.futures import ThreadPoolExecutor, as_completed

# 설정
STREAM_REQ = REIDS_STREAM_REQ_OCR
STREAM_RESP = REIDS_STREAM_RESP_OCR

# Redis 연결 (문자열 모드 - UTF-8 디코딩 자동)
r = redis.Redis(host=REDIS_HOST_OCR, port=REDIS_PORT_OCR, decode_responses=True)


def get_last_resp_id():
    """
    model:responses에 이미 있는 마지막 메시지 ID 반환.
    없으면 "0-0" 반환.
    """
    try:
        last = r.xrevrange(STREAM_RESP, count=1)  # [(id, fields), ...]
        if not last:
            return "0-0"
        # decode_responses=True 이므로 msg_id 는 이미 str
        last_id = last[0][0]
        return last_id
    except Exception as e:
        print("[get_last_resp_id] error:", e)
        return "0-0"

def send_one(payload: str) -> str:
    job_id = str(uuid.uuid4())
    # decode_responses=True 이므로 key/value 를 문자열로 넘기면
    # 내부에서 UTF-8로 인코딩되어 저장된다.
    fields = {
        "job_id": job_id,
        "payload": payload,
        "timestamp": str(time.time()),
    }
    msg_id = r.xadd(REIDS_STREAM_REQ_OCR, fields)
    print(f"[send] job_id={job_id} msg_id={msg_id}")
    return job_id

def push_many(payloads, max_workers=8):
    job_ids = []
    # JSON 문자열로만 만들고, 따로 encode()는 하지 않는다.
    payloads = [json.dumps(p) for p in payloads]
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
    start_time = perf_counter()
    start = time.time()
    print(f"[wait] waiting for {len(expected)} results (timeout={timeout}s), reading after {start_after_id}")
    while time.time() - start < timeout and len(seen) < len(expected):
        try:
            out = r.xread({REIDS_STREAM_RESP_OCR: last_id}, block=2000, count=50)
        except Exception as e:
            print("[wait] xread error:", e)
            time.sleep(0.5)
            continue

        if not out:
            continue

        for stream_name, messages in out:
            for msg_id, fields in messages:
                # decode_responses=True 이므로 msg_id 는 이미 str
                # 다음 읽기는 이 msg_id 이후부터 시작
                last_id = msg_id

                try:
                    # decode_responses=True 이므로 key/value 모두 str
                    job_id = fields.get("job_id")
                    result = fields.get("payload")
                    if job_id is None:
                        continue
                    seen[job_id] = result
                    print(f"[wait] got result for {job_id} (msg {last_id})")
                except Exception as e:
                    print("[wait] parse result error:", e)
        elapsed = perf_counter() - start_time
        print(f"[wait] elapsed time: {elapsed:.2f}s")
    return seen

def main(concurrency=8, timeout=600):
    # find last response id BEFORE sending jobs
    start_after_id = get_last_resp_id()
    print(f"[main] start_after_id (responses) = {start_after_id}")

    # payloads = [payload_template % i for i in range(num_jobs)]
    from glob import glob
    from pathlib import Path
    payloads = glob("/home/hslee/workdir/test/redis_test/sample_image/*.png")
    payloads = [{'image_path': f"/app/sample_image/{Path(p).name}", "page_number": 1, "total_pages": 1} for p in payloads]
    
    payloads = payloads * 10
    job_ids = push_many(payloads, max_workers=concurrency)

    results = wait_for_results(job_ids, timeout=timeout, start_after_id=start_after_id)
    got = len(results)
    print(f"[main] got {got}/{len(job_ids)} results")
    missing = set(job_ids) - set(results.keys())
    if missing:
        print("[main] missing job ids:", list(missing)[:20], "...")
    for i, jid in enumerate(job_ids):
        if jid in results:
            res = results[jid]
            # 결과는 JSON 문자열이라고 가정
            try:
                out = json.loads(res)
            except Exception:
                out = res
            # out_key = ["payload", "worker_id"]
            # out = {k: v for k, v in out.items() if k in out_key}
            print(f"  {i+1}. {jid} -> {out}")
        else:
            print(f"  {i+1}. {jid} -> (no result)")
    return results

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Push multiple jobs to Redis Streams and wait for results (fixed)")
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument("--host", type=str, default=REDIS_HOST_OCR)
    parser.add_argument("--port", type=int, default=REDIS_PORT_OCR)
    args = parser.parse_args()

    # main 에서 사용하는 전역 r 을 재설정 (문자열 모드)
    r = redis.Redis(host=args.host, port=args.port, decode_responses=True)
    main(concurrency=args.concurrency, timeout=args.timeout)
