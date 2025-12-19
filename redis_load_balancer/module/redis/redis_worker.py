from email import message_from_string
import os
import redis
import socket
import uuid
import time
import traceback
import json
from abc import abstractmethod
from time import perf_counter
from redis_load_balancer.redis_config import REDIS_STREAM_MAX_LEN

class RedisWorker:
    def __init__(
        self,
        host:str,
        port:int,
        input_streams:list[dict],
        output_streams:list[dict],
        worker_id:str = None
    ):
        '''
        input_streams: list[dict] | output_streams: list[dict] =>
        [
            {
                "stream": str,
                "group": str,
                "message_batch_size": int,
                "claim_idle_time_ms": int,
                "claim_max_count": int,
                "stream_block_time_ms": int,
                "stream_error_claim": str,
            },
            ...
        ]
        '''
        if worker_id is None:
            if os.environ.get("WORKER_ID") is not None:
                worker_id = os.environ.get("WORKER_ID")
            else:
                worker_id = socket.gethostname()
        
        self.input_streams = input_streams
        self.output_streams = output_streams
        self.stream_id_map = {stream['stream']: stream for stream in input_streams + output_streams}
        
        self.worker_id = f"worker-[{','.join([i['stream'] for i in input_streams])}]-{worker_id}"
        self.r = redis.Redis(host=host, port=port, decode_responses=True)
        
        self.create_group(self.input_streams)
    
        
    def decode_resp(self, resps: list[dict])-> dict:
        result_resps = []
        for stream_id, msgs in resps:
            msg_resps = []
            for msg_id, fields in msgs:
                meta = json.loads(fields["meta"])
                payload = json.loads(fields["payload"])
                msg_resps.append((msg_id, {
                    "meta": meta,
                    "payload": payload}))
            result_resps.append((stream_id, msg_resps))
        return result_resps
    
    def encode_message(self, message: dict)-> dict:
        encoded = {k: json.dumps(v, ensure_ascii=False, default=str) for k, v in message.items()}
        return encoded
    
    @abstractmethod
    def set_processor(self, processor):
        pass
    
    @abstractmethod
    def process(self, resp: dict) -> list[dict]:
        '''
        return processed messages for output streams
        messages: [
            {
                "meta": list[dict] 
                [
                    {
                        "job_id": str,
                        "prod_id": str,
                        "timestamp": str,
                    },
                    ...
                ],
                "payload": dict,
            },
            ...
        ]
        '''
        messages=[]
        return messages
            
    def create_group(self, streams: list[dict]):
        for stream in streams:
            try:
                self.r.xgroup_create(stream['stream'], stream['group'], id='0-0', mkstream=True)
                print(f"[{self.worker_id}] created group '{stream['group']}'")
            except redis.exceptions.ResponseError as e:
                # group already exists
                if "BUSYGROUP" in str(e):
                    print(f"[{self.worker_id}] group already exists")
                else:
                    raise
                
    def read_messages(self, streams: list[dict]):
        for stream in streams:
            resp = self.r.xreadgroup(
                stream['group'], 
                self.worker_id, 
                {stream['stream']: ">"}, 
                block=stream['stream_block_time_ms'], count=stream['message_batch_size']
            )
            if resp:
                return self.decode_resp(resp)
        return []
            
    def commit_resp(self, resp: dict):
        for stream_id, msgs in resp:
            stream_group = self.stream_id_map[stream_id]['group']
            for msg_id, _ in msgs:
                self.r.xack(stream_id, stream_group, msg_id)

    def write_and_commit_pipeline(self, resp_for_ack: dict, out_streams: list[dict], out_messages: list[dict]):
        """
        XADD(결과 쓰기)와 XACK(원본 ACK)를 하나의 Redis 트랜잭션으로 묶어서 실행.
        
        message: {
            "meta": list[dict] 
            [
                {
                    "job_id": str,
                    "prod_id": str,
                    "timestamp": str,
                },
                ...
            ],
            "payload": dict,
        }
        """
        pipe = self.r.pipeline(transaction=True)
        for stream in out_streams:
            for message in out_messages:
                message["meta"].append({
                    "job_id": str(uuid.uuid4()),
                    "prod_id": self.worker_id,
                    "timestamp": str(time.time()),
                })
                pipe.xadd(stream['stream'], self.encode_message(message), maxlen=REDIS_STREAM_MAX_LEN, approximate=True)

        for stream_id, msgs in resp_for_ack:
            stream_group = self.stream_id_map[stream_id]['group']
            for msg_id, _ in msgs:
                pipe.xack(stream_id, stream_group, msg_id)

        pipe.execute()
    
    def claim_pending_if_any(self, min_idle_time_ms=100000, count=10):
        """
        처리 지연된 오래된 메시지를 claim.
        min_idle_time_ms: idle time 기준
        """
        for stream_request in self.input_streams:
            pending_info = self.r.xpending_range(stream_request['stream'], stream_request['group'], '-', '+', count, None)
            for info in pending_info:
                msg_id, last_worker_id, idle, delivered_count = info["message_id"], info["consumer"], info["time_since_delivered"], info["times_delivered"]
                if int(idle) >= min_idle_time_ms:
                    if  delivered_count >= stream_request['claim_max_count']:
                        print(f"[{self.worker_id}] max claim count reached for msg {msg_id} from {last_worker_id}")
                        
                        self.r.xadd(stream_request['stream_error_claim'], self.encode_message({
                            "stream_request": stream_request,
                            "msg_id": msg_id,
                            "last_worker_id": last_worker_id,
                            "idle": idle,
                            "delivered_count": delivered_count
                        }), maxlen=REDIS_STREAM_MAX_LEN, approximate=True)
                        # remove job
                        self.r.xdel(stream_request['stream'], msg_id)
                        print(f"[{self.worker_id}] removed job {msg_id} from {stream_request['stream']}")
                        continue
                    else:
                        self.r.xclaim(stream_request['stream'], stream_request['group'], self.worker_id, min_idle_time_ms, [msg_id])
                        print(f"[{self.worker_id}] claimed pending msg {msg_id} from {last_worker_id}")
                        
                    
    def listen(self):
        print(f"[{self.worker_id}] listening...")
        try:
            while True:
                resp = self.read_messages(self.input_streams)
                if len(resp) == 0:
                    self.claim_pending_if_any()
                else:
                    messages = self.process(resp)
                    self.write_and_commit_pipeline(resp, self.output_streams, messages) # XADD + XACK atomic process
                
        except Exception as e:
            print(f"[{self.worker_id}] error: {e}")
            traceback.print_exc()
            return 1
        return 0
            
    def write_new_messages(
        self, 
        streams: list[dict], 
        payloads: list[dict],
        verbose: bool = False
        )-> int : 
        '''
        payloads: list[dict]
        '''
        messages = []
        job_ids = []
        for payload in payloads:
            job_id = str(uuid.uuid4())
            meta = {
                "prod_id": self.worker_id,
                "job_id": job_id,
                "timestamp": str(time.time()),
            }
            message = {
                "meta": [meta],
                "payload":payload,
            }
            messages.append(message)
            job_ids.append(job_id)
            
        pipe = self.r.pipeline(transaction=True)
        for stream in streams:
            for message in messages:
                pipe.xadd(stream['stream'], self.encode_message(message), maxlen=REDIS_STREAM_MAX_LEN, approximate=True)
        pipe.execute()
        if verbose:
            print(f"[{self.worker_id}] wrote {len(messages)} messages to {[stream['stream'] for stream in streams]}")
            
        return job_ids

        
    def write_and_wait_for_response(
        self,
        payloads: list[dict],
        verbose: bool = False,
        max_len_fields:int = 100
    )-> list[dict]:
        '''
        payloads: list[dict]
        '''
        start_time = perf_counter()
        job_ids = self.write_new_messages(
            self.output_streams,
            payloads,
            verbose=verbose)
        message_send_time = perf_counter() - start_time
        if verbose:
            print(f"[{self.worker_id}] message send time: {message_send_time:.2f}s")
            
        start_time = perf_counter()
        # listen
        msg_recv_times = []
        while True:
            read_start_time = perf_counter()
            resp = self.read_messages(self.input_streams)
            if len(resp) == 0:
                continue
                
            for stream_id, msgs in resp:
                for msg_id, fields in msgs:
                    print(f"[{self.worker_id}] got response for msg_id:{msg_id} from {stream_id} - fields: {f'{fields}' if len(f'{fields}') < max_len_fields else f'{fields}'[:max_len_fields]}...")                
                    job_id = fields["meta"][0].get("job_id", None)
                    if job_id in job_ids:
                        job_ids.remove(job_id)
                        
            read_time = perf_counter() - read_start_time
            msg_recv_times.append(read_time)
            
            if len(job_ids) == 0:
                break
            
        print(f"[{self.worker_id}] all job_ids are processed: {len(payloads)}")
        print(f"[{self.worker_id}] message send time: {message_send_time:.2f}s")
        print(f"[{self.worker_id}] total message receive time: {sum(msg_recv_times):.2f}s")
        print(f"[{self.worker_id}] message receive time per message: {sum(msg_recv_times) / len(msg_recv_times):.2f}s")