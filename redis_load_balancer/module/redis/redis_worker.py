from abc import abstractmethod
import os
import redis
import json
import socket
import uuid
import time

class RedisWorker:
    def __init__(self,
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
        
        '''
        if worker_id is None:
            if os.environ.get("WORKER_ID") is not None:
                worker_id = os.environ.get("WORKER_ID")
            else:
                worker_id = socket.gethostname()
        
        self.input_streams = input_streams
        self.output_streams = output_streams
        self.stream_id_map = {stream['stream']: stream for stream in input_streams + output_streams}
        
        self.worker_id = f"worker-[{",".join([i['stream'] for i in input_streams])}]-{worker_id}"
        self.r = redis.Redis(host=host, port=port, decode_responses=False)
        
        self.create_group(self.input_streams)
    
    @abstractmethod
    def set_processor(self, processor):
        pass
    
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
                return resp
        return []
            
    def commit_resp(self, resp: dict):
        for stream_id, msgs in resp:
            stream_group = self.stream_id_map[stream_id]['group']
            for msg_id, _ in msgs:
                self.r.xack(stream_id, stream_group, msg_id)

    def write_and_commit_atomic(self, resp_for_ack: dict, out_streams: list[dict], out_payloads: list[dict]):
        """
        XADD(결과 쓰기)와 XACK(원본 ACK)를 하나의 Redis 트랜잭션으로 묶어서 실행.
        """
        pipe = self.r.pipeline(transaction=True)

        # 1) 결과 메시지 쓰기
        for stream in out_streams:
            for payload in out_payloads:
                job_id = str(uuid.uuid4())
                fields = {
                    b"job_id": job_id.encode(),
                    b"payload": payload,
                    b"timestamp": str(time.time()).encode(),
                }
                pipe.xadd(stream['stream'], fields)

        # 2) 원본 메시지 ACK
        for stream_id, msgs in resp_for_ack:
            stream_group = self.stream_id_map[stream_id]['group']
            for msg_id, _ in msgs:
                pipe.xack(stream_id, stream_group, msg_id)

        # 3) 모두 한 번에 실행
        pipe.execute()
    
    def write_messages(self, streams, messages):
        for stream in streams:
            for message in messages:
                self.r.xadd(stream['stream'], message)
    
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
                        
                        self.r.xadd(stream_request['stream_error_claim'], {
                            "stream_request": stream_request,
                            "msg_id": msg_id,
                            "last_worker_id": last_worker_id,
                            "idle": idle,
                            "delivered_count": delivered_count
                        })
                        # remove job
                        self.r.xdel(stream_request['stream'], msg_id)
                        print(f"[{self.worker_id}] removed job {msg_id} from {stream_request['stream']}")
                        continue
                    else:
                        self.r.xclaim(stream_request['stream'], stream_request['group'], self.worker_id, min_idle_time_ms, [msg_id])
                        print(f"[{self.worker_id}] claimed pending msg {msg_id} from {last_worker_id}")
                        break
                    
    @abstractmethod
    def process(self, payload_bytes: bytes) -> dict:
        '''
        payload : {
            "image_path": "path/to/image.jpg",
            "page_number": int,
            "total_pages": int,
        }
        '''
        payload = json.loads(payload_bytes)  
        # 처리 로직 구현
        return {"payload": payload, "result": None}
    
    def listen(self):
        try:
            while True:
                resp = self.read_messages(self.input_streams)
                if len(resp) == 0:
                    self.claim_pending_if_any()
                    continue
                
                result = self.process(resp)

                messages = [{"result": result}]

                # XADD + XACK를 하나의 트랜잭션으로 처리
                self.write_and_commit_atomic(resp, self.output_streams, messages)
                
                
        except Exception as e:
            print(f"[{self.worker_id}] error: {e}")
            return 1
        return 0
            