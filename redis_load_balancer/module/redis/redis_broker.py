import json
import time
import uuid
import threading

from abc import abstractmethod
from typing import Dict, List, Optional

from .redis_worker import RedisWorker

class RedisBroker(RedisWorker):
    def __init__(
        self,
        host: str,
        port: int,
        prod_streams: list[dict],
        recv_streams: list[dict],
        worker_id: str = None,
    ):
        super().__init__(host, port, recv_streams, prod_streams, worker_id)
        
        # 요청 단위 상태 관리 (여러 덩어리 동시 처리용)
        # request_id -> {
        #   "total_parts": int,
        #   "done_count": int,
        #   "results": dict[job_id, result_dict],
        #   "job_ids": list[str],
        #   "created_at": float,
        #   "done": bool,
        #   "request_payloads": list[dict],
        # }
        self._requests: Dict[str, dict] = {}
        self._requests_lock = threading.Lock()
        self._requests_cond = threading.Condition(self._requests_lock)
        
        # response stream 을 읽는 전용 루프 관리
        self._listener_thread: Optional[threading.Thread] = None
        self._listener_running: bool = False
        self._resp_last_ids: Dict[str, str] = {
            s["stream"]: "0-0" for s in recv_streams
        }
        
    @abstractmethod
    def data_to_payloads(self, data: dict) -> list[dict]:
        pass
    
    @abstractmethod
    def resp_to_data(self, resp: dict) -> dict:
        pass
    
    def get_last_response_ids(self) -> Dict[str, str]:
        """
        현재 브로커가 읽어야 할 response stream 들에 대해
        각 스트림의 마지막 메시지 ID 를 반환.
        메시지가 없으면 "0-0" 을 반환.
        """
        last_ids: Dict[str, str] = {}
        for stream in self.input_streams:
            stream_name = stream["stream"]
            try:
                last = self.r.xrevrange(stream_name, count=1)
                if not last:
                    last_ids[stream_name] = "0-0"
                else:
                    # decode_responses=True 이므로 msg_id 는 str
                    last_ids[stream_name] = last[0][0]
            except Exception:
                # 문제가 생겨도 브로커 동작은 계속하기 위해 기본값 사용
                last_ids[stream_name] = "0-0"
        return last_ids
    
    # -----------------------------
    # 공통: worker 로 job 보내기
    # -----------------------------
    
    async def produce(self, payloads: List[dict]) -> List[str]:
        """
        payload 리스트를 Redis Stream 에 job 으로 쓰고,
        생성된 job_id 리스트를 반환한다.

        - data_to_payloads 에서 만든 payload 를 그대로 사용한다.
        - 각 payload 마다 UUID 기반 job_id 를 부여한다.
        """
        messages: List[dict] = []
        job_ids: List[str] = []
        for payload in payloads:
            job_id = str(uuid.uuid4())
            fields = {
                "job_id": job_id,
                "payload": json.dumps(payload, ensure_ascii=False),
                "timestamp": str(time.time()),
            }
            messages.append(fields)
            job_ids.append(job_id)

        # RedisWorker 가 제공하는 공통 메서드 사용
        self.write_messages(self.output_streams, messages)
        return job_ids
    
    # -----------------------------
    # 공통: response stream 백그라운드 리스너
    # -----------------------------
    def start_response_listener(self):
        """
        response stream(xread) 를 계속 읽어서
        요청 단위(request_id)로 결과를 모으는 백그라운드 스레드 시작.
        여러 번 호출해도 한 번만 실행된다.
        """
        if self._listener_thread is not None and self._listener_thread.is_alive():
            return
        
        self._listener_running = True
        self._listener_thread = threading.Thread(
            target=self._response_loop,
            name=f"{self.worker_id}-resp-listener",
            daemon=True,
        )
        self._listener_thread.start()
    
    def stop_response_listener(self):
        """
        백그라운드 리스너를 중지한다. (필요 시)
        """
        self._listener_running = False
    
    def _response_loop(self):
        """
        response stream 들을 계속 xread 하면서
        payload 안의 request_id 를 기준으로 요청 단위 결과를 모은다.
        """
        while self._listener_running:
            streams = {
                s["stream"]: self._resp_last_ids.get(s["stream"], "0-0")
                for s in self.input_streams
            }
            
            try:
                out = self.r.xread(streams, block=2000, count=100)
            except Exception:
                # 일시적인 에러는 무시하고 재시도
                time.sleep(0.5)
                continue
            
            if not out:
                continue
            
            for stream_name, messages in out:
                for msg_id, fields in messages:
                    self._resp_last_ids[stream_name] = msg_id
                    
                    job_id = fields.get("job_id")
                    raw_payload = fields.get("payload")
                    try:
                        payload = (
                            json.loads(raw_payload)
                            if isinstance(raw_payload, str)
                            else raw_payload
                        )
                    except Exception:
                        payload = raw_payload
                    
                    if not isinstance(payload, dict):
                        continue
                    
                    request_id = payload.get("request_id")
                    if request_id is None:
                        # 브로커가 관리하지 않는 단일 job 이거나,
                        # 옛 포맷일 수 있으므로 무시
                        continue
                    
                    with self._requests_lock:
                        state = self._requests.get(request_id)
                        if state is None:
                            # 이미 timeout 등으로 정리된 요청일 수 있음
                            continue
                        
                        results = state["results"]
                        results[job_id] = {
                            "stream": stream_name,
                            "msg_id": msg_id,
                            "job_id": job_id,
                            "payload": payload,
                            "timestamp": fields.get("timestamp"),
                        }
                        state["done_count"] += 1
                        
                        total_parts = state["total_parts"]
                        if state["done_count"] >= total_parts:
                            state["done"] = True
                            # 이 요청을 기다리는 쓰레드를 깨운다.
                            self._requests_cond.notify_all()
    
    async def gather_jobs(
        self,
        job_ids: List[str],
        timeout: int = 60,
        start_after_ids: Optional[Dict[str, str]] = None,
    ) -> Dict[str, dict]:
        """
        공통 job 단위 gather 로직.
        
        - job_ids: produce 에서 생성된 job_id 리스트
        - timeout: 최대 대기 시간(초)
        - start_after_ids: 각 stream 별로 어느 ID 이후부터 읽을지 지정
        
        결과는 job_id 를 key 로 하는 dict 를 반환한다.
        resp_to_data 에서 이 dict 를 받아서 최종 응답 포맷으로 가공하면 된다.
        """
        expected = set(job_ids)
        seen: Dict[str, dict] = {}
        
        if start_after_ids is None:
            start_after_ids = {s["stream"]: "0-0" for s in self.input_streams}
        
        last_ids: Dict[str, str] = dict(start_after_ids)
        start_time = time.time()
        
        while len(seen) < len(expected) and (time.time() - start_time) < timeout:
            # 각 스트림별로 어디서부터 읽을지 구성
            streams = {
                s["stream"]: last_ids.get(s["stream"], "0-0")
                for s in self.input_streams
            }
            
            try:
                # block 은 ms 단위, 너무 길게 잡지 않고 짧게 여러 번 폴링
                out = self.r.xread(streams, block=2000, count=100)
            except Exception:
                # 일시적인 에러는 무시하고 재시도
                time.sleep(0.5)
                continue
            
            if not out:
                continue
            
            for stream_name, messages in out:
                for msg_id, fields in messages:
                    last_ids[stream_name] = msg_id
                    
                    job_id = fields.get("job_id")
                    if job_id is None or job_id not in expected:
                        continue
                    
                    raw_payload = fields.get("payload")
                    try:
                        payload = (
                            json.loads(raw_payload)
                            if isinstance(raw_payload, str)
                            else raw_payload
                        )
                    except Exception:
                        payload = raw_payload
                    
                    seen[job_id] = {
                        "stream": stream_name,
                        "msg_id": msg_id,
                        "job_id": job_id,
                        "payload": payload,
                        "timestamp": fields.get("timestamp"),
                    }
        
        return seen
    
    async def request_and_wait(self, data: dict, timeout: int = 60):
        """
        여러 덩어리의 요청이 동시에 들어올 수 있는 상황에서 사용하는
        request 단위 API.
        
        워크플로우:
        1) data 를 data_to_payloads 로 job 단위로 쪼갠다.
        2) 각 payload 에 request_id / part_index / total_parts 메타를 붙인다.
        3) 백그라운드 response listener 가 돌아가는지 확인 후, worker 로 XADD.
        4) response listener 가 같은 request_id 에 해당하는 결과를 모두 모을 때까지 대기.
        5) 모인 결과를 resp_to_data 로 가공하여 반환.
        
        - 여러 요청이 동시에 들어와도, 먼저 끝난 요청부터 먼저 리턴된다.
        """
        # 항상 response listener 가 떠 있도록 보장
        self.start_response_listener()
        
        # 1) 입력 데이터를 작업 단위 payload 리스트로 변환
        base_payloads = self.data_to_payloads(data)
        total_parts = len(base_payloads)
        if total_parts == 0:
            # 쪼갤 job 이 없다면, 서브클래스가 빈 응답을 처리하게 맡긴다.
            return self.resp_to_data(
                {
                    "request_id": None,
                    "job_ids": [],
                    "total_parts": 0,
                    "results": {},
                    "done": True,
                }
            )
        
        # 하나의 큰 요청에 대한 식별자
        request_id = str(uuid.uuid4())
        
        # 2) 각 payload 에 request_id / part_index / total_parts 메타를 붙인다.
        payloads: List[dict] = []
        for idx, p in enumerate(base_payloads):
            item = dict(p) if isinstance(p, dict) else {"value": p}
            item.setdefault("request_id", request_id)
            item.setdefault("part_index", idx)
            item.setdefault("total_parts", total_parts)
            payloads.append(item)
        
        # 3) 요청 상태를 먼저 등록해 둔다.
        with self._requests_lock:
            self._requests[request_id] = {
                "total_parts": total_parts,
                "done_count": 0,
                "results": {},
                "job_ids": [],
                "created_at": time.time(),
                "done": False,
                "request_payloads": payloads,
            }
        
        # 4) worker 로 job 을 전송
        job_ids = await self.produce(payloads)
        with self._requests_lock:
            state = self._requests.get(request_id)
            if state is not None:
                state["job_ids"] = job_ids
        
        # 5) 해당 요청이 완료될 때까지 대기
        deadline = time.time() + timeout
        with self._requests_lock:
            while True:
                state = self._requests.get(request_id)
                if state is None:
                    break
                if state.get("done"):
                    break
                
                remaining = deadline - time.time()
                if remaining <= 0:
                    break
                self._requests_cond.wait(timeout=remaining)
            
            # 정리하고 raw 결과를 빼낸다.
            state = self._requests.pop(request_id, None)
        
        if state is None:
            # 이 경우는 거의 없지만, 안전 장치로 빈 결과 처리
            raw_results = {
                "request_id": request_id,
                "job_ids": job_ids,
                "total_parts": total_parts,
                "results": {},
                "done": False,
            }
        else:
            raw_results = {
                "request_id": request_id,
                "job_ids": state.get("job_ids", job_ids),
                "total_parts": state.get("total_parts", total_parts),
                "results": state.get("results", {}),
                "done": state.get("done", False),
            }
        
        # 6) 최종 응답 포맷으로 변환 (task 별로 구현)
        return self.resp_to_data(raw_results)
    
    async def receive(self, data: dict, timeout: int = 60):
        """
        기존의 batch 단위 동기 gather 가 아니라,
        여러 덩어리의 요청이 동시에 들어와도
        먼저 끝난 요청부터 응답을 주는 패턴을 기본으로 한다.
        
        내부적으로 request_and_wait 를 사용한다.
        """
        return await self.request_and_wait(data, timeout=timeout)
    
# class BrokerEndpointUvicorn:
#     def __init__(self, broker: RedisBroker):
#         self.broker = broker
        
#     def __call__(self, request: Request):
#         return self.broker.handle_request(request)