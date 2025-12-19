from time import perf_counter
from collections import defaultdict
from typing import Union
try:
    import torch
except ImportError:
    torch = None

class Timer:
    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.elapsed = {}
        self.total_elapsed = defaultdict(float)
        self.elapsed_count = defaultdict(int)
        self.in_idx = 0
        self.out_idx = 0
        
    def __enter__(self):
        self._sync()
        self.start_time = perf_counter()
        return self

    def _sync(self):
        if torch is not None and torch.cuda.is_available():
            torch.cuda.synchronize()
    
    def in_time(self, text:Union[str, int, None]=0):
        if text is None:
            tname = f"DT_{self.in_idx}"
            self.in_idx += 1
        else:
            if isinstance(text, int):
                tname = f"t_{text}"
            else:
                tname = text
        self._sync()
        self.elapsed[tname] = perf_counter()
        return self
    
    def out_time(self, text:Union[str, int, None]=0):
        if text is None:
            tname = f"DT_{self.out_idx}"
            self.out_idx += 1
        else:
            if isinstance(text, int):
                tname = f"t_{text}"
            else:
                tname = text
        self._sync()
        if tname not in self.elapsed:
            print("Warning: Time name not found in elapsed, use in_time first")
            return self
        
        self.elapsed[tname] = perf_counter() - self.elapsed[tname]
        self.total_elapsed[tname] += self.elapsed[tname]
        self.elapsed_count[tname] += 1
        return self
    
    def clear(self, except_time:list[str]=[]):
        keys = list(self.elapsed.keys())    
        [self.elapsed.pop(k) for k in keys if k not in except_time]
        [self.total_elapsed.pop(k) for k in keys if k not in except_time]
        [self.elapsed_count.pop(k) for k in keys if k not in except_time]
        self.in_idx = 0
        self.out_idx = 0
    
    @property
    def avg_elapsed(self):
        return {k: v/self.elapsed_count[k] for k, v in self.total_elapsed.items()}
    
    def __exit__(self, exc_type, exc_value, traceback):
        self._sync()
        self.end_time = perf_counter()
        self.elapsed["total"] = self.end_time - self.start_time
        self.total_elapsed["total"] += self.elapsed["total"]
        return False

class DummyTimer:
    def __init__(self):
        pass
    def in_time(self, *args, **kwargs):
        pass
    def out_time(self, *args, **kwargs):
        pass
    def clear(self, *args, **kwargs):
        pass
    def __enter__(self):
        pass
    def __exit__(self, exc_type, exc_value, traceback):
        pass
