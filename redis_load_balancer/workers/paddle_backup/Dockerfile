FROM nvidia/cuda:12.6.0-runtime-ubuntu22.04

WORKDIR /app

# Python & 필수 시스템 라이브러리 설치 (파이썬 + OpenCV/paddleocr 의존성)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        libgl1 \
        libglib2.0-0 \
        libsm6 \
        libxext6 \
        libxrender1 \
        ffmpeg \
    && ln -s /usr/bin/python3 /usr/bin/python && \
    python -m pip install --upgrade pip && \
    rm -rf /var/lib/apt/lists/*

# 의존성 먼저 복사해서 캐시 최대 활용
COPY requirements.txt /app/requirements.txt

RUN pip install -r requirements.txt

# 애플리케이션 코드 복사
COPY redis_load_balancer /app/redis_load_balancer
COPY weights /app/weights

