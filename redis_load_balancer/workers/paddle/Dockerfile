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
        vim \
        nano \
    && ln -s /usr/bin/python3 /usr/bin/python && \
    python -m pip install --upgrade pip && \
    rm -rf /var/lib/apt/lists/*

COPY requirements-cpu.txt /app/requirements-cpu.txt
RUN pip install -r requirements-cpu.txt

# 애플리케이션 코드 복사
COPY redis_load_balancer /app/redis_load_balancer
COPY weights /app/weights
COPY sample_image /app/sample_image

