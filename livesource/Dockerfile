FROM python:3.12-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends ffmpeg \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src ./src

# Pre-generate a 2-second playable MPEG-TS seed segment (testsrc + tone).
RUN ffmpeg -y -loglevel error \
    -f lavfi -i testsrc=size=320x240:rate=15 \
    -f lavfi -i sine=frequency=440:sample_rate=44100 \
    -t 2 -c:v libx264 -preset veryfast -g 30 -c:a aac \
    -f mpegts /srv/seed.ts

ENV WINDOW_SIZE=5 SEGMENT_DURATION=2 SEED_TS=/srv/seed.ts
EXPOSE 8080
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8080"]
