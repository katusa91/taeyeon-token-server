# gunicorn이 실행 폴더의 gunicorn.conf.py를 자동으로 읽는다 (시작 명령에 --config/--worker-class 등을 직접 쓰면 그 값이 우선).
# 기본 sync 워커는 요청 하나가 30초를 넘으면 워커를 죽여서(WORKER TIMEOUT) 스트리밍 대답이 'network error'로 끊긴다.
# 또 한 번에 요청 하나만 처리해서, 대답 스트림이 열려 있는 동안 /api/tts 같은 다른 요청이 줄을 서게 된다.
import os

worker_class = "gthread"                                  # 요청마다 스레드로 처리: 긴 스트림 중에도 다른 요청이 같이 돈다
workers = int(os.environ.get("WEB_WORKERS", "1"))         # 서버 메모리 캐시(필러 음성 등)가 있어서 워커는 1개로 둔다
threads = int(os.environ.get("WEB_THREADS", "16"))
timeout = int(os.environ.get("WEB_TIMEOUT", "180"))       # gthread에서는 '워커가 멈췄는지' 보는 시간이라 길어도 안전
graceful_timeout = 30
keepalive = 65
