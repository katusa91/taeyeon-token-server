"""
태연봇 음성 웹앱용 - Gemini Live API 임시 토큰(ephemeral token) 발급 서버

역할: 진짜 GEMINI_API_KEY는 이 서버(Railway 환경변수)에만 보관하고,
웹앱이 접속할 때마다 30분짜리 1회용 임시 토큰만 내려준다.
웹앱은 이 임시 토큰으로 Gemini와 직접 WebSocket 연결한다 (Google 문서의
client-to-server + ephemeral token 패턴).

연결 끊김 복구 지원:
웹앱 접속 중 소켓 연결이 끊기면 웹앱 클라이언트는 /api/token?history=1 엔드포인트를
다시 호출하여 새로운 토큰을 얻습니다. 서버는 최근 DB 대화 내역(conversations)을
시스템 프롬프트에 자동으로 포함하여 재연결 후에도 끊김 없이 대화 맥락이 유지됩니다.
"""

import base64
import json
import os
import re
import time
import math
import random
import hashlib
import logging
import datetime
import threading
from collections import Counter, OrderedDict
from concurrent.futures import ThreadPoolExecutor
from functools import wraps

import requests
from flask import Flask, Response, jsonify, request, send_from_directory
from flask_cors import CORS
from google import genai
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

try:
    import psycopg2
except ImportError:
    psycopg2 = None

try:
    from google.oauth2 import id_token as _google_id_token
    from google.auth.transport import requests as _google_auth_transport_requests
    _google_auth_request = _google_auth_transport_requests.Request()
except ImportError:
    _google_id_token = None
    _google_auth_request = None

GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
MODEL_NAME = os.environ.get("LIVE_MODEL", "models/gemini-3.8-live")

PERSONA_NAME = os.environ.get("PERSONA_NAME", "태연").strip() or "태연"
VOICE_NAME = os.environ.get("VOICE_NAME", "Leda").strip() or "Leda"
PERSONA_FILE = os.environ.get("PERSONA_FILE", "").strip()
INDEPENDENT_PERSONA = bool(PERSONA_FILE)

try:
    import market_brief
except Exception as _mb_err:  # noqa: BLE001
    market_brief = None
    logging.warning("market_brief 모듈을 불러오지 못해 주식/코인 분석 기능을 끕니다: %s", _mb_err)
MARKET_TOOL_ENABLED = bool(market_brief is not None and market_brief.MARKET_ENABLED and not INDEPENDENT_PERSONA)

PERSONA_TAGLINE = os.environ.get("PERSONA_TAGLINE", "").strip()
_aff = os.environ.get("AFFECTIVE_DIALOG", "").strip()
AFFECTIVE_DIALOG_ENABLED = (_aff != "0") if _aff else (not INDEPENDENT_PERSONA)
PERSONA_USER_INFO = os.environ.get("PERSONA_USER_INFO", "").replace("\\n", "\n").strip()
_CHANNEL_PHRASE = "이전 통화든" if INDEPENDENT_PERSONA else "텔레그램이든"

LIVE_SEARCH_ENABLED = os.environ.get("LIVE_SEARCH", "1").strip() != "0"
INPUT_TRANSCRIPTION_LANGUAGE = os.environ.get("INPUT_TRANSCRIPTION_LANGUAGE", "").strip()

STT_MODEL_NAME = os.environ.get("STT_MODEL", "models/gemini-3.5-transcribe-live")
STT_LANGUAGE = os.environ.get("STT_LANGUAGE", "ko-KR").strip() or "ko-KR"

KAKAO_REST_API_KEY = os.environ.get("KAKAO_REST_API_KEY", "").strip()
ODSAY_API_KEY = os.environ.get("ODSAY_API_KEY", "").strip()

TAGO_SERVICE_KEY = os.environ.get("TAGO_SERVICE_KEY", "").strip().strip("\"'")
if "%" in TAGO_SERVICE_KEY:
    from urllib.parse import unquote as _unquote
    TAGO_SERVICE_KEY = _unquote(TAGO_SERVICE_KEY)
EXPRESS_DEP_KEYWORDS = [k.strip() for k in os.environ.get("EXPRESS_DEP_KEYWORDS", "").split(",") if k.strip()]
EXPRESS_BUS_ENABLED = bool(TAGO_SERVICE_KEY)
TRAIN_TIMETABLE_ENABLED = bool(TAGO_SERVICE_KEY) and os.environ.get("TRAIN_TIMETABLE", "1").strip() != "0"
TRAIN_DEP_KEYWORDS = [k.strip() for k in os.environ.get("TRAIN_DEP_KEYWORDS", "").split(",") if k.strip()]
BUS_ARRIVAL_ENABLED = bool(TAGO_SERVICE_KEY) and os.environ.get("BUS_ARRIVAL", "1").strip() != "0"
ODSAY_REFERER = os.environ.get("ODSAY_REFERER", "").strip()
BUS_ROUTE_ENABLED = (
    os.environ.get("BUS_ROUTE_ENABLED", "1").strip() != "0"
    and bool(KAKAO_REST_API_KEY)
    and bool(ODSAY_API_KEY)
)
NEARBY_PLACES_ENABLED = bool(KAKAO_REST_API_KEY) and os.environ.get("NEARBY_PLACES", "1").strip() != "0"
KAKAO_JS_KEY = os.environ.get("KAKAO_JS_KEY", "").strip()

def _env_float(name: str, default=None):
    v = os.environ.get(name, "").strip()
    if not v:
        return default
    try:
        return float(v)
    except ValueError:
        return default

def _env_int(name: str, default: int) -> int:
    v = os.environ.get(name, "").strip()
    if not v:
        return default
    try:
        return int(v)
    except ValueError:
        return default

HOME_LAT = _env_float("HOME_LAT")
HOME_LNG = _env_float("HOME_LNG")
WORK_LAT = _env_float("WORK_LAT")
WORK_LNG = _env_float("WORK_LNG")
HOME_RADIUS_M = _env_float("HOME_RADIUS_M", 150)
WORK_RADIUS_M = _env_float("WORK_RADIUS_M", 150)
DEBUG_LOG_LOCATION = os.environ.get("DEBUG_LOG_LOCATION", "0").strip() == "1"

def _haversine_m(lat1, lon1, lat2, lon2):
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def get_location_label(lat, lng):
    if DEBUG_LOG_LOCATION and lat is not None and lng is not None:
        logging.info("[위치 디버그] 받은 좌표: lat=%s lng=%s", lat, lng)
    has_home = HOME_LAT is not None and HOME_LNG is not None
    has_work = WORK_LAT is not None and WORK_LNG is not None
    if lat is None or lng is None or not (has_home or has_work):
        return ""
    if has_home and _haversine_m(lat, lng, HOME_LAT, HOME_LNG) <= HOME_RADIUS_M:
        return "집"
    if has_work and _haversine_m(lat, lng, WORK_LAT, WORK_LNG) <= WORK_RADIUS_M:
        return "회사"
    if DEBUG_LOG_LOCATION:
        logging.info("[위치 디버그] lat=%s lng=%s -> 밖으로 판정", lat, lng)
    return "밖"

PERSONA_PROMPT = """당신은 '태연'입니다. 유명 가수 태연과는 동명이인입니다. 한국어로만 답변하고 기본 반말로 편안하게 친근하게 대화하세요."""
VOICE_PROMPT = """[음성 대화 - 지금은 실시간 음성 통화다]
- 목록, 번호, 기호, 이모티콘을 쓰지 말고 실제 사람이 말하듯 자연스럽게 대화할 것
- 상대가 말을 걸면 친근하게 받아주고 이전 통화 내용이 있다면 그에 이어서 대화를 다정하게 이어갈 것
"""

if INDEPENDENT_PERSONA:
    _pf = PERSONA_FILE if os.path.isabs(PERSONA_FILE) else os.path.join(os.path.dirname(os.path.abspath(__file__)), PERSONA_FILE)
    try:
        with open(_pf, encoding="utf-8") as _f:
            PERSONA_PROMPT = _f.read().strip()
    except OSError as _e:
        raise RuntimeError(f"PERSONA_FILE을 읽지 못했습니다: {_pf} ({_e})")

STT_NOTE = """[입력 방식 안내]
- 상대의 말은 한국어 음성 인식 결과가 글자로 전달된다. 문맥상 가장 그럴듯한 뜻으로 유연하게 이해하고 답할 것."""

KST = datetime.timezone(datetime.timedelta(hours=9))

DATABASE_URL = os.environ.get("DATABASE_URL")

ADMIN_USER_IDS = {
    int(uid.strip()) for uid in os.environ.get("ADMIN_USER_IDS", "").split(",") if uid.strip().isdigit()
}
_owner_env = os.environ.get("OWNER_USER_ID", "").strip()
if _owner_env.isdigit():
    OWNER_USER_ID = int(_owner_env)
elif ADMIN_USER_IDS:
    OWNER_USER_ID = min(ADMIN_USER_IDS)
else:
    OWNER_USER_ID = None

ACTIVE_CONTEXT_LIMIT = 20

def _history_db_connect():
    return psycopg2.connect(DATABASE_URL, sslmode="require", connect_timeout=5)

def get_history_today(user_id, max_rows=600):
    if not DATABASE_URL or psycopg2 is None or user_id is None:
        return []
    conn = _history_db_connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT role, content, created_at FROM conversations
            WHERE user_id = %s ORDER BY created_at DESC LIMIT %s
        """, (user_id, max_rows))
        rows = cur.fetchall()
        cur.close()
    finally:
        conn.close()
    today = datetime.datetime.now(KST).date()
    out = []
    for r in reversed(rows):
        c = r[2]
        c_naive = c.replace(tzinfo=None) if getattr(c, "tzinfo", None) else c
        if c_naive.date() == today:
            out.append({"role": r[0], "content": r[1], "created_at": r[2]})
    return out

def load_recent_conversation_and_maybe_reset():
    """재연결 시 최근 대화 내역을 가져와 이전 맥락을 복구합니다."""
    if not DATABASE_URL or psycopg2 is None or OWNER_USER_ID is None:
        return []
    try:
        rows = get_history_today(OWNER_USER_ID, max_rows=200)
        return rows[-ACTIVE_CONTEXT_LIMIT:] if rows else []
    except Exception as e:
        logging.warning("최근 대화 조회 실패 - %s", e)
        return []

VOICE_HISTORY_MAX_CHARS = _env_int("VOICE_HISTORY_MAX_CHARS", 20000)

def build_voice_history_context(rows):
    if not rows:
        return "", 0

    picked, used = [], 0
    for r in reversed(rows):
        line = f"{'상대' if r['role'] == 'user' else PERSONA_NAME}: {r['content']}"[:VOICE_HISTORY_MAX_CHARS]
        if picked and used + len(line) > VOICE_HISTORY_MAX_CHARS:
            break
        picked.append(line)
        used += len(line)
    picked.reverse()

    block = (
        f"[직전 대화 기록 - 소켓이 재연결되었거나 방금 전까지 나누던 대화 흐름입니다. "
        f"자연스럽게 맥락을 이어서 대답해주세요]\n" + "\n".join(picked)
    )
    return block, len(picked)

def build_system_prompt(stt_mode: bool = False, voice_history: str = "", location_label: str = "", eleven_voice: bool = False) -> str:
    parts = [PERSONA_PROMPT]
    if voice_history:
        parts.append(voice_history)
    parts.append(VOICE_PROMPT)
    if stt_mode:
        parts.append(STT_NOTE)

    now = datetime.datetime.now(tz=KST)
    time_block = (
        f"\n\n[현재 시각] {now.strftime('%Y년 %m월 %d일 %H시 %M분')}\n"
    )
    return "\n\n".join(parts) + time_block

ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "*")

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 10 * 1024 * 1024
CORS(app, resources={r"/api/*": {"origins": ALLOWED_ORIGIN, "allow_headers": ["Content-Type", "Authorization"]}})

client = genai.Client(api_key=GEMINI_API_KEY, http_options={"api_version": "v1alpha"})

GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID", "").strip()
_ALLOWED_GOOGLE_EMAILS = {
    e.strip().lower() for e in os.environ.get("ALLOWED_GOOGLE_EMAILS", "").split(",") if e.strip()
}

SESSION_SECRET = os.environ.get("SESSION_SECRET", "").strip()
if not SESSION_SECRET:
    SESSION_SECRET = hashlib.sha256((GEMINI_API_KEY + "|taeyeon-voice-session").encode("utf-8")).hexdigest()
SESSION_MAX_AGE_SEC = int(os.environ.get("SESSION_MAX_AGE_SEC", str(30 * 24 * 3600)))
_session_serializer = URLSafeTimedSerializer(SESSION_SECRET, salt="taeyeon-voice-auth")

def _verify_session_token(token: str):
    if not token:
        return None
    try:
        data = _session_serializer.loads(token, max_age=SESSION_MAX_AGE_SEC)
    except (BadSignature, SignatureExpired):
        return None
    email = str(data.get("email", "")).strip().lower()
    if _ALLOWED_GOOGLE_EMAILS and email not in _ALLOWED_GOOGLE_EMAILS:
        return None
    return email

def require_google_auth(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not GOOGLE_CLIENT_ID:
            return fn(*args, **kwargs)
        auth_header = request.headers.get("Authorization", "")
        token = auth_header[len("Bearer "):].strip() if auth_header.startswith("Bearer ") else ""
        email = _verify_session_token(token)
        if not email:
            return jsonify({"error": "unauthorized"}), 401
        request.google_email = email
        return fn(*args, **kwargs)
    return wrapper

@app.get("/")
def index():
    return send_from_directory(os.path.dirname(os.path.abspath(__file__)), "taeyeon-voice.html")

@app.get("/api/config")
def get_config():
    return jsonify({
        "authRequired": bool(GOOGLE_CLIENT_ID),
        "googleClientId": GOOGLE_CLIENT_ID,
        "personaName": PERSONA_NAME,
        "personaTagline": PERSONA_TAGLINE,
    })

@app.post("/api/token")
@require_google_auth
def issue_token():
    """
    웹앱 접속 시 및 끊김 후 재연결 시 호출되는 토큰 발급 엔드포인트.
    ?history=1 파라미터를 추가하면 DB에서 최근 대화 기록을 가져와 프롬프트에 실어줍니다.
    """
    text_input_mode = request.args.get("mode") == "stt"
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    expire_time = now + datetime.timedelta(minutes=30)
    new_session_expire_time = now + datetime.timedelta(minutes=2)

    try:
        _lat = float(request.args.get("lat"))
        _lng = float(request.args.get("lng"))
    except (TypeError, ValueError):
        _lat = _lng = None
    location_label = get_location_label(_lat, _lng)

    voice_history_ctx, history_count = "", 0
    if request.args.get("history") == "1":
        voice_history_ctx, history_count = build_voice_history_context(load_recent_conversation_and_maybe_reset())

    token = client.auth_tokens.create(
        config={
            "uses": 1,
            "expire_time": expire_time.isoformat(),
            "new_session_expire_time": new_session_expire_time.isoformat(),
            "live_connect_constraints": {
                "model": MODEL_NAME,
                "config": {
                    "response_modalities": ["AUDIO"],
                    "enable_affective_dialog": AFFECTIVE_DIALOG_ENABLED,
                    "system_instruction": {
                        "parts": [{"text": build_system_prompt(
                            stt_mode=text_input_mode, voice_history=voice_history_ctx, location_label=location_label
                        )}]
                    },
                    "speech_config": {
                        "voice_config": {
                            "prebuilt_voice_config": {"voice_name": VOICE_NAME}
                        },
                    },
                    "output_audio_transcription": {},
                },
            },
            "http_options": {"api_version": "v1beta"},
        }
    )

    return jsonify({
        "token": token.name, "model": MODEL_NAME, "expiresAt": expire_time.isoformat(),
        "historyCount": history_count,
    })

@app.post("/api/voice-log")
@require_google_auth
def voice_log():
    if not DATABASE_URL or psycopg2 is None or OWNER_USER_ID is None:
        return jsonify({"saved": 0, "reason": "db_not_configured"})
    data = request.get_json(force=True, silent=True) or {}
    msgs = data.get("messages")
    if not isinstance(msgs, list):
        return jsonify({"error": "bad request"}), 400
    
    saved_count = 0
    conn = _history_db_connect()
    try:
        cur = conn.cursor()
        for m in msgs:
            role, text = m.get("role"), m.get("text")
            if role in ("user", "model") and text:
                role_db = "assistant" if role == "model" else "user"
                cur.execute(
                    "INSERT INTO conversations (user_id, role, content, created_at) VALUES (%s, %s, %s, NOW())",
                    (OWNER_USER_ID, role_db, text)
                )
                saved_count += 1
        conn.commit()
        cur.close()
    except Exception as e:
        logging.warning("대화 적재 실패: %s", e)
    finally:
        conn.close()

    return jsonify({"saved": saved_count})

@app.get("/healthz")
def healthz():
    return jsonify({"status": "ok"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
