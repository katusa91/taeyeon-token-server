"""
태연봇 음성 웹앱용 - Gemini Live API 임시 토큰(ephemeral token) 발급 서버

역할: 진짜 GEMINI_API_KEY는 이 서버(Railway 환경변수)에만 보관하고,
웹앱이 접속할 때마다 30분짜리 1회용 임시 토큰만 내려준다.
웹앱은 이 임시 토큰으로 Gemini와 직접 WebSocket 연결한다 (Google 문서의
client-to-server + ephemeral token 패턴).
"""

import os
import datetime

from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS
from google import genai

GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]

# 웹앱 setup 메시지에서 쓰는 모델과 반드시 동일해야 함
MODEL_NAME = "models/gemini-2.5-flash-native-audio-preview-12-2025"

# taeyeon-voice.html의 SYSTEM_PROMPT와 반드시 동일하게 유지할 것.
# 임시 토큰(ephemeral token) + BidiGenerateContentConstrained 방식에서는
# 브라우저가 나중에 WebSocket setup 메시지로 보내는 systemInstruction이
# 통째로 무시된다. 페르소나는 반드시 여기, 토큰 발급 시점에 박아넣어야 적용된다.
SYSTEM_PROMPT = """너는 '태연'이다. 서울 연남동 골목에서 작은 타로샵을 운영하는 29세 여성으로, 회색 푸들 '제로'와 함께 지낸다.
말투는 다정하고 담백하며, 손님을 편하게 대하되 과하게 들뜨지 않는다. 반말과 존댓말 사이, 친근한 존댓말을 기본으로 쓴다.
답변은 실제 대화처럼 짧고 자연스럽게 한다 — 한 번에 1~3문장 정도. 음성 대화이므로 목록이나 형식적인 문장은 피하고, 사람이 말하듯 이어간다.
사용자가 어떤 언어로 말하든, 너는 항상 한국어로만 대답한다. 절대로 다른 언어로 전환하지 않는다.
너는 실존 인물이 아니라 가상의 캐릭터다."""

# 실제로 웹앱을 서빙할 도메인으로 좁혀두는 걸 권장 (일단 전체 허용)
ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "*")

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": ALLOWED_ORIGIN}})

client = genai.Client(api_key=GEMINI_API_KEY, http_options={"api_version": "v1alpha"})


@app.get("/")
def index():
    # taeyeon-voice.html이 이 파일과 같은 디렉토리에 있다고 가정.
    # 같은 서비스에서 내려주므로 /api/token 호출이 같은 오리진(same-origin)이 되어
    # 브라우저의 CORS 제약을 받지 않는다.
    return send_from_directory(os.path.dirname(os.path.abspath(__file__)), "taeyeon-voice.html")


@app.post("/api/token")
def issue_token():
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    expire_time = now + datetime.timedelta(minutes=30)
    new_session_expire_time = now + datetime.timedelta(minutes=2)

    token = client.auth_tokens.create(
        config={
            "uses": 1,
            "expire_time": expire_time.isoformat(),
            "new_session_expire_time": new_session_expire_time.isoformat(),
            "live_connect_constraints": {
                "model": MODEL_NAME,
                "config": {
                    "response_modalities": ["AUDIO"],
                    "system_instruction": {
                        "parts": [{"text": SYSTEM_PROMPT}]
                    },
                    "speech_config": {
                        "voice_config": {
                            "prebuilt_voice_config": {"voice_name": "Despina"}
                        },
                        "language_code": "ko-KR",
                    },
                    "input_audio_transcription": {},
                    "output_audio_transcription": {},
                },
            },
            "http_options": {"api_version": "v1alpha"},
        }
    )

    return jsonify({"token": token.name, "expiresAt": expire_time.isoformat()})


@app.get("/healthz")
def healthz():
    return "ok"


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
