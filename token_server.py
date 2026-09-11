"""
태연봇 음성 웹앱용 - Gemini Live API 임시 토큰(ephemeral token) 발급 서버

역할: 진짜 GEMINI_API_KEY는 이 서버(Railway 환경변수)에만 보관하고,
웹앱이 접속할 때마다 30분짜리 1회용 임시 토큰만 내려준다.
웹앱은 이 임시 토큰으로 Gemini와 직접 WebSocket 연결한다 (Google 문서의
client-to-server + ephemeral token 패턴).
"""

import os
import datetime

from flask import Flask, jsonify
from flask_cors import CORS
from google import genai

GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]

# 웹앱 setup 메시지에서 쓰는 모델과 반드시 동일해야 함
MODEL_NAME = "models/gemini-live-2.5-flash-preview-native-audio-09-2025"

# 실제로 웹앱을 서빙할 도메인으로 좁혀두는 걸 권장 (일단 전체 허용)
ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "*")

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": ALLOWED_ORIGIN}})

client = genai.Client(api_key=GEMINI_API_KEY, http_options={"api_version": "v1alpha"})


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
