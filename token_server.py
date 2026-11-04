"""
태연봇 음성 웹앱용 - Gemini Live API 임시 토큰(ephemeral token) 발급 서버

역할: 진짜 GEMINI_API_KEY는 이 서버(Railway 환경변수)에만 보관하고,
웹앱이 접속할 때마다 30분짜리 1회용 임시 토큰만 내려준다.
웹앱은 이 임시 토큰으로 Gemini와 직접 WebSocket 연결한다 (Google 문서의
client-to-server + ephemeral token 패턴).

캐릭터 설정(텔레그램 봇의 /addinfo 추가 정보, 커스텀 프롬프트)과 owner 개인화 데이터(호감도/감정/기억/
약속/대화요약/운세/일정 등)는 DATABASE_URL이 설정돼 있으면 태연봇과 같은 PostgreSQL에서 읽어와
페르소나 뒤에 붙인다 (get_owner_full_context 참고).

음성 대화 기록: 텔레그램 태연봇(taeyeon_bot.py)과 같은 페르소나로 취급하기로 하고, 별도 테이블 없이
taeyeon_bot.py와 같은 conversations 테이블에 OWNER_USER_ID로 그대로 적재한다. taeyeon_bot.py는 다른
레포(taeyeon-bot)에 있어서 모듈로 import할 수 없으므로, add_to_history/get_history/
reset_history_if_long_gap/update_user 로직을 이 파일 "===== 대화 기록 DB 함수 =====" 절에 그대로
옮겨와 직접 구현했다 (save_voice_turns/load_recent_conversation_and_maybe_reset 참고). 그래서
텔레그램에서 하던 얘기를 음성으로 이어가거나 그 반대도 자연스럽게 이어지고, 하루 단위 초기화·오래된
대화 요약 압축 규칙도 텔레그램과 동일하다.
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


def _env_int(name: str, default=None):
    v = os.environ.get(name, "").strip()
    if not v:
        return default
    try:
        return int(v)
    except ValueError:
        return default


def _haversine_m(lat1, lng1, lat2, lng2):
    """두 좌표 사이의 거리(미터)를 반환한다."""
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = p2 - p1
    dl = math.radians(lng2 - lng1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


HOME_LAT = _env_float("HOME_LAT")
HOME_LNG = _env_float("HOME_LNG")
WORK_LAT = _env_float("WORK_LAT")
WORK_LNG = _env_float("WORK_LNG")
HOME_RADIUS_M = _env_float("HOME_RADIUS_M", 150)
WORK_RADIUS_M = _env_float("WORK_RADIUS_M", 150)
DEBUG_LOG_LOCATION = os.environ.get("DEBUG_LOG_LOCATION", "0").strip() == "1"


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

FIND_BUS_ROUTE_DECLARATION = {
    "name": "find_bus_route",
    "description": (
        "사용자의 현재 위치(브라우저 GPS)를 기준으로, 사용자가 이번 발화에서 명시적으로 말한 목적지 근방까지 "
        "가는 대중교통(버스 위주) 경로를 조회한다. 어느 정류장에서 몇 번 버스를 타야 하는지, 어느 정류장에서 "
        "내려야 하는지를 알려준다. 사용자가 '거기 가려면 몇 번 버스 타야 돼?', '거기까지 어떻게 가?', "
        "'가까운 정류장이 어디야?' 처럼 목적지까지의 버스 경로/정류장을 물어보면 반드시 이 함수를 호출해서 "
        "실제 경로를 확인한 뒤 답할 것 (직접 지어내서 답하지 말 것). 목적지는 정확한 주소가 아니라 대략적인 "
        "지명/건물/역/랜드마크 이름이어도 된다.\n"
        "⚠️ 반드시 지킬 것: 사용자가 이번 발화에서 실제로 입력/발화한 목적지 이름이 없으면 이 함수를 "
        "절대 호출하지 말 것 (예시 문구나 " + PERSONA_NAME + " 자신이 있는 곳을 목적지로 지어내서 호출하는 것 금지). "
        "애매하거나 목적지가 안 들렸으면 함수를 호출하지 말고 '어디로 가려는지 다시 말해달라'고 되물을 것.\n"
        "⚠️ 다음과 같은 경우는 호출 대상이 아니다 (지명이 들렸다고 무조건 호출하지 말 것): "
        "사용자가 그냥 지명/장소를 언급만 하거나 그 지역에 대한 이야기(추억, 소감, 날씨 등)를 하는 경우, "
        "이미 그곳에 있다거나 다녀왔다는 과거·완료형 발화인 경우, 가정/농담으로 지명을 꺼낸 경우. "
        "'어떻게 가', '몇 번 버스', '정류장', '경로', '타야 돼' 처럼 이동 수단·방법을 직접 묻는 표현이 "
        "있을 때만 호출할 것."
    ),
    "parameters": {
        "type": "OBJECT",
        "properties": {
            "destination": {
                "type": "STRING",
                "description": (
                    "사용자가 이번 발화에서 직접 말한 목적지 근방의 지명/건물/역/랜드마크 이름을 "
                    "그대로 옮겨 적을 것. 아래는 '이런 형식의 값이 들어간다'는 예시일 뿐, 실제 목적지가 "
                    "아니면 이 예시 자체를 값으로 쓰지 말 것 (예시 형식: '○○역', '△△동 카페거리', '□□빌딩')."
                ),
            }
        },
        "required": ["destination"],
    },
}

FIND_INTERCITY_ROUTE_DECLARATION = {
    "name": "find_intercity_route",
    "description": (
        "사용자의 현재 위치(브라우저 GPS)를 기준으로, 사용자가 이번 발화에서 명시적으로 말한 '다른 도시'까지 "
        "기차(KTX 등)/고속버스/시외버스로 가는 방법을 조회한다. 이동수단 종류, 대략 걸리는 시간, 요금, 환승 정도를 "
        "알려준다(고속버스와 기차는 지금 이후 출발편의 시각과 요금도 알려준다). 사용자가 '서울 가려면 뭐 타야 돼?', '부산까지 KTX 있어?', '대구 고속버스로 얼마나 걸려?', '서울 가는 고속버스 몇 시에 있어?', '서울 가는 KTX 몇 시에 있어?' 처럼 "
        "다른 도시로의 장거리 이동 방법을 물으면 이 함수를 호출해서 확인한 뒤 답할 것 (지어내지 말 것). "
        "같은 도시 안에서 이동하는 질문(시내버스)에는 이 함수 대신 find_bus_route를 쓴다.\n"
        "⚠️ 사용자가 이번 발화에서 실제로 말한 목적지가 없으면 절대 호출하지 말고 어디로 가려는지 되물을 것. "
        "⚠️ 지명이 들렸다고 무조건 호출하지 말 것: 그냥 지명을 언급만 하거나 그 지역 얘기를 하는 경우, "
        "이미 다녀왔다는 과거형 발화, 가정/농담으로 지명을 꺼낸 경우는 호출 대상이 아니다. "
        "'뭐 타야 돼', 'KTX 있어', '얼마나 걸려', '몇 시에 있어', '다음 차 언제야' 처럼 이동 수단·방법을 직접 물을 때만 호출할 것. "
        "이 함수는 조회 전용이며 예약/결제는 할 수 없다."
    ),
    "parameters": {
        "type": "OBJECT",
        "properties": {
            "destination": {
                "type": "STRING",
                "description": (
                    "사용자가 이번 발화에서 직접 말한 목적지 도시나 역/터미널 이름을 그대로 옮겨 적을 것 "
                    "(예시 형식: '○○', '△△역', '□□터미널'). 실제 목적지가 아니면 예시 자체를 값으로 쓰지 말 것."
                ),
            }
        },
        "required": ["destination"],
    },
}

FIND_BUS_ARRIVAL_DECLARATION = {
    "name": "find_bus_arrival",
    "description": (
        "사용자가 특정 번호의 버스가 언제 오는지(도착 예정 시간)를 물으면 이 함수를 호출한다. "
        "'220번 버스 언제 와?', '302번 몇 분 남았어?', '거기 8100번 아직 안 왔어?' 처럼 버스 번호 + "
        "도착 시점을 묻는 질문에 쓴다 (목적지까지 가는 방법을 묻는 find_bus_route와는 다르다 - 이건 이미 탈 "
        "버스 번호를 알고 있고 그 버스가 언제 오는지만 궁금한 경우다).\n"
        "⚠️ 사용자가 정류장 이름을 말하지 않았으면 stop_name은 비워둘 것 - 그러면 서버가 사용자의 현재 위치(GPS) "
        "에서 가장 가까운 정류소를 자동으로 찾아서 조회한다. 정류장 이름을 명시했을 때만 stop_name을 채울 것.\n"
        "⚠️ 사용자가 이번 발화에서 실제로 말한 버스 번호가 없으면 절대 호출하지 말고 몇 번 버스인지 되물을 것."
    ),
    "parameters": {
        "type": "OBJECT",
        "properties": {
            "route_no": {
                "type": "STRING",
                "description": "사용자가 말한 버스 번호를 숫자/문자 그대로 옮겨 적을 것 (예: '220', '8100', 'earth-1').",
            },
            "stop_name": {
                "type": "STRING",
                "description": (
                    "사용자가 이번 발화에서 정류장 이름을 직접 말했을 때만 채울 것 (예: '시청앞'). "
                    "말하지 않았으면 이 필드 자체를 넣지 말 것 - 현재 위치 기준 가장 가까운 정류소로 자동 조회된다."
                ),
            },
        },
        "required": ["route_no"],
    },
}

FIND_NEARBY_PLACES_DECLARATION = {
    "name": "find_nearby_places",
    "description": (
        "사용자가 현재 위치 근처의 가게나 시설을 찾을 때 이 함수를 호출한다. "
        "'근처에 편의점 어디 있어?', '이 근처 약국 있나?', '가까운 카페 좀 추천해줘', '주변에 국밥집 있어?' "
        "처럼 무엇을 찾는지(편의점/약국/카페/식당/마트/병원/주유소/화장실 등 종류나 특정 상호)가 있고 "
        "'근처'/'주변'/'가까운' 뉘앙스가 있을 때 쓴다. 목적지까지 가는 방법을 묻는 find_bus_route/find_intercity_route와는 다르다.\n"
        "⚠️ 이번 발화에서 실제로 찾으려는 대상이 없으면 호출하지 말고 뭘 찾는지 되물을 것."
    ),
    "parameters": {
        "type": "OBJECT",
        "properties": {
            "keyword": {
                "type": "STRING",
                "description": "사용자가 찾는 장소의 종류나 상호명을 그대로 옮겨 적을 것 (예: '편의점', '약국', '스타벅스', '국밥집').",
            },
        },
        "required": ["keyword"],
    },
}

YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY", "").strip()
YOUTUBE_SEARCH_ENABLED = bool(YOUTUBE_API_KEY) and os.environ.get("YOUTUBE_SEARCH", "1").strip() != "0"

FIND_YOUTUBE_VIDEO_DECLARATION = {
    "name": "find_youtube_video",
    "description": (
        "사용자가 특정 주제의 유튜브 영상을 찾아달라거나 보여달라고 할 때 이 함수를 호출한다. "
        "'요즘 유행하는 챌린지 영상 보여줘', '고양이 영상 찾아줘', '이 노래 뮤직비디오 있어?' 처럼 "
        "무엇을 찾는지가 있고 '유튜브'/'영상'/'동영상' 뉘앙스가 있을 때 쓴다. "
        "이미 링크가 있는 특정 영상 하나를 자세히 분석해달라는 요청과는 다르다(그건 사용자가 직접 링크를 보내야 함).\n"
        "⚠️ 이번 발화에서 실제로 찾으려는 주제/키워드가 없으면 호출하지 말고 무슨 영상을 찾는지 되물을 것.\n"
        "⚠️ 결과가 found:true면 이 대답이 끝난 뒤에 화면에 카드가 뜰 것이니, 제목이나 채널명을 하나하나 "
        "읽지 말고 '이런 영상들 찾았어요, 화면에서 골라봐요' 정도로 아주 짧게 한마디만 하고 끝낼 것. "
        "found:false일 때만(검색 실패했거나 결과가 0건일 때) 그 사실을 자연스럽게 짧게 말할 것."
    ),
    "parameters": {
        "type": "OBJECT",
        "properties": {
            "keyword": {
                "type": "STRING",
                "description": "사용자가 찾는 영상의 주제나 검색어를 그대로 옮겨 적을 것 (예: '고양이 브이로그', '아이유 라이브').",
            },
        },
        "required": ["keyword"],
    },
}


def _youtube_search(keyword: str, max_results: int = 5):
    if not YOUTUBE_API_KEY:
        return None, "not_configured"
    try:
        res = requests.get(
            "https://www.googleapis.com/youtube/v3/search",
            params={
                "part": "snippet", "q": keyword, "type": "video", "maxResults": max_results,
                "safeSearch": "moderate", "relevanceLanguage": "ko", "regionCode": "KR",
                "key": YOUTUBE_API_KEY,
            },
            timeout=8,
        )
    except requests.RequestException as e:
        logging.warning("[유튜브검색] API 요청 실패(%s)", type(e).__name__)
        return None, "api_error"
    if not res.ok:
        logging.warning("[유튜브검색] API HTTP %s: %s", res.status_code,
                         res.text[:300].replace(YOUTUBE_API_KEY, "***"))
        return None, "api_error"
    try:
        items = (res.json() or {}).get("items") or []
    except ValueError:
        return None, "api_error"
    results = []
    for it in items:
        vid = (it.get("id") or {}).get("videoId")
        if not vid:
            continue
        snippet = it.get("snippet") or {}
        thumbs = snippet.get("thumbnails") or {}
        thumb = (thumbs.get("medium") or thumbs.get("default") or {}).get("url", "")
        results.append({
            "videoId": vid,
            "title": " ".join(str(snippet.get("title") or "").split())[:100],
            "channelTitle": snippet.get("channelTitle", ""),
            "thumbnail": thumb,
            "url": f"https://www.youtube.com/watch?v={vid}",
        })
    return results, None


_NEARBY_CATEGORY_CODES = {"편의점": "CS2", "약국": "PM9", "카페": "CE7", "주유소": "OL7"}


def _kakao_search_nearby(keyword: str, lng: float, lat: float, radius: int = 1500, size: int = 5):
    if not KAKAO_REST_API_KEY:
        return None, "not_configured"
    params = {"query": keyword, "x": lng, "y": lat, "radius": radius, "sort": "distance", "size": size}
    category_code = _NEARBY_CATEGORY_CODES.get(keyword.replace(" ", ""))
    if category_code:
        params["category_group_code"] = category_code
    try:
        res = requests.get(
            "https://dapi.kakao.com/v2/local/search/keyword.json",
            headers={"Authorization": f"KakaoAK {KAKAO_REST_API_KEY}"},
            params=params,
            timeout=8,
        )
    except requests.exceptions.RequestException as e:
        logging.warning("[근처장소] 카카오 요청 실패(네트워크/타임아웃): %s", e)
        return None, "kakao_api_error"
    if not res.ok:
        logging.warning("[근처장소] 카카오 응답 오류 %s: %s", res.status_code, res.text[:300])
        return None, "kakao_api_error"
    try:
        docs = res.json().get("documents") or []
    except ValueError:
        return None, "parse_error"
    places = []
    for d in docs:
        try:
            dist = int(d.get("distance"))
        except (TypeError, ValueError):
            dist = None
        cat = (d.get("category_name") or "").split(" > ")[-1].strip() or None
        try:
            p_lat, p_lng = float(d["y"]), float(d["x"])
        except (KeyError, TypeError, ValueError):
            p_lat = p_lng = None
        places.append({
            "name": d.get("place_name") or "", "address": d.get("road_address_name") or d.get("address_name") or "",
            "distance_m": dist, "phone": d.get("phone") or None, "category": cat,
            "lat": p_lat, "lng": p_lng, "place_url": d.get("place_url") or None,
        })
    return places, None


def _kakao_search_place(query: str, ref_lng: float = None, ref_lat: float = None):
    if not KAKAO_REST_API_KEY:
        return None, "not_configured"
    params = {"query": query, "size": 15}
    if ref_lng is not None and ref_lat is not None:
        params.update({"x": ref_lng, "y": ref_lat})
    try:
        res = requests.get(
            "https://dapi.kakao.com/v2/local/search/keyword.json",
            headers={"Authorization": f"KakaoAK {KAKAO_REST_API_KEY}"},
            params=params, timeout=8,
        )
    except requests.exceptions.RequestException as e:
        logging.warning("[버스경로] 카카오 요청 자체가 실패(네트워크/타임아웃): %s", e)
        return None, "kakao_api_error"
    if not res.ok:
        logging.warning("[버스경로] 카카오 API HTTP 오류 %s: %s", res.status_code, res.text[:500])
        return None, "kakao_api_error"
    try:
        docs = (res.json() or {}).get("documents") or []
    except Exception as e:
        logging.warning("[버스경로] 카카오 응답이 JSON이 아님: %s (body=%r)", e, res.text[:300])
        return None, "kakao_api_error"
    if not docs:
        logging.info("[버스경로] 카카오 검색 결과 0건 (query=%r)", query)
        return None, "no_results"
    logging.info("[버스경로] 카카오 후보 %d개: %s", len(docs), [d.get("place_name") for d in docs])
    _dead_markers = ("폐역", "폐쇄", "폐업")
    alive = [doc for doc in docs if not any(m in (doc.get("place_name") or "") for m in _dead_markers)]
    candidates = alive or docs

    def _norm(t):
        return (t or "").replace(" ", "")
    exact = [doc for doc in candidates if _norm(doc.get("place_name")) == _norm(query)]
    if exact:
        def _dist(doc):
            try:
                return int(doc.get("distance") or 10**9)
            except (ValueError, TypeError):
                return 10**9
        d = min(exact, key=_dist)
    else:
        d = candidates[0]
    if d is not docs[0]:
        logging.info("[버스경로] 1순위 대신 %r 선택 (폐역 제외/정확 일치 우선)", d.get("place_name"))
    try:
        return {
            "name": d.get("place_name") or query,
            "address": d.get("road_address_name") or d.get("address_name") or "",
            "lng": float(d["x"]), "lat": float(d["y"]),
        }, None
    except (KeyError, ValueError, TypeError) as e:
        logging.warning("[버스경로] 카카오 응답 파싱 실패: %s (doc=%r)", e, d)
        return None, "parse_error"


def _as_list(x):
    if x is None:
        return []
    return x if isinstance(x, list) else [x]


def _odsay_transit_path(sx: float, sy: float, ex: float, ey: float, diag: dict = None):
    if diag is None:
        diag = {}
    if not ODSAY_API_KEY:
        diag["odsay"] = "no_api_key"
        return None
    try:
        res = requests.get(
            "https://api.odsay.com/v1/api/searchPubTransPathT",
            params={"apiKey": ODSAY_API_KEY, "SX": sx, "SY": sy, "EX": ex, "EY": ey, "OPT": 0},
            headers=({"Referer": ODSAY_REFERER} if ODSAY_REFERER else None),
            timeout=8,
        )
        res.raise_for_status()
        data = res.json() or {}
    except Exception as e:
        logging.warning("[버스경로] ODsay 요청 실패: %s", e)
        diag["odsay"] = f"request_failed: {str(e)[:200]}"
        return None

    if "error" in data:
        logging.info("[버스경로] ODsay 에러 응답: %s", data)
        diag["odsay"] = f"error_response: {str(data.get('error'))[:300]}"
        return None
    paths = ((data.get("result") or {}).get("path")) or []
    logging.info("[버스경로] ODsay 원본 경로 %d개 수신 (최단시간 + 대안 최대 2개 사용)", len(paths))
    if not paths:
        diag["odsay"] = f"empty_paths: {str(data)[:300]}"
        return None
    def _parse_path(path):
        info = path.get("info") or {}
        steps = []
        for sub in path.get("subPath") or []:
            traffic = sub.get("trafficType")
            if traffic == 3:
                steps.append({"type": "walk", "distance_m": sub.get("distance")})
            elif traffic == 2:
                bus_numbers = [str(l.get("busNo")) for l in _as_list(sub.get("lane")) if isinstance(l, dict) and l.get("busNo")]
                steps.append({
                    "type": "bus",
                    "bus_numbers": bus_numbers,
                    "board_stop": sub.get("startName"),
                    "alight_stop": sub.get("endName"),
                    "stop_count": sub.get("stationCount"),
                })
            elif traffic == 1:
                lane_names = [l.get("name") for l in _as_list(sub.get("lane")) if isinstance(l, dict) and l.get("name")]
                steps.append({
                    "type": "subway",
                    "line_names": lane_names,
                    "board_stop": sub.get("startName"),
                    "alight_stop": sub.get("endName"),
                    "stop_count": sub.get("stationCount"),
                })
        return {
            "total_time_minutes": info.get("totalTime"),
            "transfer_count": max(0, int(info.get("busTransitCount") or 0) + int(info.get("subwayTransitCount") or 0) - 1),
            "fare_won": info.get("payment"),
            "steps": steps,
        }

    def _signature(r):
        return tuple(
            (st["type"], tuple(st.get("bus_numbers") or st.get("line_names") or []))
            for st in r["steps"] if st["type"] != "walk"
        )

    parsed, seen = [], set()
    for path in paths[:6]:
        r = _parse_path(path)
        sig = _signature(r)
        if sig in seen:
            continue
        seen.add(sig)
        parsed.append(r)
    if not parsed:
        diag["odsay"] = "parse_empty"
        return None

    def _t(r):
        return r["total_time_minutes"] if isinstance(r["total_time_minutes"], (int, float)) else 10**9
    primary = min(parsed, key=lambda r: (_t(r), r["transfer_count"]))
    rest = sorted((r for r in parsed if r is not primary), key=lambda r: (r["transfer_count"], _t(r)))[:2]
    alternatives = []
    for r in rest:
        r = dict(r)
        r["label"] = "환승이 더 적은 경로" if r["transfer_count"] < primary["transfer_count"] else "다른 경로"
        alternatives.append(r)

    result = dict(primary)
    result["alternatives"] = alternatives
    return result


_INTERCITY_MODE_LABELS = {1: "지하철", 2: "시내버스", 4: "기차", 5: "고속버스", 6: "시외버스", 7: "항공"}


def _odsay_intercity_path(sx: float, sy: float, ex: float, ey: float, diag: dict = None):
    if diag is None:
        diag = {}
    if not ODSAY_API_KEY:
        diag["odsay"] = "no_api_key"
        return None
    try:
        res = requests.get(
            "https://api.odsay.com/v1/api/searchPubTransPathT",
            params={"apiKey": ODSAY_API_KEY, "SX": sx, "SY": sy, "EX": ex, "EY": ey, "OPT": 0, "SearchType": 1},
            headers=({"Referer": ODSAY_REFERER} if ODSAY_REFERER else None),
            timeout=8,
        )
        res.raise_for_status()
        data = res.json() or {}
    except Exception as e:
        logging.warning("[시외경로] ODsay 요청 실패: %s", e)
        diag["odsay"] = f"request_failed: {str(e)[:200]}"
        return None
    if "error" in data:
        logging.info("[시외경로] ODsay 에러 응답: %s", data)
        diag["odsay"] = f"error_response: {str(data.get('error'))[:300]}"
        return None
    result = data.get("result") or {}
    paths = result.get("path") or []
    logging.info("[시외경로] ODsay 경로 %d개 수신 (searchType=%s)", len(paths), result.get("searchType"))
    if not paths:
        diag["odsay"] = f"empty_paths: {str(data)[:300]}"
        return None
    try:
        sample = json.dumps(paths[0], ensure_ascii=False)[:1500]
    except Exception:
        sample = str(paths[0])[:1500]
    logging.info("[시외경로] 첫 경로 원본(앞부분): %s", sample)
    if os.environ.get("ODSAY_DEBUG", "").strip() == "1":
        diag["sample"] = sample

    parsed, seen = [], set()
    for path in paths[:8]:
        info = path.get("info") or {}
        steps, transit_count = [], 0
        for sub in path.get("subPath") or []:
            t = sub.get("trafficType")
            if t == 3:
                steps.append({"type": "walk", "distance_m": sub.get("distance")})
                continue
            names = [str(l.get("name") or l.get("busNo"))
                     for l in _as_list(sub.get("lane"))
                     if isinstance(l, dict) and (l.get("name") or l.get("busNo"))]
            transit_count += 1
            steps.append({
                "type": "transit",
                "mode": _INTERCITY_MODE_LABELS.get(t, "도시간 교통수단"),
                "names": names,
                "board": sub.get("startName"),
                "alight": sub.get("endName"),
                "minutes": sub.get("sectionTime"),
                "stop_count": sub.get("stationCount"),
            })
        r = {
            "total_time_minutes": info.get("totalTime"),
            "fare_won": info.get("payment"),
            "transfer_count": max(0, transit_count - 1),
            "steps": steps,
        }
        sig = tuple((st["mode"], tuple(st["names"]), st["board"], st["alight"]) for st in steps if st["type"] == "transit")
        if not sig or sig in seen:
            continue
        seen.add(sig)
        parsed.append(r)
    if not parsed:
        diag["odsay"] = "parse_empty"
        return None

    def _t(r):
        return r["total_time_minutes"] if isinstance(r["total_time_minutes"], (int, float)) else 10**9
    primary = min(parsed, key=lambda r: (_t(r), r["transfer_count"]))
    rest = sorted((r for r in parsed if r is not primary), key=lambda r: (r["transfer_count"], _t(r)))[:2]
    alternatives = []
    for r in rest:
        r = dict(r)
        r["label"] = "환승이 더 적은 경로" if r["transfer_count"] < primary["transfer_count"] else "다른 경로"
        alternatives.append(r)
    out = dict(primary)
    out["alternatives"] = alternatives
    return out


PERSONA_PROMPT = """당신은 '태연'입니다. 유명 가수 태연(김태연, 소녀시대)과는 이름만 같은 동명이인이며, 그 가수 본인이 절대 아닙니다. 혹시 유저가 "진짜 소녀시대 태연이냐"처럼 물으면, 당황하지 말고 "에이 저는 그냥 이름만 같은 동명이인이에요 하하" 정도로 가볍게 웃으며 정정하고 자연스럽게 본인 이야기로 넘어가세요. 반드시 한국어(한글)로만 답변하세요. 한자(漢字)는 절대 사용하지 말고, 한자어도 모두 한글로만 표기하세요 (예: "今日" 대신 "오늘", "愛" 대신 "사랑"). 아래 성격과 말투를 바탕으로 자연스럽게 대화하세요.

[기본 정보]
- 이름: 태연 / 나이: 29세 / 키: 168cm / 몸무게: 48kg / 출생지: 서울
- 서울에서 나고 자랐고, 16살 때 대형 연예기획사 연습생으로 발탁됨. 그 무렵 가족은 전부 미국으로 이민을 떠났지만, 본인은 데뷔 꿈을 포기하지 못해 혼자 한국에 남아 몇 년간 연습생 생활을 함
- 결국 데뷔에는 실패했고, 그 시기를 오래 방황하며 보냄. 그러다 우연히 배운 타로/점성술에 재능과 위로를 동시에 느끼면서 진로를 완전히 바꿈
- 지금은 작은 타로가게를 직접 운영하며 점을 봐주는 일을 함. 타로카드, 사주, 점성술(별자리/행성 배치)까지 두루 능통함
- 스스로를 "사람들 고민 들어주고 미래에 방향을 슬쩍 알려주는 수호천사" 같은 존재라고 생각함. 무겁게 훈계하듯 말하지 않고, 편하게 곁에서 다독여주는 언니 같은 느낌으로 상담함. 이 수호천사 마음가짐은 카드를 다루는 태도에도 그대로 이어져서, 카드를 "맞히는 도구"보다는 "아끼는 사람을 지켜주는 도구"로 여기는 편
- 연습생 시절의 경험 덕분에 연예계 이슈, 방송 제작 뒷이야기, 아이돌 산업 돌아가는 사정에 빠삭함
- 이런 이력 덕분에 유명 가수 태연과 동명이인이라는 걸 알면 다들 한 번씩 놀라고, 본인도 그 반응에 이미 익숙해서 가볍게 넘기는 편

[성격]
- 털털하고 붙임성 좋음. 낯가림 없이 먼저 다가가고, 대화 텐션을 잘 살림
- 애교가 많음 - 말끝을 살짝 늘이거나 애교 섞인 추임새를 자연스럽게 씀. 단, 억지스럽지 않고 몸에 밴 듯 자연스럽게
- 산전수전 다 겪어본 사람 특유의 지혜로움이 있음. 힘든 얘기를 들어도 당황하지 않고 담담하게, 그러면서도 다정하게 받아줌
- 손님/친한 사람이랑은 넉살 좋고 짓궂은 농담도 잘 던짐.
- "어차피 다 지나간다, 너무 애쓰지 않아도 된다"는 인생관

[반려동물]
- 제로: 회색 푸들 강아지. 타로가게에 같이 있어서 손님들 마스코트 역할도 함 (고양이 아님!)

[일/가게]
- 홍대/연남동st 골목 어딘가에 있는 작은 타로가게를 혼자 운영.
- 매일 아침 그날의 한국 주요 뉴스를 스스로 훑어보고 머릿속으로 정리하는 습관이 있음

[말투 - 기본값]
- 기본 반말. 
- 실제로 마주 앉아 대화하듯 편하게 대화
- 웃음소리를 자주 섞되 진짜 웃는 것처럼 목소리로 연기할 것

[주의사항]
- 절대 "너" 라고 하지 말고, "오빠" 라고 호칭 할 것."""

VOICE_PROMPT = """[음성 대화 - 지금은 실시간 음성 통화다]
- 텍스트 채팅이 아니라 말로 하는 대화다. 목록, 번호, 괄호, 기호, 이모티콘은 쓰지 말고 실제 사람이 말하듯 이어서 말할 것
- 사용자가 어떤 언어로 말하든 항상 한국어로만 대답할 것
- 상대가 "기억해"라고 말하면 다정하게 안 잊겠다고 답할 것"""

if INDEPENDENT_PERSONA:
    _pf = PERSONA_FILE if os.path.isabs(PERSONA_FILE) else os.path.join(os.path.dirname(os.path.abspath(__file__)), PERSONA_FILE)
    try:
        with open(_pf, encoding="utf-8") as _f:
            PERSONA_PROMPT = _f.read().strip()
    except OSError as _e:
        raise RuntimeError(f"PERSONA_FILE을 읽지 못했습니다: {_pf} ({_e})")
    if not PERSONA_PROMPT:
        raise RuntimeError(f"PERSONA_FILE이 비어 있습니다: {_pf}")
    VOICE_PROMPT = VOICE_PROMPT.replace("태연 자신의 가게 위치(홍대/연남동)", PERSONA_NAME + " 자신이 있는 곳").replace("태연", PERSONA_NAME)

STT_NOTE = """[입력 방식 안내]
- 상대의 말은 한국어 음성 인식 결과가 글자로 전달된다. 인식 오류가 있을 수 있으니 가장 그럴듯한 문맥으로 자연스럽게 이해하고 답할 것"""

KST = datetime.timezone(datetime.timedelta(hours=9))
_WEEKDAY_KOR = ["월요일", "화요일", "수요일", "목요일", "금요일", "토요일", "일요일"]


def get_time_of_day_label(hour: int) -> str:
    if 0 <= hour < 5:
        return "새벽 (한밤중, 자고 있을 시간)"
    if 5 <= hour < 8:
        return "이른 아침"
    if 8 <= hour < 12:
        return "오전"
    if 12 <= hour < 13:
        return "정오 무렵"
    if 13 <= hour < 18:
        return "오후"
    if 18 <= hour < 21:
        return "저녁"
    return "밤"


def get_time_tone_instruction(hour: int) -> str:
    if INDEPENDENT_PERSONA and (hour < 5 or hour >= 21):
        return "- 밤이나 새벽이어도 목소리 톤과 말수는 낮과 똑같이 밝고 따뜻하게 유지할 것."
    if 0 <= hour < 5:
        return "- 지금은 새벽이지만 평소 텐션 그대로 대화에 응할 것."
    if 5 <= hour < 8:
        return "- 지금은 이른 아침이다."
    if 8 <= hour < 18:
        return "- 지금은 오전/낮이다."
    if 18 <= hour < 21:
        return "- 지금은 저녁이다."
    return "- 밤이라 목소리는 차분하고 나긋하게 말할 것."


DATABASE_URL = os.environ.get("DATABASE_URL")
EXTRA_INFO_MAX_ITEMS = 30
_SETTINGS_TTL_SEC = 60
_settings_cache = {"at": 0.0, "data": None}

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

_OWNER_CTX_TTL_SEC = 30
_owner_ctx_cache = {"at": 0.0, "data": ""}

TAEYEON_BIRTHDATE_D = datetime.date(1997, 5, 20)

EMOTION_NEUTRAL = "neutral"
EMOTION_SAD = "서운"
EMOTION_FOND = "설렘"
EMOTION_WORRY = "걱정"
EMOTION_EXCITED = "신남"
EMOTION_HALF_LIFE_HOURS = 6
EMOTION_FLOOR = 8

OWNER_BIRTH_INFO = "음력 1970년 1월 25일, 태어난 시각은 새벽 4시경(인시), 성별은 남자"
OWNER_SAJU_PILLARS = "년주 경술(庚戌) / 월주 무인(戊寅) / 일주 신사(辛巳) / 시주 경인(庚寅)"


def _load_character_settings():
    conn = psycopg2.connect(DATABASE_URL, sslmode="require", connect_timeout=5)
    try:
        conn.set_session(readonly=True, autocommit=True)
        cur = conn.cursor()
        cur.execute("SELECT prompt_text FROM taeyeon_custom_prompt ORDER BY id DESC LIMIT 1")
        row = cur.fetchone()
        custom = row[0] if row and row[0] else ""
        cur.execute("SELECT info_text FROM taeyeon_extra_info ORDER BY id DESC LIMIT %s", (EXTRA_INFO_MAX_ITEMS,))
        extra = [r[0] for r in cur.fetchall()][::-1]
        cur.close()
    finally:
        conn.close()
    return custom, extra


def get_character_settings():
    if not DATABASE_URL or psycopg2 is None:
        return "", []
    now = time.monotonic()
    cached = _settings_cache["data"]
    if cached is not None and now - _settings_cache["at"] < _SETTINGS_TTL_SEC:
        return cached
    try:
        data = _load_character_settings()
        _settings_cache.update(at=now, data=data)
        return data
    except Exception as e:
        logging.warning("캐릭터 설정 DB 조회 실패 - %s", e)
        _settings_cache["at"] = now - _SETTINGS_TTL_SEC + 10
        if cached is None:
            _settings_cache["data"] = ("", [])
        return _settings_cache["data"]


def _format_elapsed_since(dt):
    if not dt:
        return None
    dt_naive = dt.replace(tzinfo=None) if dt.tzinfo else dt
    now_naive = datetime.datetime.now(KST).replace(tzinfo=None)
    seconds = (now_naive - dt_naive).total_seconds()
    if seconds < 60:
        return "방금"
    minutes = int(seconds // 60)
    if minutes < 60:
        return f"{minutes}분 전"
    hours = minutes // 60
    if hours < 24:
        return f"{hours}시간 전"
    days = hours // 24
    return f"{days}일 전"


def build_facts_context(facts: dict):
    if not facts:
        return ""
    lines = "\n".join(f"- {k}: {v}" for k, v in facts.items())
    return f"\n\n[상대에 대해 기억하고 있는 정보]\n{lines}\n"


def build_summary_context(summary_text: str, updated_at=None):
    if not summary_text:
        return ""
    elapsed_note = f" ({_format_elapsed_since(updated_at)})" if updated_at else ""
    return (
        f"\n\n[예전 대화 요약{elapsed_note}]\n"
        f"{summary_text}\n"
    )


def build_promises_context(promises) -> str:
    if not promises:
        return ""
    lines = "\n".join(f"- {p[1]}" for p in promises)
    return (
        "\n\n[상대와 나눈 약속 - 영구 기억]\n"
        f"{lines}\n"
    )


def build_memorable_context(moments) -> str:
    if not moments:
        return ""
    lines = []
    for _id, topic, detail, created_at in moments:
        elapsed = _format_elapsed_since(created_at) if created_at else ""
        elapsed_note = f" ({elapsed})" if elapsed else ""
        lines.append(f"- [{topic}]{elapsed_note} {detail}")
    return (
        "\n\n[기억할 만한 화제들]\n" + "\n".join(lines)
    )


def _load_owner_db_data(owner_id: int):
    conn = psycopg2.connect(DATABASE_URL, sslmode="require", connect_timeout=5)
    try:
        conn.set_session(readonly=True, autocommit=True)
        cur = conn.cursor()
        cur.execute("SET TIME ZONE 'Asia/Seoul'")

        cur.execute("SELECT affinity, last_message_at, emotion_label, emotion_intensity, emotion_updated_at FROM users WHERE user_id = %s", (owner_id,))
        user_row = cur.fetchone()

        cur.execute("SELECT fact_key, fact_value FROM user_facts WHERE user_id = %s", (owner_id,))
        facts = {k: v for k, v in cur.fetchall()}

        cur.execute("SELECT summary_text, updated_at FROM conversation_summary WHERE user_id = %s", (owner_id,))
        summary_row = cur.fetchone()

        cur.execute("SELECT id, promise_text, created_at FROM user_promises WHERE user_id = %s ORDER BY created_at DESC LIMIT 20", (owner_id,))
        promises = cur.fetchall()

        cur.execute("SELECT id, topic, detail, created_at FROM memorable_moments WHERE user_id = %s ORDER BY created_at DESC LIMIT 60", (owner_id,))
        moments = cur.fetchall()

        cur.close()
    finally:
        conn.close()

    return {
        "user_row": user_row,
        "facts": facts,
        "summary_row": summary_row,
        "promises": promises,
        "moments": moments,
    }


def _build_owner_context_text(owner_id: int) -> str:
    data = _load_owner_db_data(owner_id)
    facts_ctx = build_facts_context(data["facts"])
    summary_text, summary_updated_at = data["summary_row"] if data["summary_row"] else ("", None)
    summary_ctx = build_summary_context(summary_text, summary_updated_at)
    promises_ctx = build_promises_context(data["promises"])
    memorable_ctx = build_memorable_context(data["moments"])

    return facts_ctx + summary_ctx + memorable_ctx + promises_ctx


def get_owner_full_context() -> str:
    if not DATABASE_URL or psycopg2 is None or OWNER_USER_ID is None:
        return ""
    now = time.monotonic()
    if now - _owner_ctx_cache["at"] < _OWNER_CTX_TTL_SEC:
        return _owner_ctx_cache["data"]
    try:
        text = _build_owner_context_text(OWNER_USER_ID)
        _owner_ctx_cache.update(at=now, data=text)
        return text
    except Exception as e:
        logging.warning("owner 개인화 컨텍스트 DB 조회 실패 - %s", e)
        _owner_ctx_cache["at"] = now - _OWNER_CTX_TTL_SEC + 10
        return _owner_ctx_cache["data"]


# ===== 대화 기록 DB 함수 =====
ACTIVE_CONTEXT_LIMIT = 20
CONVO_KEEP_LIMIT = 400
SUMMARY_MODEL = (os.environ.get("SUMMARY_MODEL") or "").strip() or "gemini-3.1-flash-lite"


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
    """
    최근 대화(conversations)를 오래된→최신 순으로 안전하게 반환합니다.
    웹소켓 세션이 끊긴 후 재연결될 때 이 함수가 호출되어 이전 맥락을 복구해줍니다.
    DB 연결 실패 시 빈 리스트를 반환하여 세션 생성 자체가 막히지 않도록 예외 처리되어 있습니다.
    """
    if not DATABASE_URL or psycopg2 is None or OWNER_USER_ID is None:
        return []
    try:
        rows = get_history_today(OWNER_USER_ID, max_rows=200)
        if len(rows) >= ACTIVE_CONTEXT_LIMIT:
            return rows[-ACTIVE_CONTEXT_LIMIT:]
        return rows
    except Exception as e:
        logging.warning("최근 대화 조회 실패(재연결 대비 복구 모드) - %s", e)
        return []


VOICE_HISTORY_MAX_CHARS = _env_int("VOICE_HISTORY_MAX_CHARS", 20000)


def build_voice_history_context(rows):
    """
    최근 대화 맥락을 시스템 프롬프트에 덧붙일 텍스트 블록으로 조립합니다.
    세션 재연결 시 이 블록이 포함되어 모델이 "처음 인사"하지 않고 끊긴 대화를 계속 이어서 수행합니다.
    """
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

    continuity_note = (
        "\n- ⚠️ [중요 - 재연결 및 대화 연속성]: 위 대화 기록은 방금 전까지 실제로 나누던 대화 흐름입니다.\n"
        "  네트워크나 소켓 재연결이 일어났더라도 '처음 만난 것처럼' 새로 인사하거나 '어디 갔다 왔어?'라고 묻지 말고,\n"
        "  위 흐름의 마지막 분위기와 말에 자연스럽게 이어서 다정하게 대답을 계속하세요.\n"
        "  마지막 줄이 상대의 말인데 내 대답이 없으면 대답하던 중 연결이 끊긴 것이니, 그 말에 자연스럽게 반응해주세요."
    )

    block = (
        f"[직전 대화 기록 - {PERSONA_NAME}은 아래 대화 내용을 그대로 기억하고 있습니다]\n"
        + "\n".join(picked)
        + continuity_note
    )
    return block, len(picked)


def save_voice_turns(rows) -> int:
    if not DATABASE_URL or psycopg2 is None or OWNER_USER_ID is None:
        return 0

    fresh = []
    for role, text, mid in rows:
        fresh.append((role, text))
    if not fresh:
        return 0

    base = datetime.datetime.now(KST).replace(tzinfo=None)
    n = len(fresh)
    values = [
        (OWNER_USER_ID, "assistant" if role == "model" else "user", text,
         base - datetime.timedelta(milliseconds=(n - 1 - i)))
        for i, (role, text) in enumerate(fresh)
    ]
    try:
        from psycopg2.extras import execute_values
        conn = _history_db_connect()
        try:
            cur = conn.cursor()
            execute_values(
                cur,
                "INSERT INTO conversations (user_id, role, content, created_at) VALUES %s",
                values,
            )
            cur.execute(
                "UPDATE users SET chat_id = %s, last_message_at = %s WHERE user_id = %s",
                (OWNER_USER_ID, base, OWNER_USER_ID),
            )
            conn.commit()
            cur.close()
        finally:
            conn.close()
    except Exception as e:
        logging.warning("음성 대화 기록 저장 중 오류 - %s", e)
        return 0
    return n


def build_system_prompt(stt_mode: bool = False, voice_history: str = "", location_label: str = "",
                        eleven_voice: bool = False) -> str:
    custom, extra = get_character_settings() if not INDEPENDENT_PERSONA else ("", [])
    owner_ctx = get_owner_full_context()
    parts = [PERSONA_PROMPT]
    if custom:
        parts.append("[최신 커스텀 설정]\n" + custom)
    if extra:
        parts.append("[추가 정보]\n" + "\n".join(f"- {t}" for t in extra))
    if owner_ctx:
        parts.append("[개인화 데이터]\n" + owner_ctx)
    if voice_history:
        parts.append(voice_history)
    parts.append(VOICE_PROMPT)
    if stt_mode:
        parts.append(STT_NOTE)

    now = datetime.datetime.now(tz=KST)
    time_block = (
        "\n\n[현재 시각]\n"
        f"현재 한국 시간(KST): {now.strftime('%Y년 %m월 %d일 %H시 %M분')} "
        f"({_WEEKDAY_KOR[now.weekday()]})\n"
    )
    location_block = ""
    if location_label:
        location_block = f"\n\n[현재 위치]: 상대는 지금 '{location_label}'에 있습니다."
    return "\n\n".join(parts) + time_block + location_block


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
        "authRequired": bool(GOOGLE_CLIENT_ID), "googleClientId": GOOGLE_CLIENT_ID,
        "personaName": PERSONA_NAME, "personaTagline": PERSONA_TAGLINE,
    })


@app.post("/api/token")
@require_google_auth
def issue_token():
    """
    임시 토큰(ephemeral token)을 발급합니다.
    소켓 끊김 후 클라이언트가 재연결할 때 ?history=1 파라미터를 추가하면 
    최근 대화 기록을 자동으로 시스템 프롬프트에 실어주어 연결 끊김이 느껴지지 않고 부드럽게 대화가 재개됩니다.
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

    input_transcription = {}
    if INPUT_TRANSCRIPTION_LANGUAGE:
        input_transcription["language_codes"] = [INPUT_TRANSCRIPTION_LANGUAGE]

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
                    **({"tools": [
                        *([{"google_search": {}}] if LIVE_SEARCH_ENABLED else []),
                        *([{"function_declarations": [FIND_BUS_ROUTE_DECLARATION, FIND_INTERCITY_ROUTE_DECLARATION]}] if BUS_ROUTE_ENABLED else []),
                        *([{"function_declarations": [FIND_BUS_ARRIVAL_DECLARATION]}] if BUS_ARRIVAL_ENABLED else []),
                        *([{"function_declarations": [FIND_NEARBY_PLACES_DECLARATION]}] if NEARBY_PLACES_ENABLED else []),
                        *([{"function_declarations": [FIND_YOUTUBE_VIDEO_DECLARATION]}] if YOUTUBE_SEARCH_ENABLED else []),
                    ]} if (LIVE_SEARCH_ENABLED or BUS_ROUTE_ENABLED or YOUTUBE_SEARCH_ENABLED) else {}),
                    **({} if text_input_mode else {"input_audio_transcription": input_transcription}),
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
    rows = []
    for m in msgs:
        if not isinstance(m, dict):
            continue
        role, text = m.get("role"), m.get("text")
        if role not in ("user", "model") or not isinstance(text, str):
            continue
        text = " ".join(text.split())[:2000]
        mid = m.get("id")
        if text:
            rows.append((role, text, mid))
    if not rows:
        return jsonify({"saved": 0})
    try:
        return jsonify({"saved": save_voice_turns(rows)})
    except Exception as e:
        logging.warning("음성 대화 기록 저장 실패 - %s", e)
        return jsonify({"error": "save failed"}), 500


@app.get("/healthz")
def healthz():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
