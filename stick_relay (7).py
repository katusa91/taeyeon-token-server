"""
M5StickS3 <-> Gemini Live 중계 서버 (stick_relay.py)
호출어("이코") + 가족 목소리 식별 + 핸즈프리 대화

흐름:
  1) 스틱(수면 중)이 소리가 나면 짧은 클립을 서버로 보냄
  2) 서버가 클립의 목소리를 등록된 가족과 비교 (가족이 아니면 여기서 끝, Gemini 호출 없음)
  3) 가족이 맞으면 Gemini로 "이코"를 불렀는지 확인
  4) 맞으면 Live 대화 세션을 열고(누구인지 이름도 알려줌) "awake"를 보냄
  5) 스틱이 "sleep"을 보내면 세션 종료

목소리 등록: 브라우저에서  https://<도메인>/admin?token=<DEVICE_TOKEN>  을 열고
             이름을 입력해 "등록 시작"을 누른 뒤, 그 사람이 스틱에 대고 "이코"를 5번 말하면 됩니다.

스틱 -> 서버  TEXT "clip" / BINARY(16kHz PCM16)... / TEXT "clip_end", "wake", "sleep", BINARY(음성)
서버 -> 스틱  TEXT "awake" "nowake" "unknown" "turn_end" "closed" "enroll:<n>/<목표>" "enroll:retry", BINARY(24kHz 음성)

설치:   pip install aiohttp google-genai sherpa-onnx numpy
환경변수:
  GEMINI_API_KEY  (필수)
  DEVICE_TOKEN    (필수, 스틱 스케치의 TOKEN과 동일. 관리 페이지 접근에도 사용)
  LIVE_MODEL      (대화용 Live 모델, 기본 gemini-3.8-live)
  WAKE_MODEL      (호출어 확인용, 기본 gemini-3.5-flash-lite)
  WAKE_WORD       (기본 이코)
  VOICE_NAME      (기본 Aoede)
  PERSONA_FILE    (선택, "이코"로 부르는 봇의 성격. 없으면 기본 태연 말투)
  WAKE_WORD_2     (두 번째 봇의 호출어, 기본 보살님. 비우면 두 번째 봇 끔)
  PERSONA_FILE_2  (선택, 두 번째 봇(선녀보살)의 성격 파일. 없으면 기본 선녀보살 말투)
  VOICE_NAME_2    (두 번째 봇의 목소리, 기본 Charon)
  DATA_DIR        (등록한 목소리와 모델을 저장할 폴더. Railway에서는 Volume을 /data에 연결하고 /data로 지정)
  SPEAKER_ID      (on/off, 기본 on. off면 가족 확인 없이 누구나 호출 가능)
  SPEAKER_THRESHOLD (기본 0.45. 가족인데 자꾸 거절되면 낮추고, 낯선 사람이 통과하면 올리세요)
  WAKE_ENGINE     (gemini | porcupine, 기본 gemini)  porcupine이면 아래 3개 필요, 실패 시 gemini로 대체
  PICOVOICE_ACCESS_KEY    Picovoice 콘솔의 AccessKey
  PORCUPINE_KEYWORD_PATH  콘솔에서 받은 "이코" 키워드 파일(.ppn, 서버용 Linux x86_64)의 경로
  PORCUPINE_SENSITIVITY   0~1 (기본 0.6). 높이면 잘 깨지만 오작동이 늘어남
  PORT            (Railway가 자동 지정)
"""
import asyncio
import html
import io
import json
import os
import time
import urllib.request
import wave
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote

import numpy as np
from aiohttp import web, WSMsgType
from google import genai
from google.genai import types

API_KEY = os.environ["GEMINI_API_KEY"]
DEVICE_TOKEN = os.environ["DEVICE_TOKEN"].strip()
MODEL = os.environ.get("LIVE_MODEL", "gemini-3.8-live")
WAKE_MODEL = os.environ.get("WAKE_MODEL", "gemini-3.5-flash-lite")
WAKE_WORD = os.environ.get("WAKE_WORD", "이코")
WAKE_WORD_2 = os.environ.get("WAKE_WORD_2", "보살님").strip()  # 비우면 두 번째 봇 끔
VOICE = os.environ.get("VOICE_NAME", "Aoede")
VOICE_2 = os.environ.get("VOICE_NAME_2", "Charon")
PORT = int(os.environ.get("PORT", "8765"))

DATA_DIR = os.environ.get("DATA_DIR", ".")
SPEAKER_ID = os.environ.get("SPEAKER_ID", "on").lower() != "off"
SPK_THRESHOLD = float(os.environ.get("SPEAKER_THRESHOLD", "0.45"))
SPK_MODEL_URL = os.environ.get(
    "SPEAKER_MODEL_URL",
    "https://github.com/k2-fsa/sherpa-onnx/releases/download/"
    "speaker-recongition-models/wespeaker_en_voxceleb_resnet34.onnx",
)
VOICE_DB = os.path.join(DATA_DIR, "voices.json")
SPK_MODEL_PATH = os.path.join(DATA_DIR, os.path.basename(SPK_MODEL_URL))

WAKE_ENGINE = os.environ.get("WAKE_ENGINE", "gemini").lower()
PV_ACCESS_KEY = os.environ.get("PICOVOICE_ACCESS_KEY", "").strip()
PV_KEYWORD_PATH = os.environ.get("PORCUPINE_KEYWORD_PATH", "").strip()
PV_SENSITIVITY = float(os.environ.get("PORCUPINE_SENSITIVITY", "0.6"))
PV_MODEL_URL = ("https://raw.githubusercontent.com/Picovoice/porcupine/master/"
                "lib/common/porcupine_params_ko.pv")  # 한국어 언어 모델
PV_MODEL_PATH = os.path.join(DATA_DIR, "porcupine_params_ko.pv")

ENROLL_TARGET = 5       # 한 사람당 등록할 샘플 수
ENROLL_WINDOW_S = 300   # 등록 모드 유지 시간(5분)
ENROLL_MIN_SIM = 0.2    # 같은 사람 샘플끼리 이 값보다 낮으면 잡음/다른 소리로 보고 다시 요청
MAX_SAMPLES = 10

CHUNK_BYTES = 4800            # 24kHz * 2byte * 0.1초 (스틱 쪽과 동일)
CLIP_RATE = 16000
MAX_CLIP_BYTES = CLIP_RATE * 2 * 5
MIN_CLIP_BYTES = CLIP_RATE * 2 // 5

DEFAULT_PERSONA = (
    "너는 가수 태연처럼 다정하고 친근하게 말하는 음성 비서야. "
    "음성 대화이므로 대답은 한두 문장으로 짧고 자연스럽게 해."
)


DEFAULT_PERSONA_2 = (
    "너는 '선녀보살'이라는 이름의 다정하고 푸근한 점집 보살님이야. "
    "반말과 존댓말을 섞어 정겹게 말하고, 사주·운세·고민 상담을 구수하고 재치 있게 풀어 줘. "
    "재미로 보는 이야기라는 느낌을 살리고, 건강·돈·법률 같은 중요한 결정은 "
    "'그건 전문가랑 꼭 상의해 보렴'이라고 말해 줘. "
    "음성 대화이므로 대답은 한두 문장으로 짧고 자연스럽게 해."
)

# 봇 목록: 키 -> (호출어, 성격 파일 환경변수, 기본 성격, 목소리)
BOTS = {
    "iko": {"word": WAKE_WORD, "persona_env": "PERSONA_FILE",
            "default": DEFAULT_PERSONA, "voice": VOICE},
}
if WAKE_WORD_2:
    BOTS["bosal"] = {"word": WAKE_WORD_2, "persona_env": "PERSONA_FILE_2",
                     "default": DEFAULT_PERSONA_2, "voice": VOICE_2}


def load_persona(bot: str = "iko") -> str:
    b = BOTS.get(bot) or BOTS["iko"]
    path = os.environ.get(b["persona_env"])
    if path and os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return f.read()
    return b["default"]


def make_config(who: str | None = None, bot: str = "iko") -> types.LiveConnectConfig:
    persona = load_persona(bot)
    voice = (BOTS.get(bot) or BOTS["iko"])["voice"]
    if who:
        persona += f"\n\n[지금 대화하는 사람은 가족 구성원 '{who}'(이)야. 필요하면 이름을 불러 줘.]"
    return types.LiveConnectConfig(
        response_modalities=["AUDIO"],
        system_instruction=persona,
        speech_config=types.SpeechConfig(
            voice_config=types.VoiceConfig(
                prebuilt_voice_config=types.PrebuiltVoiceConfig(voice_name=voice)
            )
        ),
    )


client = genai.Client(api_key=API_KEY)
EXECUTOR = ThreadPoolExecutor(max_workers=1)  # 화자 인식은 CPU 작업이라 별도 스레드에서


def run_blocking(fn, *args):
    return asyncio.get_running_loop().run_in_executor(EXECUTOR, fn, *args)


# ===================== 화자(가족) 식별 =====================
def trim_silence(x: np.ndarray, frame: int = 320) -> np.ndarray:
    """앞뒤 무음을 잘라냄 (20ms 프레임 기준)."""
    n = len(x) // frame
    if n < 3:
        return x
    rms = np.sqrt((x[: n * frame].reshape(n, frame) ** 2).mean(axis=1))
    thr = max(0.15 * float(rms.max()), 1e-4)
    idx = np.where(rms > thr)[0]
    if len(idx) == 0:
        return x
    a = max(int(idx[0]) - 3, 0)
    b = min(int(idx[-1]) + 4, n)
    return x[a * frame : b * frame]


class Speakers:
    def __init__(self):
        self.extractor = None
        self.voices: dict[str, list[np.ndarray]] = {}
        os.makedirs(DATA_DIR, exist_ok=True)
        self.load_db()

    # --- 저장/불러오기 ---
    def load_db(self):
        try:
            if os.path.exists(VOICE_DB):
                with open(VOICE_DB, encoding="utf-8") as f:
                    raw = json.load(f)
                self.voices = {
                    n: [np.array(e, dtype=np.float32) for e in embs] for n, embs in raw.items()
                }
                print("voices loaded:", {n: len(v) for n, v in self.voices.items()})
        except Exception as e:
            print("voice db load error:", e)

    def save_db(self):
        tmp = VOICE_DB + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(
                {n: [e.tolist() for e in embs] for n, embs in self.voices.items()},
                f, ensure_ascii=False,
            )
        os.replace(tmp, VOICE_DB)

    # --- 엔진 ---
    def init_engine(self):
        if not SPEAKER_ID:
            print("speaker id: OFF")
            return
        try:
            import sherpa_onnx

            if not os.path.exists(SPK_MODEL_PATH):
                print("downloading speaker model...")
                tmp = SPK_MODEL_PATH + ".part"
                urllib.request.urlretrieve(SPK_MODEL_URL, tmp)
                os.replace(tmp, SPK_MODEL_PATH)
            cfg = sherpa_onnx.SpeakerEmbeddingExtractorConfig(
                model=SPK_MODEL_PATH, num_threads=1, debug=False, provider="cpu"
            )
            if not cfg.validate():
                raise RuntimeError("invalid speaker model config")
            self.extractor = sherpa_onnx.SpeakerEmbeddingExtractor(cfg)
            print("speaker engine ready, dim =", self.extractor.dim)
        except Exception as e:
            self.extractor = None
            print("SPEAKER ENGINE UNAVAILABLE:", e)

    def embed(self, pcm: bytes):
        """PCM16(16kHz mono) -> 정규화된 목소리 특징 벡터. 너무 짧으면 None."""
        x = np.frombuffer(pcm, dtype=np.int16).astype(np.float32) / 32768.0
        x = trim_silence(x)
        if len(x) < CLIP_RATE * 4 // 10:  # 0.4초 미만
            return None
        s = self.extractor.create_stream()
        s.accept_waveform(sample_rate=CLIP_RATE, waveform=x)
        s.input_finished()
        e = np.array(self.extractor.compute(s), dtype=np.float32)
        n = float(np.linalg.norm(e))
        return e / n if n > 0 else None

    # --- 식별 ---
    def identify(self, emb) -> tuple[str | None, float]:
        best, best_s = None, -1.0
        for name, embs in self.voices.items():
            scores = sorted((float(np.dot(emb, e)) for e in embs), reverse=True)
            if not scores:
                continue
            k = min(2, len(scores))  # 상위 2개 샘플 평균
            s = sum(scores[:k]) / k
            if s > best_s:
                best, best_s = name, s
        return best, best_s

    def add_sample(self, name: str, emb) -> int:
        lst = self.voices.setdefault(name, [])
        lst.append(emb)
        if len(lst) > MAX_SAMPLES:
            del lst[0]
        self.save_db()
        return len(lst)

    def remove(self, name: str):
        if self.voices.pop(name, None) is not None:
            self.save_db()


class WakeWord:
    """Picovoice Porcupine 호출어 엔진 (선택). 설정이 없거나 실패하면 Gemini 방식으로 대체."""

    def __init__(self):
        self.handle = None
        self.error = ""

    @property
    def ready(self) -> bool:
        return self.handle is not None

    def init_engine(self):
        if WAKE_ENGINE != "porcupine":
            return
        try:
            import pvporcupine

            if not PV_ACCESS_KEY:
                raise RuntimeError("PICOVOICE_ACCESS_KEY 가 설정되지 않았습니다")
            if not (PV_KEYWORD_PATH and os.path.exists(PV_KEYWORD_PATH)):
                raise RuntimeError(f"키워드 파일을 찾을 수 없습니다: {PV_KEYWORD_PATH!r}")
            if not os.path.exists(PV_MODEL_PATH):
                print("downloading porcupine korean model...")
                tmp = PV_MODEL_PATH + ".part"
                urllib.request.urlretrieve(PV_MODEL_URL, tmp)
                os.replace(tmp, PV_MODEL_PATH)
            self.handle = pvporcupine.create(
                access_key=PV_ACCESS_KEY,
                keyword_paths=[PV_KEYWORD_PATH],
                model_path=PV_MODEL_PATH,
                sensitivities=[PV_SENSITIVITY],
            )
            print("porcupine ready: frame", self.handle.frame_length,
                  "rate", self.handle.sample_rate)
        except Exception as e:
            self.handle = None
            self.error = str(e)
            print("PORCUPINE UNAVAILABLE -> using Gemini wake check:", e)

    def detect(self, pcm: bytes) -> bool:
        """16kHz PCM16 클립 안에 호출어가 있으면 True."""
        x = np.frombuffer(pcm, dtype=np.int16)
        n = self.handle.frame_length
        pad = np.zeros(n * 8, dtype=np.int16)  # 앞뒤 무음: 이전 클립의 잔여 상태를 비우고 끝 판정을 확정
        x = np.concatenate([pad, x, pad[: n * 4]])
        for i in range(0, len(x) - n + 1, n):
            if self.handle.process(x[i : i + n].tolist()) >= 0:
                return True
        return False


spk = Speakers()
porc = WakeWord()
DEVICES: dict = {}  # 연결된 스틱 {ws: stop_live 함수} - 등록 시작 시 대화 중인 스틱을 재우기 위해
ENROLL = {"name": None, "count": 0, "until": 0.0, "fresh": True}


def enroll_active() -> bool:
    return ENROLL["name"] is not None and time.time() < ENROLL["until"]


# ===================== 호출어 확인 (Gemini) =====================
def pcm16_to_wav(pcm: bytes, rate: int = CLIP_RATE) -> bytes:
    buf = io.BytesIO()
    with wave.open(buf, "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(rate)
        w.writeframes(pcm)
    return buf.getvalue()


async def check_wake(pcm: bytes, candidates: list[str] | None = None) -> str | None:
    """어떤 봇의 호출어를 불렀는지 판단해 봇 키("iko"/"bosal")를 돌려준다. 아니면 None."""
    keys = [k for k in (candidates or list(BOTS)) if k in BOTS]
    if not keys:
        return None
    lines = "\n".join(f"- {k.upper()}: '{BOTS[k]['word']}'" for k in keys)
    names = ", ".join(k.upper() for k in keys) + ", NO"
    prompt = (
        "이 오디오는 방 안에서 녹음된 짧은 소리입니다. 화자가 아래 호출어 중 하나를 "
        "직접 불러서 AI 비서를 호출했는지 판단하세요.\n"
        f"{lines}\n"
        "호출어를 직접 부른 경우만 해당합니다. 일반 대화, TV·음악 소리, "
        "다른 단어 속에 우연히 비슷한 소리가 섞인 경우는 NO입니다. "
        f"{names} 중 한 단어로만 답하세요."
    )
    try:
        resp = await client.aio.models.generate_content(
            model=WAKE_MODEL,
            contents=[
                types.Part.from_bytes(data=pcm16_to_wav(pcm), mime_type="audio/wav"),
                prompt,
            ],
            config=types.GenerateContentConfig(temperature=0),
        )
        answer = (resp.text or "").strip().upper()
        print("wake answer:", answer)
        for k in keys:
            if answer.startswith(k.upper()):
                return k
        return None
    except Exception as e:
        print("wake check error:", e)
        return None


# ===================== WebSocket (스틱) =====================
async def ws_handler(request: web.Request) -> web.WebSocketResponse:
    if request.query.get("token") != DEVICE_TOKEN:
        print("rejected: token mismatch from", request.remote)
        raise web.HTTPUnauthorized(text="unauthorized")

    ws = web.WebSocketResponse(max_msg_size=0, protocols=("arduino",))
    await ws.prepare(request)
    print("device connected")

    tasks: set = set()
    state = {"q": None, "checking": False}

    def spawn(coro):
        t = asyncio.create_task(coro)
        tasks.add(t)
        t.add_done_callback(tasks.discard)
        return t

    async def say(text: str):
        if not ws.closed:
            await ws.send_str(text)

    def live_running() -> bool:
        return state["q"] is not None

    async def run_live(q: asyncio.Queue, config):
        try:
            async with client.aio.live.connect(model=MODEL, config=config) as session:
                print("live session started: model =", MODEL)

                async def gemini_to_device():
                    buf = bytearray()
                    while True:
                        async for resp in session.receive():
                            if resp.data:
                                buf.extend(resp.data)
                                while len(buf) >= CHUNK_BYTES:
                                    await ws.send_bytes(bytes(buf[:CHUNK_BYTES]))
                                    del buf[:CHUNK_BYTES]
                            sc = resp.server_content
                            if sc and sc.turn_complete:
                                if len(buf) % 2:
                                    buf = buf[:-1]
                                if buf:
                                    await ws.send_bytes(bytes(buf))
                                buf.clear()
                                await ws.send_str("turn_end")

                async def device_to_gemini():
                    while True:
                        data = await q.get()
                        if data is None:
                            return
                        await session.send_realtime_input(
                            audio=types.Blob(data=data, mime_type="audio/pcm;rate=16000")
                        )

                t1 = asyncio.create_task(gemini_to_device())
                t2 = asyncio.create_task(device_to_gemini())
                try:
                    done, pending = await asyncio.wait(
                        {t1, t2}, return_when=asyncio.FIRST_COMPLETED
                    )
                finally:
                    # 중간에 취소돼도 두 작업이 남아 "never retrieved" 오류를 내지 않게 정리
                    for t in (t1, t2):
                        if not t.done():
                            t.cancel()
                    await asyncio.gather(t1, t2, return_exceptions=True)
                for t in done:
                    if not t.cancelled() and t.exception():
                        print("error:", t.exception())
                    elif t is t2:
                        print("live end reason: audio queue closed (sleep/disconnect)")
                    elif t is t1:
                        print("live end reason: gemini receive loop stopped")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print("session error:", e)
        finally:
            print("live session ended")
            if state["q"] is q:
                state["q"] = None
                try:
                    await say("closed")
                except Exception:
                    pass

    def start_live(who: str | None = None, bot: str = "iko"):
        if live_running():
            return
        q: asyncio.Queue = asyncio.Queue()
        state["q"] = q  # 연결되는 동안 들어오는 음성은 큐에 쌓임
        print("starting bot:", bot)
        spawn(run_live(q, make_config(who, bot)))

    def stop_live() -> bool:
        q = state["q"]
        if q is None:
            return False
        state["q"] = None
        q.put_nowait(None)
        return True

    DEVICES[ws] = stop_live

    async def handle_enroll_clip(pcm: bytes):
        name = ENROLL["name"]
        emb = await run_blocking(spk.embed, pcm) if spk.extractor is not None else None
        if emb is None:
            await say("enroll:retry")
            return
        existing = [] if ENROLL["fresh"] else spk.voices.get(name, [])
        if len(existing) >= 2:  # 앞서 등록한 샘플과 너무 다르면(잡음 등) 다시 요청
            sim = max(float(np.dot(emb, e)) for e in existing)
            if sim < ENROLL_MIN_SIM:
                print(f"enroll: rejected outlier for {name} (sim {sim:.2f})")
                await say("enroll:retry")
                return
        if ENROLL["fresh"]:  # 같은 이름으로 다시 등록하면 기존 샘플은 교체
            spk.voices[name] = []
            ENROLL["fresh"] = False
        spk.add_sample(name, emb)
        ENROLL["count"] += 1
        print(f"enroll: {name} {ENROLL['count']}/{ENROLL_TARGET}")
        await say(f"enroll:{ENROLL['count']}/{ENROLL_TARGET}")
        if ENROLL["count"] >= ENROLL_TARGET:
            ENROLL["name"] = None

    async def do_wake_check(pcm: bytes):
        state["checking"] = True
        who = None
        bot = None
        try:
            # 0) 등록 모드: 이 클립을 가족 목소리 샘플로 저장
            if enroll_active():
                await handle_enroll_clip(pcm)
                return

            # 1) Porcupine이 있으면 호출어부터 확인 (가볍고 빠름, API 비용 없음)
            use_pv = porc.ready
            pv_heard = False
            if use_pv:
                pv_heard = await run_blocking(porc.detect, pcm)
                print("porcupine:", "DETECTED" if pv_heard else "no wake word")
                # Porcupine은 첫 번째 봇("이코") 전용이라, 못 들었으면 다른 봇 호출어만 Gemini로 확인
                if not pv_heard and len(BOTS) == 1:
                    await say("nowake")
                    return

            # 2) 가족 목소리인지 확인 (등록된 사람이 있을 때만)
            if SPEAKER_ID and spk.voices:
                if spk.extractor is None:
                    print("speaker engine unavailable -> skipping family check")
                else:
                    emb = await run_blocking(spk.embed, pcm)
                    if emb is None:
                        await say("nowake")
                        return
                    name, score = spk.identify(emb)
                    ok_spk = score >= SPK_THRESHOLD
                    print(f"speaker: best={name} score={score:.2f} "
                          f"(threshold {SPK_THRESHOLD}) -> {'family' if ok_spk else 'unknown'}")
                    if not ok_spk:
                        await say("unknown")
                        return
                    who = name

            # 3) Porcupine이 없으면 Gemini로 호출어 확인 (Porcupine이면 이미 확인됨)
            if pv_heard:
                bot = "iko"
            elif use_pv:
                bot = await check_wake(pcm, [k for k in BOTS if k != "iko"])
            else:
                bot = await check_wake(pcm)
        finally:
            state["checking"] = False
        ok = bot is not None
        print("wake check:", f"YES ({bot})" if ok else "NO", "| who:", who)
        if ws.closed:
            return
        if ok:
            if not live_running():
                start_live(who, bot)  # 큐를 먼저 만들고
                await say("awake")  # 그다음 알림 (이후 음성이 유실되지 않음)
        else:
            await say("nowake")

    clip = bytearray()
    in_clip = False
    try:
        async for msg in ws:
            if msg.type == WSMsgType.BINARY:
                if live_running():
                    state["q"].put_nowait(msg.data)
                elif in_clip and len(clip) < MAX_CLIP_BYTES:
                    clip.extend(msg.data)
            elif msg.type == WSMsgType.TEXT:
                cmd = msg.data
                if cmd == "clip":
                    clip.clear()
                    in_clip = True
                elif cmd == "clip_end":
                    in_clip = False
                    data = bytes(clip)
                    clip.clear()
                    if live_running() or state["checking"] or len(data) < MIN_CLIP_BYTES:
                        await say("nowake")
                    else:
                        spawn(do_wake_check(data))
                elif cmd == "wake":  # 스틱 버튼: 물리적으로 누른 것이므로 가족 확인 없이 허용
                    if not live_running():
                        start_live()
                    await say("awake")
                elif cmd.startswith("log:"):  # 스틱이 보내는 디버그 메시지
                    print("device log:", cmd[4:])
                elif cmd == "sleep":
                    print("device sent: sleep")
                    stop_live()
    except Exception as e:
        print("handler error:", e)
    finally:
        print("device disconnected")
        DEVICES.pop(ws, None)
        state["q"] = None
        for t in list(tasks):
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if not ws.closed:
            await ws.close()
    return ws


# ===================== 관리 페이지 (목소리 등록) =====================
def require_token(request: web.Request) -> str:
    token = request.query.get("token", "")
    if token != DEVICE_TOKEN:
        raise web.HTTPUnauthorized(text="unauthorized")
    return token


ADMIN_HTML = """<!doctype html><html lang="ko"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">%REFRESH%
<title>이코 목소리 등록</title>
<style>
body{font-family:-apple-system,"Malgun Gothic",sans-serif;max-width:480px;margin:2em auto;padding:0 1em;line-height:1.5}
h1{font-size:1.3em} .box{background:#f3f4f6;border-radius:10px;padding:1em;margin:1em 0}
.on{background:#fff4d6} input[type=text]{padding:.6em;width:60%;font-size:1em}
button{padding:.6em 1em;font-size:1em} li{margin:.4em 0} a{color:#c0392b;margin-left:.5em}
small{color:#666}
@media(prefers-color-scheme:dark){body{background:#111;color:#eee}.box{background:#222}.on{background:#4a3b10}}
</style></head><body>
<h1>이코 목소리 등록</h1>
<div class="box %ONCLASS%">%STATUS%</div>
<div class="box"><b>등록된 가족</b><ul>%ROWS%</ul>%MSG%</div>
<div class="box"><form action="/enroll" method="get">
<input type="hidden" name="token" value="%TOKEN%">
<input type="text" name="name" placeholder="이름 (예: 엄마)" maxlength="20" required>
<button type="submit">등록 시작</button></form>
<small>누른 뒤 5분 안에, 그 사람이 스틱 가까이에서 "이코"를 %TARGET%번 말해 주세요.
조용한 곳에서 평소 부르는 톤으로, 조금씩 다르게 불러 주면 인식이 더 잘 됩니다.
스틱 화면에 Saved 1/%TARGET% 처럼 표시됩니다.</small></div>
<div class="box"><small>호출어 엔진: %WAKE%<br>화자 인식 엔진: %ENGINE% · 판정 기준 %THR%</small></div>
</body></html>"""


def render_admin(token: str, msg: str = "") -> str:
    t = quote(token, safe="")
    if not SPEAKER_ID:
        engine = "꺼짐 (SPEAKER_ID=off)"
    elif spk.extractor is None:
        engine = "사용 불가 (서버 로그를 확인하세요. 이 상태에서는 가족 확인 없이 누구나 호출됩니다)"
    else:
        engine = "정상"
    if porc.ready:
        wake_status = "Porcupine (정상)"
    elif WAKE_ENGINE == "porcupine":
        wake_status = "Gemini로 대체 중 - Porcupine 사용 불가: " + html.escape(porc.error)
    else:
        wake_status = "Gemini (WAKE_ENGINE=porcupine 으로 바꾸면 전용 엔진 사용)"
    rows = "".join(
        f'<li><b>{html.escape(n)}</b> · 샘플 {len(v)}개'
        f'<a href="/forget?token={t}&name={quote(n)}" '
        f'onclick="return confirm(\'삭제할까요?\')">삭제</a></li>'
        for n, v in spk.voices.items()
    ) or "<li><small>아직 없음 (등록 전에는 누구 목소리든 호출 가능)</small></li>"
    if enroll_active():
        left = int(ENROLL["until"] - time.time())
        status = (f"<b>'{html.escape(ENROLL['name'])}' 님 등록 중</b> "
                  f"{ENROLL['count']}/{ENROLL_TARGET}<br>스틱에 대고 \"이코\"라고 말해 주세요. "
                  f"(남은 시간 {left}초)")
    else:
        status = "대기 중 - 아래에서 등록할 사람의 이름을 입력하세요."
    return (ADMIN_HTML
            .replace("%REFRESH%", '<meta http-equiv="refresh" content="4">' if enroll_active() else "")
            .replace("%ONCLASS%", "on" if enroll_active() else "")
            .replace("%STATUS%", status).replace("%ROWS%", rows)
            .replace("%MSG%", f"<p>{html.escape(msg)}</p>" if msg else "")
            .replace("%TOKEN%", html.escape(token)).replace("%TARGET%", str(ENROLL_TARGET))
            .replace("%WAKE%", wake_status).replace("%ENGINE%", engine).replace("%THR%", str(SPK_THRESHOLD)))


async def admin_page(request: web.Request) -> web.Response:
    token = require_token(request)
    return web.Response(text=render_admin(token, request.query.get("msg", "")),
                        content_type="text/html")


async def enroll_start(request: web.Request) -> web.Response:
    token = require_token(request)
    name = request.query.get("name", "").strip()[:20]
    if not name:
        raise web.HTTPFound(f"/admin?token={quote(token, safe='')}&msg={quote('이름을 입력하세요')}")
    ENROLL.update(name=name, count=0, until=time.time() + ENROLL_WINDOW_S, fresh=True)
    print("enroll started:", name)
    # 대화 중인 스틱은 재워야 "이코" 클립이 등록용으로 전달됨
    for dev_ws, stop in list(DEVICES.items()):
        if stop():
            try:
                await dev_ws.send_str("closed")
            except Exception:
                pass
    raise web.HTTPFound(f"/admin?token={quote(token, safe='')}")


async def forget(request: web.Request) -> web.Response:
    token = require_token(request)
    name = request.query.get("name", "")
    spk.remove(name)
    print("forgot:", name)
    raise web.HTTPFound(f"/admin?token={quote(token, safe='')}&msg={quote(name + ' 삭제됨')}")


async def health(request: web.Request) -> web.Response:
    return web.Response(text="ok")


async def on_startup(app: web.Application):
    # 모델 다운로드/로딩은 백그라운드에서 (서버는 바로 시작)
    def init_all():
        spk.init_engine()
        porc.init_engine()

    app["init"] = asyncio.ensure_future(run_blocking(init_all))


app = web.Application()
app.on_startup.append(on_startup)
app.router.add_get("/", health)
app.router.add_get("/ws", ws_handler)
app.router.add_get("/admin", admin_page)
app.router.add_get("/enroll", enroll_start)
app.router.add_get("/forget", forget)

if __name__ == "__main__":
    print(f"models: live={MODEL} wake={WAKE_MODEL} bots={list(BOTS)} voice={VOICE}/{VOICE_2}")
    web.run_app(app, host="0.0.0.0", port=PORT)
