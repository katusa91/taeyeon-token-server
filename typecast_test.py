#!/usr/bin/env python3
"""타입캐스트 보이스 고르기 / 한 줄 합성 테스트 - 서버 배포 없이 내 PC에서 실행

준비:  pip install requests
       Windows(cmd):  set TYPECAST_API_KEY=발급받은키
       Mac/Linux   :  export TYPECAST_API_KEY=발급받은키
       (키 발급: studio.typecast.ai/developers/api)

사용:
  python typecast_test.py voices                         # 보이스 목록 (voice_id, 이름, 성별, 나이, 감정, 미리듣기 주소)
  python typecast_test.py voices --gender female --age young_adult
  python typecast_test.py say  tc_xxxxxxxx "아 진짜? 그걸 지금 말해주면 어떡해" --prev "나 사실 어제 말하려고 했는데"
  python typecast_test.py demo tc_xxxxxxxx               # 감정이 다른 대사 4줄을 이어서 합성 (일레븐랩스와 같은 문장으로 비교용)
  python typecast_test.py say tc_xxxxxxxx "작게 말하는 대사" --emotion whisper --volume 150    # 음량 올리기
  python typecast_test.py say tc_xxxxxxxx "작게 말하는 대사" --emotion whisper --lufs -14      # 목표 음량으로 맞추기
결과는 taeyeon_test_*.wav 로 저장됩니다.
"""
import argparse
import os
import sys

import requests

BASE = "https://api.typecast.ai"
KEY = os.environ.get("TYPECAST_API_KEY", "").strip()
# 연동 출처 표시 (고정 값 유지)
UA = "typecast-direct/1 python-requests typecast-integration/1 (source=api-page; generated_by=claude)"

# 같은 문장을 일레븐랩스/Gemini TTS에도 넣어서 비교하세요 (웃음, 서운함, 장난, 숫자)
DEMO_LINES = [
    "아 진짜? 그걸 지금 말해주면 어떡해, 나 완전 웃겼잖아.",
    "근데 말이야, 사실 어제 좀 서운했어. 나한테 먼저 얘기해줬으면 했거든.",
    "에이, 장난이야 장난. 그렇게 진지하게 받아들이면 내가 더 미안해지잖아.",
    "내일 오후 3시 30분에 예약 잡아뒀고, 비용은 이만 오천 원이에요.",
]


def need_key():
    if not KEY:
        sys.exit("TYPECAST_API_KEY 환경변수를 먼저 설정하세요.")


def list_voices(args):
    need_key()
    params = {"model": args.model, "voice_type": "original"}
    if args.gender:
        params["gender"] = args.gender
    if args.age:
        params["age"] = args.age
    r = requests.get(f"{BASE}/v3/voices", headers={"X-API-KEY": KEY, "User-Agent": UA}, params=params, timeout=30)
    if r.status_code != 200:
        sys.exit(f"HTTP {r.status_code}: {r.text[:300]}")
    voices = r.json()
    print(f"{len(voices)}개\n")
    for v in voices:
        emos = sorted({e for m in v.get("models", []) for e in m.get("emotions", [])})
        name = (v.get("voice_name") or {}).get("kor") or (v.get("voice_name") or {}).get("eng") or "?"
        print(f"{v['voice_id']}  {name}  {v.get('gender') or '-'}/{v.get('age') or '-'}  "
              f"용도:{','.join(v.get('use_cases') or []) or '-'}  감정:{','.join(emos) or '-'}")
        if v.get("preview_url"):
            print(f"    미리듣기: {v['preview_url']}")


def synth(voice_id, text, prev, args):
    prompt = {"emotion_type": "smart"}
    if args.emotion != "smart":
        prompt = {"emotion_type": "preset", "emotion_preset": args.emotion, "emotion_intensity": args.intensity}
    elif prev:
        prompt["previous_text"] = prev
    body = {
        "voice_id": voice_id, "text": text, "model": args.model, "language": "kor", "prompt": prompt,
        "output": {"audio_format": "wav", "audio_tempo": args.tempo},
    }
    if args.lufs is not None:          # volume과 target_lufs는 동시에 쓸 수 없음
        body["output"]["target_lufs"] = args.lufs
    elif args.volume != 100:
        body["output"]["volume"] = args.volume
    r = requests.post(f"{BASE}/v1/text-to-speech", headers={"X-API-KEY": KEY, "Content-Type": "application/json", "User-Agent": UA},
                      json=body, timeout=60)
    if r.status_code != 200:
        hint = {401: "API 키 확인", 402: "크레딧 부족", 422: "파라미터 오류(보이스가 그 감정/모델을 지원하는지 확인)",
                429: "요청이 너무 잦음"}.get(r.status_code, "")
        sys.exit(f"HTTP {r.status_code} {hint}: {r.text[:300]}")
    return r.content


def say(args):
    need_key()
    data = synth(args.voice_id, args.text, args.prev, args)
    open(args.out, "wb").write(data)
    print(f"저장: {args.out} ({len(data) // 1024}KB)")


def demo(args):
    need_key()
    prev = ""
    for i, line in enumerate(DEMO_LINES, 1):
        data = synth(args.voice_id, line, prev, args)
        out = f"taeyeon_test_{i}.wav"
        open(out, "wb").write(data)
        print(f"저장: {out}  <- {line}")
        prev = line      # 직전 대사를 다음 줄의 감정 추론 맥락으로 넘김 (서버가 하는 것과 같은 방식)


def main():
    ap = argparse.ArgumentParser(description="타입캐스트 보이스/합성 테스트")
    ap.add_argument("--model", default="ssfm-v31")
    sub = ap.add_subparsers(dest="cmd", required=True)
    pv = sub.add_parser("voices"); pv.add_argument("--gender", choices=["male", "female"]); pv.add_argument("--age")
    pv.set_defaults(fn=list_voices)
    for name, fn in (("say", say), ("demo", demo)):
        p = sub.add_parser(name)
        p.add_argument("voice_id")
        if name == "say":
            p.add_argument("text"); p.add_argument("--prev", default=""); p.add_argument("--out", default="taeyeon_test.wav")
        p.add_argument("--emotion", default="smart", help="smart(기본) 또는 normal/happy/sad/angry/whisper/toneup/tonedown")
        p.add_argument("--intensity", type=float, default=1.0)
        p.add_argument("--tempo", type=float, default=1.0)
        p.add_argument("--volume", type=int, default=100, help="음량 0~200 (기본 100). whisper는 140~160으로 올려보기")
        p.add_argument("--lufs", type=float, default=None, help="목표 음량 -70~0 (권장 -14). 주면 --volume은 무시")
        p.set_defaults(fn=fn)
    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
