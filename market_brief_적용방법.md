# market_brief.py 적용 방법 (token_server.py 수정 4곳 + 크론 1개)

## 0. 준비
- requirements.txt에 추가: `yfinance`, `pandas`, (권장) `defusedxml`
- market_brief.py를 token_server.py와 같은 폴더에 둔다.
- Railway(음성 서비스) 환경변수: `NAVER_CLIENT_ID`, `NAVER_CLIENT_SECRET`, `ECON_WATCHLIST`(예: 삼성전자,엔비디아,비트코인,이더리움)
  선택: `NEWS_FEEDS`(비우면 코드의 기본 9개 피드 사용), `BLOG_FEEDS`(기본값 없음, 아래 '블로거 정하기' 참고), `ASSET_ALIASES`
- 상세 리포트 메일용: `REPORT_TO_EMAIL`(받을 주소)과 `SMTP_PASSWORD`(Gmail이면 앱 비밀번호) 두 개면 된다. 보내는 계정은 기본적으로 받는 주소와 같은 Gmail을 쓴다(다른 계정으로 보내려면 `SMTP_USER` 추가, 필요시 `SMTP_HOST`/`SMTP_PORT`/`SMTP_FROM`).
  Gmail 앱 비밀번호: 구글 계정에서 2단계 인증을 켠 뒤 '앱 비밀번호'를 발급받아 16자리를 넣는다(평소 로그인 비밀번호가 아님).
  받는 주소는 이 환경변수 값으로만 고정된다(모델이나 브라우저가 준 주소로는 보내지 않음).
  ⚠️ 호스팅 요금제에 따라 SMTP 외부 연결이 막혀 있을 수 있다. 발송이 안 되면 로그의 '리포트 발송 실패'를 알려달라(HTTP 방식 메일 API로 바꿀 수 있음).

## 1. token_server.py: 수정본으로 교체
수정이 이미 반영된 `token_server.py`를 같이 드렸다. 올려주신 원본에 5곳만 추가했고(25줄 추가, 1줄 교체), 나머지는 그대로다.
- 파일 위쪽: `market_brief` import와 `MARKET_TOOL_ENABLED` (market_brief.py가 없거나 import에 실패해도 서버는 그대로 뜨고 이 기능만 꺼진다)
- `/api/token`의 tools 목록: `get_market_analysis` 추가, 메일 설정이 되어 있으면 `email_market_report`도 추가. 바로 아래 조건식에도 `MARKET_TOOL_ENABLED`를 넣었다
- `build_system_prompt()`: 최근 시장 흐름 메모 주입
- `/healthz` 위: `/api/market-analysis`, `/api/market-report` 등록
- `VOICE_PROMPT`: 두 함수 사용 지침 추가
독립 페르소나(PERSONA_FILE을 설정한 선녀보살 서비스)에서는 자동으로 꺼진다.
기존 코드의 참고 사항: tools를 싣는 조건식이 원래 `BUS_ARRIVAL_ENABLED`, `NEARBY_PLACES_ENABLED`는 보지 않아서, 검색/버스경로/유튜브가 전부 꺼진 상태에서는 근처 장소와 버스 도착 함수도 같이 빠진다. 이번에는 건드리지 않았다.

## 2. 프론트: 수정된 taeyeon-voice.html로 교체
올려주신 파일에 추가만 한 버전이다. 기존 줄은 하나도 지우거나 바꾸지 않았다.
- `MARKET_ANALYSIS_ENDPOINT`, `MARKET_REPORT_ENDPOINT` 상수
- `runGetMarketAnalysis()`: 짧은 음성 분석용 (시간 제한 20초)
- `runEmailMarketReport()`: 메일 리포트 요청용. 화면에 "정리 메일을 준비하고 있어요" 한 줄이 뜬다
- `handleToolCall()`에 `get_market_analysis`, `email_market_report` 분기
메일 함수에는 '수락 확인' 방어선을 넣었다: 방금 발화에 수락 표현(응/좋아/보내줘 등)이 있고 사양 표현(아니/됐어 등)이 없을 때만 실제 발송된다. 서버에서도 같은 종목 10분 중복 차단과 시간당 10통 한도가 있다.

## 3. 수집기 크론 (Railway 새 Cron 서비스 또는 기존 Oracle 서버 crontab)
- 같은 레포, 시작 명령: `python market_brief.py run`
- 환경변수: DATABASE_URL, GEMINI_API_KEY, NAVER_*, ECON_WATCHLIST 등 동일하게
- 스케줄 예: 하루 3번(06:30, 12:30, 18:30 KST). Railway 크론은 UTC 기준이라 `30 21,3,9 * * *`
- 첫 실행은 로컬/SSH에서 `python market_brief.py run`으로 로그를 보며 확인

## 4. 동작 확인 순서
1. `python market_brief.py collect` → "[collect] 새로 저장한 글 N건"
2. `python market_brief.py digest` → "[digest] 메모 N개 저장"
3. 서버 띄운 뒤 `curl -X POST localhost:8080/api/market-analysis -H 'Content-Type: application/json' -d '{"asset":"삼성전자"}'`
4. 메일 확인: 서버를 띄운 뒤
   `curl -X POST localhost:8080/api/market-report -H 'Content-Type: application/json' -d '{"asset":"삼성전자"}'`
   -> 응답은 바로 오고, 1분 안에 메일이 와야 한다. 안 오면 서버 로그의 "[market] 상세 리포트 발송 실패" 확인

## 5. 뉴스 피드와 블로거 정하기
- 뉴스 피드는 기본값이 코드에 들어 있다(한국경제, 매일경제, 연합뉴스, 이투데이, 인베스팅닷컴). 환경변수 `NEWS_FEEDS`를 따로 설정하면 그 값이 우선한다.
- 언론사가 RSS 주소를 바꾸면 끊길 수 있으니, 처음 한 번과 가끔 `python market_brief.py check-feeds`로 살아 있는지 확인한다.
- 블로거는 `python market_brief.py suggest-bloggers`를 실행하면 네이버 블로그 검색(관련도순) 상위에 자주 나오는 블로거를 RSS 후보 주소와 함께 보여준다. 대표 글 제목을 보고 직접 골라 `BLOG_FEEDS`에 `이름=주소` 형태로 쉼표로 이어 넣는다. 고른 뒤에는 `check-feeds`로 RSS가 열리는지 확인한다.
