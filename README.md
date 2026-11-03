# demo-app: 이전 대상 데모 채팅 앱

AI 에이전트들이 계속 대화하는 채팅방입니다. 메시지마다 **처리한 앱의 환경(env-a, env-b)과 저장된 DB(pg-a, pg-b)**를 함께 기록하므로, 이전 과정이 채팅 기록에 그대로 남습니다. 평가 대상은 관리 플랫폼이고, 이 앱은 플랫폼이 무중단 이전을 해냈다는 사실을 눈으로 보여 주는 수단입니다.

## 구성

| 경로 | 내용 |
|---|---|
| `main.go` | HTTP 서버. `POST /api/messages`, `GET /api/stream`(SSE), `GET /api/messages`, `POST /api/verify`, `GET /api/stats`, `GET /healthz` |
| `store.go` | pgx (PgBouncer transaction 모드에 맞춰 simple protocol 사용) |
| `hub.go` | DB를 150ms마다 조회해 새 메시지를 SSE 구독자에게 전달 |
| `static/` | 채팅 화면 (빌드 단계 없는 순수 JS, 바이너리에 embed) |
| `db/` | 채팅 스키마와 노드 이름 설정 (PoC의 Compose가 pg-a, pg-b 초기화에 사용) |
| `agents/` | 대화하는 AI 에이전트 (Python, Anthropic SDK) |
| `haproxy/`, `scripts/weight.sh` | 로컬 엣지 프록시와 가중치 변경 |

## 실행

```bash
# CDC PoC(pg-a, pg-b, PgBouncer) + 앱 2개(env-a, env-b) + HAProxy
docker compose up -d --build
open http://localhost:8080          # 채팅 화면
open http://localhost:8404          # HAProxy 통계

# 에이전트 (API 키 필요. --offline이면 준비된 대사로 대화)
cd agents
export ANTHROPIC_API_KEY=...
uv run chatbots.py --url http://localhost:8080
```

모델은 기본값이 `claude-haiku-5-5`이며 `CHAT_MODEL` 환경 변수로 바꿀 수 있습니다. 4~5초에 한 마디씩 말하면 시간당 비용은 약 $0.2입니다.

## 이전 리허설 (로컬)

```bash
tb() { docker compose run --rm -T toolbox "$@"; }

# 복제 시작
tb scripts/01_copy_schema.sh pg-a pg-b
tb scripts/02_start_replication.sh pg-a pg-b
tb scripts/03_wait_sync.sh pg-a pg-b 60

# Phase A: 트래픽 전환
./scripts/weight.sh 50 50
./scripts/weight.sh 0 100

# Phase B: DB 전환 (+ 역방향 복제)
tb scripts/05_switchover.sh pg-a pg-b

# 기존 환경 종료
docker compose stop app-a
```

에이전트를 Ctrl-C로 종료하면 보낸 메시지, 저장 확인, 스트림 수신, 유실, 중복 집계가 출력됩니다.

## 설계 메모

- **id 순서와 커밋 순서를 맞춥니다.** 동시에 들어온 INSERT는 id 순서와 커밋 순서가 어긋날 수 있고, 그러면 `id > 마지막 id` 조회가 늦게 커밋된 작은 id를 건너뜁니다. 그래서 INSERT를 advisory lock으로 직렬화합니다.
- **유실 판단은 `client_msg_id`로 합니다.** 시퀀스는 중복 거부나 롤백에도 번호를 소비하므로 id가 건너뛴 것을 유실로 보면 안 됩니다.
- **헬스 체크는 DB를 확인하지 않습니다.** DB 전환 중 PgBouncer PAUSE 동안 헬스 체크까지 실패하면 로드 밸런서가 모든 인스턴스를 내립니다.
- **종료 순서:** SIGTERM → 헬스 체크 503 → SSE 연결 종료(브라우저가 다른 인스턴스로 재접속) → 3초 대기 → 진행 중인 요청 완료 후 종료.
- **`LISTEN/NOTIFY`를 쓰지 않습니다.** PgBouncer transaction 모드에서 동작하지 않고 논리 복제로도 전달되지 않으므로, 공유 DB를 조회하는 방식을 메시지 버스로 씁니다.

## 레지스트리 배포

```bash
brew install go ko
KO_DOCKER_REPO=ghcr.io/<org>/demo-app ko build --bare .   # linux/amd64 + linux/arm64
```

## 남은 작업

- 클라우드 2단계 스택(`poc/cdc/cloud`)에는 아직 `db/node.sql`과 `db/schema.sql`을 적용하지 않았습니다. 적용하지 않으면 `db_node`가 `unknown`으로 표시됩니다.
- 진입점(HAProxy)이 env-a에 있으면 env-a를 완전히 지울 수 없습니다. DNS나 외부 엣지로 옮기는 설계가 필요합니다.
