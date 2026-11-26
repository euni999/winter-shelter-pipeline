# winter-shelter-pipeline

한파특보 발령 시 주변 한파쉼터 정보를 Kafka·Airflow로 수집·처리하는 데이터 파이프라인 포트폴리오.

## 흐름

```text
기상청 특보 API ──(매일)──▶ Airflow ──▶ Postgres ──▶ Kafka (kma.warning.sgg) ──▶ consumer ──▶ warning_event
행안부 쉼터 API ──(매주)──▶ Airflow ──▶ Postgres ─┘   "한파특보 × 시군구 + 쉼터 요약" 이벤트
```

특보 API는 최근 6일치만 조회되므로 매일 받아 쌓는다. 특보구역 이름은 시군구 매핑표로 쉼터 주소와 연결한다.

## 서비스 (docker compose)

| 서비스 | 역할 |
| --- | --- |
| `db` | 적재용 Postgres 16 |
| `airflow-db` | Airflow 메타데이터 Postgres 16 |
| `airflow-init` | 메타DB 준비 후 종료 |
| `airflow-apiserver` | 웹 UI·API (localhost:8080) |
| `airflow-scheduler` | 스케줄링·태스크 실행 (LocalExecutor) |
| `airflow-dag-processor` | DAG 파일 읽기 |
| `kafka` | 메시지 브로커 (KRaft 단일 노드) |
| `consumer` | 한파 이벤트를 받아 `warning_event`에 기록 (Python 3.11, 중복은 event_id로 걸러냄) |

## DAG

| DAG | 주기 | 하는 일 |
| --- | --- | --- |
| `kma_warning_list` | 매일 | 특보 목록 → `kma_warning_list` |
| `kma_warning_msg` | 매일 | 통보문 원문 저장 → 특보·구역 파싱 → 한파 이벤트 Kafka 발행 |
| `shelter_list` | 매주 | 쉼터 전체 → `shelter` (upsert, first/last_seen_at) |

## 실행

```bash
cp .env.example .env   # 값 채우기 (API 키, 비밀번호)
docker compose up -d
```

특보구역 매핑표 갱신 (`warning_area_unmapped` 뷰에 행이 나올 때):

```bash
P='psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
Q=$(python3.9 -I scripts/map_area_sgg.py --query)
docker compose exec -T db sh -c "$P -At -c \"$Q\"" \
  | python3.9 -I scripts/map_area_sgg.py --sql | docker compose exec -T db sh -c "$P -v ON_ERROR_STOP=1 -q"
```

## 매일 점검

특보 API는 6일치만 조회되므로, 실행이 5일 넘게 빠지면 데이터가 비게 된다.

1. 웹 UI(localhost:8080)에서 전날 0시 실행이 success인지 확인
2. 데이터 상태 확인 (모두 0이어야 정상)

```bash
docker compose exec db sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -c "select count(*) as 미매핑_구역 from warning_area_unmapped" \
  -c "select count(*) as 파싱_실패 from kma_warning_msg_parse where error is not null"'
```
