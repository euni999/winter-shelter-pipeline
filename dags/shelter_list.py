"""한파쉼터 목록(행정안전부 DSSP-IF-10804) 전체를 매주 받아 shelter 테이블에 upsert하는 DAG.

왜 매주 전체를 받나: API에 "변경분만" 받는 조건이 없고, 전체가 5.6만 건(56회 호출)이라
일일 한도(1000회) 안에서 충분하다. 쉼터는 특보처럼 매일 바뀌지 않아 주 1회면 된다.
왜 덮어쓰지 않고 upsert하나: first_seen_at/last_seen_at을 남겨 쉼터가 언제 생기고
(목록에서) 사라졌는지 추적하려고. 사라진 쉼터는 지우지 않고 last_seen_at이 멈춘 채 남는다.
"""
import json

import pendulum
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Variable, dag, task

URL = "https://www.safetydata.go.kr/V2/api/DSSP-IF-10804"
KST = pendulum.timezone("Asia/Seoul")
ROWS = 1000  # 사전점검에서 1000이 최대로 확인됨

# 자주 쓰는 값만 컬럼으로 꺼내고, 운영시간 등 나머지는 원문(item)에 그대로 둔다.
# sido/sgg는 주소 앞 두 단어 → 특보구역 매핑표(warning_area_sgg)와 같은 이름 체계.
CREATE_SQL = """
CREATE TABLE IF NOT EXISTS shelter (
    fclt_no       BIGINT PRIMARY KEY,  -- REARE_FCLT_NO (사전점검에서 중복 0 확인)
    name          TEXT,
    fclt_type     TEXT,                -- 001 공공, 003 경로당, 004 민간
    sido          TEXT,                -- 주소가 없으면 NULL (약 610건)
    sgg           TEXT,
    capacity      INTEGER,             -- 이용가능인원 UTZTN_PSBLTY_TNOP
    lat           DOUBLE PRECISION,
    lon           DOUBLE PRECISION,
    item          JSONB NOT NULL,      -- API 원문 한 행
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),  -- 처음 목록에 나온 시각 (갱신 안 함)
    last_seen_at  TIMESTAMPTZ NOT NULL DEFAULT now()   -- 마지막으로 목록에 있던 시각
);
"""
# 이미 있는 쉼터는 내용을 최신으로 바꾸고 last_seen_at만 당긴다. first_seen_at은 그대로.
UPSERT_SQL = """
INSERT INTO shelter (fclt_no, name, fclt_type, sido, sgg, capacity, lat, lon, item)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
ON CONFLICT (fclt_no) DO UPDATE SET
    name = EXCLUDED.name, fclt_type = EXCLUDED.fclt_type, sido = EXCLUDED.sido,
    sgg = EXCLUDED.sgg, capacity = EXCLUDED.capacity, lat = EXCLUDED.lat, lon = EXCLUDED.lon,
    item = EXCLUDED.item, last_seen_at = now();
"""


def sido_sgg(r):
    # 도로명주소 우선, 없으면 지번주소. 세종은 시군구가 없어 두 번째 단어가 도로명이라 시 이름을 쓴다.
    # (scripts/map_area_sgg.py의 load_sgg와 같은 규칙이어야 매핑표와 이어진다)
    a = (r.get("RONA_DADDR") or r.get("DADDR") or "").split()
    if len(a) < 2:
        return None, None
    return a[0], (a[0] if a[0] == "세종특별자치시" else a[1])


def get_page(key, page):
    try:
        r = requests.get(URL, params={"serviceKey": key, "pageNo": page, "numOfRows": ROWS}, timeout=60)
        r.raise_for_status()
        data = r.json()
    except (requests.RequestException, ValueError) as e:
        # 예외 메시지엔 키가 든 URL이 찍힐 수 있어 오류 종류만 남기고 원인 연결도 끊는다.
        raise RuntimeError("쉼터 API 호출 실패(page %d): %s" % (page, type(e).__name__)) from None
    header = data.get("header") or {}
    if header.get("resultCode") != "00":
        raise RuntimeError("쉼터 API 오류: %s %s" % (header.get("resultCode"), header.get("resultMsg")))
    return int(data.get("totalCount") or 0), data.get("body") or []


@dag(
    start_date=pendulum.datetime(2026, 11, 16, tz=KST),
    schedule="@weekly",  # 일요일 0시(KST)
    catchup=False,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=5)},
    tags=["shelter"],
)
def shelter_list():
    # 받기와 넣기를 한 태스크로 둔 이유: 5.6만 건(수십 MB)을 태스크 사이로 넘기면
    # XCom이 메타DB에 그대로 저장돼 무거워진다. 특보 DAG(수백 건)와 다르게 판단했다.
    @task
    def fetch_and_load():
        key = Variable.get("SHELTER_API_KEY")
        total, rows = get_page(key, 1)
        for page in range(2, min(-(-total // ROWS), 100) + 1):  # 올림 나눗셈, 100페이지 안전장치
            rows += get_page(key, page)[1]
        # 일부만 받은 채 넣으면 못 받은 쉼터의 last_seen_at이 멈춰 "사라진 것"처럼 보인다 → 아예 실패시킨다.
        if len(rows) < total:
            raise RuntimeError("쉼터 %d/%d건만 받음" % (len(rows), total))

        params = []
        for r in rows:
            sido, sgg = sido_sgg(r)
            params.append((r["REARE_FCLT_NO"], r.get("REARE_NM"), r.get("FCLT_TYPE"), sido, sgg,
                           r.get("UTZTN_PSBLTY_TNOP"), r.get("LAT"), r.get("LOT"),
                           json.dumps(r, ensure_ascii=False)))

        conn = PostgresHook(postgres_conn_id="shelter_db").get_conn()
        try:
            # 전부 성공하면 커밋, 하나라도 실패하면 전체 롤백 → 반쯤 갱신된 상태가 남지 않는다.
            # with conn 대신 직접 커밋: psycopg 3에선 with conn이 연결까지 닫아 버려서(통보문 DAG에서 겪음).
            with conn.cursor() as cur:
                cur.execute(CREATE_SQL)
                cur.executemany(UPSERT_SQL, params)
                cur.execute("SELECT count(*), count(*) FILTER (WHERE last_seen_at < now()) FROM shelter")
                n_all, n_gone = cur.fetchone()
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        # now()는 트랜잭션 시작 시각이라, 이번에 안 들어온 쉼터만 last_seen_at < now()가 된다.
        print("받은 %d건 upsert, 테이블 전체 %d건, 이번 목록에 없던 쉼터 %d건" % (len(params), n_all, n_gone))

    fetch_and_load()


shelter_list()
