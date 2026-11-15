"""기상청 특보 통보문(getWthrWrnMsg)을 하루 1번 받아 원문을 쌓고, 특보·구역 단위로 파싱하는 DAG.

왜 통보문인가: 목록 API에는 구역이 없고, 통보문에만 구역(t2)과 발효·해제 시각(t3)이 있다.
왜 원문과 파싱 결과를 나누나: 구역은 문장이라 파싱 규칙이 앞으로 바뀔 수 있는데, 6일이 지나면
API에서 다시 받을 수 없다. 원문을 남겨 두면 파서를 고친 뒤 언제든 다시 파싱할 수 있다.

테이블: kma_warning_msg_raw(원문) → kma_warning_item(특보 N개) → kma_warning_area(구역 M개)
        kma_warning_msg_parse(통보문별 파싱 성공/실패 기록)
"""
import json
import re

import pendulum
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Variable, dag, task

BASE = "https://apis.data.go.kr/1360000/WthrWrnInfoService/"
KST = pendulum.timezone("Asia/Seoul")

# 항목 전체를 jsonb로 저장: 지금 안 쓰는 필드(t4 해제 예고, t6 현재 특보 현황 등)도 나중에 쓸 수 있게.
# 기본키는 목록 테이블과 같은 (관서, 발표시각, 발표번호) → 두 테이블을 이 키로 맞춰 볼 수 있다.
CREATE_SQL = """
CREATE TABLE IF NOT EXISTS kma_warning_msg_raw (
    stn_id     TEXT        NOT NULL,  -- 발표 관서 코드
    tm_fc      TEXT        NOT NULL,  -- 발표시각 YYYYMMDDHHMI
    tm_seq     INTEGER     NOT NULL,  -- 발표 번호
    item       JSONB       NOT NULL,  -- API 응답 항목 원문 (t1 제목, t2 구역, t3·t5 발효·해제 시각 ...)
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (stn_id, tm_fc, tm_seq)
);
"""
# 목록 DAG와 같은 방식의 멱등 적재: 이미 있는 통보문은 건너뛴다.
INSERT_SQL = """
INSERT INTO kma_warning_msg_raw (stn_id, tm_fc, tm_seq, item)
VALUES (%s, %s, %s, %s::jsonb)
ON CONFLICT (stn_id, tm_fc, tm_seq) DO NOTHING;
"""


# 파싱 로직을 고치면 이 숫자를 올린다 → 옛 버전으로 파싱된 원문만 다시 처리된다.
PARSER_VERSION = 1

# 외래키(REFERENCES): 원문에 없는 특보, 특보에 없는 구역이 생기지 않게 DB가 막아 준다.
# 구역은 ON DELETE CASCADE: 특보 행을 지우면 딸린 구역도 같이 지워져, 다시 파싱할 때 지우기가 한 줄로 끝난다.
PARSE_DDL = """
CREATE TABLE IF NOT EXISTS kma_warning_item (
    stn_id       TEXT    NOT NULL,
    tm_fc        TEXT    NOT NULL,
    tm_seq       INTEGER NOT NULL,
    item_no      INTEGER NOT NULL,  -- t2·t3의 (1), (2) ...
    warn_type    TEXT    NOT NULL,  -- 한파 / 강풍 / 풍랑 ...
    warn_level   TEXT    NOT NULL,  -- 주의보 / 경보
    action       TEXT    NOT NULL,  -- 발표 / 변경 / 해제
    effective_at TIMESTAMPTZ,       -- t3의 시각 (t5는 통보문당 하나라 특보별 시각으로 못 씀)
    area_text    TEXT    NOT NULL,  -- t2의 ':' 뒤 원문 (구역 파싱 검증용)
    PRIMARY KEY (stn_id, tm_fc, tm_seq, item_no),
    FOREIGN KEY (stn_id, tm_fc, tm_seq) REFERENCES kma_warning_msg_raw
);
CREATE TABLE IF NOT EXISTS kma_warning_area (
    stn_id  TEXT    NOT NULL,
    tm_fc   TEXT    NOT NULL,
    tm_seq  INTEGER NOT NULL,
    item_no INTEGER NOT NULL,
    area_no INTEGER NOT NULL,  -- 문장 안 순서
    parent  TEXT,              -- 경기도 / 동해남부앞바다 / NULL(울릉도.독도처럼 단독)
    area    TEXT    NOT NULL,  -- 가평 / 경북북부앞바다 / 울릉도.독도
    note    TEXT,              -- '여서도 제외' 같은 괄호 부연
    PRIMARY KEY (stn_id, tm_fc, tm_seq, item_no, area_no),
    FOREIGN KEY (stn_id, tm_fc, tm_seq, item_no) REFERENCES kma_warning_item ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS kma_warning_msg_parse (
    stn_id         TEXT        NOT NULL,
    tm_fc          TEXT        NOT NULL,
    tm_seq         INTEGER     NOT NULL,
    parser_version INTEGER     NOT NULL,
    parsed_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    error          TEXT,       -- 실패 사유 (성공이면 NULL). 실패해도 원문은 지우지 않는다.
    PRIMARY KEY (stn_id, tm_fc, tm_seq),
    FOREIGN KEY (stn_id, tm_fc, tm_seq) REFERENCES kma_warning_msg_raw
);
"""

# "(3) 한파주의보 발표 : 경기도(가평, ...)" → 번호, 종류, 등급, 상태, 구역 문장
ITEM_RE = re.compile(r"^\((\d+)\)\s*(\S+?)(주의보|경보)\s+(발표|변경|해제)\s*:\s*(.+)$")
# "(3) 한파주의보 발표 : 2026년 11월 06일 21시 00분" → 번호와 시각
TIME_RE = re.compile(r"^\((\d+)\).*:\s*(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일\s*(\d{1,2})시\s*(\d{1,2})분")


def numbered_lines(text):
    """'(n)'으로 시작하는 줄 단위로 묶는다. 긴 구역 문장이 다음 줄로 넘어가도 앞 줄에 이어 붙인다."""
    lines = []
    for line in (text or "").replace("\r", "").split("\n"):
        line = line.strip()
        if re.match(r"^\(\d+\)", line):
            lines.append(line)
        elif line and lines:
            lines[-1] += " " + line
    return lines


def split_top(text):
    """괄호 밖의 쉼표에서만 나눈다: '전라남도(여수, 완도(여서도 제외)), 강원도(태백)' → 2조각.
    단순 split(',')은 괄호 안 쉼표까지 잘라서 '시도(구역, 구역)' 구조가 깨진다."""
    parts, depth, cur = [], 0, ""
    for ch in text:
        depth += (ch == "(") - (ch == ")")
        if ch == "," and depth == 0:
            parts.append(cur.strip())
            cur = ""
        else:
            cur += ch
    parts.append(cur.strip())
    return [p for p in parts if p]


def name_and_inner(token):
    """'경기도(가평, 양평서부)' → ('경기도', '가평, 양평서부'), '울릉도.독도' → ('울릉도.독도', None)"""
    m = re.match(r"^([^()]+?)\s*\((.*)\)$", token)
    return (m.group(1).strip(), m.group(2).strip()) if m else (token.strip(), None)


def parse_areas(area_text):
    """구역 문장 → [(parent, area, note), ...]
    같은 'A(B)' 모양이 세 가지 뜻으로 쓰여서 구분한다(11/06·11/07 샘플 기준):
      시도(구역들)      경기도(가평, 양평서부)        → parent=경기도
      바다(세부 바다)   동해남부앞바다(경북북부앞바다) → parent=동해남부앞바다
      구역(부연)        완도(여서도 제외)             → note='여서도 제외'  ('제외'로 끝나면 부연으로 본다)"""
    rows = []
    for token in split_top(area_text):
        name, inner = name_and_inner(token)
        if inner is None:
            rows.append((None, name, None))
        elif inner.endswith("제외"):
            rows.append((None, name, inner))
        else:
            for child in split_top(inner):
                cname, cinner = name_and_inner(child)
                rows.append((name, cname, cinner))
    return rows


def parse_msg(t2, t3):
    """통보문 t2(구역), t3(시각) → [(item_no, 종류, 등급, 상태, 시각, 구역문장, [구역...]), ...]
    형식이 다르면 예외를 던진다 → 호출한 쪽이 그 통보문만 실패로 기록하고 넘어간다."""
    times = {}
    for line in numbered_lines(t3):
        m = TIME_RE.match(line)
        if m:
            n, y, mo, d, h, mi = map(int, m.groups())
            # '24시 00분'이 오면 다음날 0시로 바꾼다 (datetime은 24시를 못 받는다).
            times[n] = pendulum.datetime(y, mo, d, 0, mi, tz=KST).add(hours=h)
    items = []
    for line in numbered_lines(t2):
        m = ITEM_RE.match(line)
        if not m:
            raise ValueError("t2 형식 불일치: " + line[:80])
        n = int(m.group(1))
        items.append((n, m.group(2), m.group(3), m.group(4), times.get(n), m.group(5), parse_areas(m.group(5))))
    if not items:
        raise ValueError("t2에 특보 줄이 없음")
    return items


def window():
    # 목록 DAG와 같은 이유로 "지금" 기준 최근 6일 (API가 허용하는 최대 범위).
    # 조회 가능한 첫날의 00시 발표분은 응답에서 빠지지만, 그 전 실행들에서 이미 받아진다(11/15 점검).
    now = pendulum.now(KST)
    return now.subtract(days=6).format("YYYYMMDD"), now.format("YYYYMMDD")


def get_items(operation, **params):
    """API 1회 호출 → item 목록. 특보가 없으면(03 NO_DATA) 빈 목록."""
    frm, to = window()
    query = {"ServiceKey": Variable.get("KMA_API_KEY"), "pageNo": 1, "numOfRows": 1000,
             "dataType": "JSON", "fromTmFc": frm, "toTmFc": to, **params}
    try:
        r = requests.get(BASE + operation, params=query, timeout=30)
        r.raise_for_status()
        data = r.json()
    except (requests.RequestException, ValueError) as e:
        # 원래 예외 메시지엔 키가 든 URL이 찍힐 수 있어 오류 종류만 남긴다.
        raise RuntimeError("%s 호출 실패: %s" % (operation, type(e).__name__)) from None
    header = data["response"]["header"]
    if header["resultCode"] == "03":
        return []
    if header["resultCode"] != "00":
        raise RuntimeError("%s 오류: %s %s" % (operation, header["resultCode"], header["resultMsg"]))
    body = data["response"]["body"]
    items = body["items"]["item"]
    items = [items] if isinstance(items, dict) else items
    # 점검 결과 관서당 6일치는 30건 안팎이라 1000건 한 페이지로 충분하다.
    # 혹시 넘치면 조용히 잘리지 않도록 실패시켜서 알아챈다.
    if len(items) < int(body["totalCount"]):
        raise RuntimeError("%s: %s건 중 %d건만 받음 — 페이지 처리 필요" % (operation, body["totalCount"], len(items)))
    return items


@dag(
    start_date=pendulum.datetime(2026, 11, 15, tz=KST),
    schedule="@daily",
    catchup=False,  # API가 6일 전까지만 줘서 과거 날짜를 몰아서 실행해도 의미가 없다.
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=5)},
    tags=["kma", "warning"],
)
def kma_warning_msg():
    @task
    def stations():
        # 통보문은 stnId가 필수라 관서별로 불러야 한다. 관서 목록을 코드에 고정하지 않고
        # 목록 API에서 "최근 6일에 특보를 낸 관서"만 뽑는다 → 새 관서도 놓치지 않고, 헛호출도 없다.
        ids = sorted({str(i["stnId"]) for i in get_items("getWthrWrnList")})
        print("특보를 낸 관서 %d곳: %s" % (len(ids), ids))
        return ids

    @task
    def fetch(stn_ids):
        rows = []
        for stn in stn_ids:
            items = get_items("getWthrWrnMsg", stnId=stn)
            rows += [(str(i["stnId"]), str(i["tmFc"]), int(i["tmSeq"]), json.dumps(i, ensure_ascii=False))
                     for i in items]
            print("관서 %s: %d건" % (stn, len(items)))
        return rows

    @task
    def load(rows):
        conn = PostgresHook(postgres_conn_id="shelter_db").get_conn()
        try:
            with conn, conn.cursor() as cur:  # 블록 끝에서 커밋, 실패하면 전체 롤백
                cur.execute(CREATE_SQL)
                cur.executemany(INSERT_SQL, rows)
                cur.execute("SELECT count(*) FROM kma_warning_msg_raw")
                total = cur.fetchone()[0]
        finally:
            conn.close()
        print("받은 %d건 처리, 테이블 전체 %d건" % (len(rows), total))

    @task
    def parse():
        # 오늘 받은 것만이 아니라 "아직 파싱 안 됐거나 옛 버전으로 파싱된" 원문 전체가 대상.
        # → 백업처럼 DAG 밖에서 넣은 원문도, 파서를 고친 뒤의 재처리도 같은 태스크가 맡는다.
        # 연결 하나로 통보문 여러 건을 각각의 트랜잭션으로 처리하므로 commit/rollback을 직접 부른다.
        # (이 이미지의 드라이버는 psycopg 3이라 `with conn:`을 쓰면 블록 끝에서 연결까지 닫혀 버린다)
        conn = PostgresHook(postgres_conn_id="shelter_db").get_conn()
        ok = failed = 0
        try:
            with conn.cursor() as cur:
                cur.execute(PARSE_DDL)
                cur.execute("""
                    SELECT r.stn_id, r.tm_fc, r.tm_seq, r.item->>'t2', r.item->>'t3'
                    FROM kma_warning_msg_raw r
                    LEFT JOIN kma_warning_msg_parse p USING (stn_id, tm_fc, tm_seq)
                    WHERE p.stn_id IS NULL OR p.parser_version < %s""", (PARSER_VERSION,))
                todo = cur.fetchall()
            conn.commit()
            for stn, tm_fc, seq, t2, t3 in todo:
                key = (stn, tm_fc, seq)
                status = "INSERT INTO kma_warning_msg_parse (stn_id, tm_fc, tm_seq, parser_version, error) " \
                         "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (stn_id, tm_fc, tm_seq) DO UPDATE " \
                         "SET parser_version = EXCLUDED.parser_version, parsed_at = now(), error = EXCLUDED.error"
                try:
                    items = parse_msg(t2, t3)
                    # 통보문 하나 = 트랜잭션 하나: 기존 결과 삭제 → 새로 넣기 → 상태 기록이 함께 성공하거나 함께 취소된다.
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM kma_warning_item WHERE stn_id=%s AND tm_fc=%s AND tm_seq=%s", key)
                        for n, wtype, level, action, at, area_text, areas in items:
                            cur.execute("INSERT INTO kma_warning_item VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                                        key + (n, wtype, level, action, at, area_text))
                            cur.executemany("INSERT INTO kma_warning_area VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                                            [key + (n, i, p, a, note) for i, (p, a, note) in enumerate(areas, 1)])
                        cur.execute(status, key + (PARSER_VERSION, None))
                    conn.commit()
                    ok += 1
                except Exception as e:
                    # 실패한 통보문은 반쯤 들어간 결과를 취소하고, 원문은 그대로 둔 채 사유만 남긴다.
                    # 다른 통보문 파싱은 계속한다.
                    conn.rollback()
                    with conn.cursor() as cur:
                        cur.execute(status, key + (PARSER_VERSION, "%s: %s" % (type(e).__name__, str(e)[:300])))
                    conn.commit()
                    failed += 1
        finally:
            conn.close()
        print("파싱 대상 %d건: 성공 %d, 실패 %d" % (ok + failed, ok, failed))

    load(fetch(stations())) >> parse()


kma_warning_msg()
