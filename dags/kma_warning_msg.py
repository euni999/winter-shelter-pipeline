"""기상청 특보 통보문(getWthrWrnMsg)을 하루 1번 받아 원문을 쌓고, 특보·구역 단위로 파싱하는 DAG.

왜 통보문인가: 목록 API에는 구역이 없고, 통보문에만 구역(t2)과 발효·해제 시각(t3)이 있다.
왜 원문과 파싱 결과를 나누나: 구역은 문장이라 파싱 규칙이 앞으로 바뀔 수 있는데, 6일이 지나면
API에서 다시 받을 수 없다. 원문을 남겨 두면 파서를 고친 뒤 언제든 다시 파싱할 수 있다.

테이블: kma_warning_msg_raw(원문) → kma_warning_item(특보 N개) → kma_warning_area(구역 M개)
        kma_warning_msg_parse(통보문별 파싱 성공/실패 기록)
        kma_warning_pub(Kafka로 보낸 이벤트 기록)
마지막 publish 태스크가 새 한파특보를 "특보 × 시군구 + 쉼터 요약" 이벤트로 Kafka에 보낸다.
"""
import json
import re

import pendulum
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Variable, dag, task

BASE = "https://apis.data.go.kr/1360000/WthrWrnInfoService/"
KST = pendulum.timezone("Asia/Seoul")

# 테이블 구조는 dags/tables.py(SQLAlchemy Core)에 정의돼 있다.


def engine():
    # 연결은 SQLAlchemy 엔진으로: begin() 블록이 커밋·롤백·연결 반납을 알아서 한다.
    # (예전처럼 드라이버마다 다른 `with conn` 동작을 신경 쓸 필요가 없다)
    return PostgresHook(postgres_conn_id="shelter_db").get_sqlalchemy_engine()


# 파싱 로직을 고치면 이 숫자를 올린다 → 옛 버전으로 파싱된 원문만 다시 처리된다.
# v2(11/21): 상태 '연장' 추가, 'o'로 시작하는 바다 부연 줄 무시
PARSER_VERSION = 2

# "(3) 한파주의보 발표 : 경기도(가평, ...)" → 번호, 종류, 등급, 상태, 구역 문장
# 상태는 원문 전체(11/21, 433건)에서 발표·해제·변경·연장 4종만 확인됨. 새 값이 오면 실패로 기록돼 드러난다.
ITEM_RE = re.compile(r"^\((\d+)\)\s*(\S+?)(주의보|경보)\s+(발표|변경|해제|연장)\s*:\s*(.+)$")
# "(3) 한파주의보 발표 : 2026년 11월 06일 21시 00분" → 번호와 시각
TIME_RE = re.compile(r"^\((\d+)\).*:\s*(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일\s*(\d{1,2})시\s*(\d{1,2})분")


def numbered_lines(text):
    """'(n)'으로 시작하는 줄 단위로 묶는다. 긴 구역 문장이 다음 줄로 넘어가도 앞 줄에 이어 붙인다."""
    lines = []
    for line in (text or "").replace("\r", "").split("\n"):
        line = line.strip()
        if re.match(r"^\(\d+\)", line):
            lines.append(line)
        elif line.startswith("o "):
            # 'o 충남북부앞바다(... 평수구역 제외)' 같은 바다 구역 부연 줄. 구역 목록이 아니라
            # 이어 붙이면 구역 이름이 깨진다 → 버린다. (원문은 raw 테이블에 그대로 남아 있다)
            continue
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


# ── Kafka 발행 ──────────────────────────────────────────────
KAFKA = "kafka:9092"  # compose 네트워크 안 주소 (비밀이 아니라 상수로 둔다)
TOPIC = "kma.warning.sgg"
# 쉼터 요약이 의미 있는 건 한파뿐이라 한파만 보낸다. 다른 특보도 보내려면 여기에 추가.
PUB_TYPES = ("한파",)

# 특보 → 구역 → 매핑표 → 시군구, 그리고 그 시군구의 쉼터 수·수용인원.
# DISTINCT를 먼저 하는 이유: 강릉산지·강릉평지처럼 구역 둘이 같은 시군구로 가면
# 쉼터를 두 번 붙여 합계가 두 배가 된다.
EVENT_SQL = """
WITH t AS (
    SELECT DISTINCT i.stn_id, i.tm_fc, i.tm_seq, i.item_no, i.warn_type, i.warn_level,
           i.action, i.effective_at, m.sido, m.sgg
    FROM kma_warning_item i
    JOIN kma_warning_area a USING (stn_id, tm_fc, tm_seq, item_no)
    JOIN warning_area_sgg m ON m.parent = coalesce(a.parent, '') AND m.area = a.area
    WHERE i.warn_type = ANY(:types)
)
SELECT t.*, count(s.fclt_no), coalesce(sum(s.capacity), 0)
FROM t LEFT JOIN shelter s USING (sido, sgg)
WHERE NOT EXISTS (SELECT 1 FROM kma_warning_pub p
                  WHERE (p.stn_id, p.tm_fc, p.tm_seq, p.item_no, p.sido, p.sgg)
                      = (t.stn_id, t.tm_fc, t.tm_seq, t.item_no, t.sido, t.sgg))
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
ORDER BY t.tm_fc, t.item_no
"""


def ensure_topic():
    # 브로커에서 토픽 자동 생성을 꺼 뒀으므로(compose) 여기서 없을 때만 만든다.
    # 파티션 3개: 시군구 키로 나뉘어 컨슈머를 늘리면 나눠 받을 수 있다. 브로커 1대라 복제는 1.
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": KAFKA})
    if TOPIC not in admin.list_topics(timeout=10).topics:
        admin.create_topics([NewTopic(TOPIC, num_partitions=3, replication_factor=1)])[TOPIC].result()
        print("토픽 생성:", TOPIC)


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
            # item은 dict 그대로: JSONB 컬럼이라 SQLAlchemy가 JSON으로 바꿔 넣는다.
            rows += [{"stn_id": str(i["stnId"]), "tm_fc": str(i["tmFc"]), "tm_seq": int(i["tmSeq"]), "item": i}
                     for i in items]
            print("관서 %s: %d건" % (stn, len(items)))
        return rows

    @task
    def load(rows):
        from sqlalchemy import func, select
        from sqlalchemy.dialects.postgresql import insert
        from tables import kma_warning_msg_raw as t

        with engine().begin() as conn:  # 블록 끝에서 커밋, 실패하면 전체 롤백
            t.create(conn, checkfirst=True)
            if rows:  # 빈 목록이면 값 없는 INSERT가 돼 NOT NULL 오류가 난다
                # 목록 DAG와 같은 멱등 적재: 이미 있는 통보문은 건너뛴다.
                conn.execute(insert(t).on_conflict_do_nothing(index_elements=t.primary_key.columns), rows)
            total = conn.scalar(select(func.count()).select_from(t))
        print("받은 %d건 처리, 테이블 전체 %d건" % (len(rows), total))

    @task
    def parse():
        # 오늘 받은 것만이 아니라 "아직 파싱 안 됐거나 옛 버전으로 파싱된" 원문 전체가 대상.
        # → 백업처럼 DAG 밖에서 넣은 원문도, 파서를 고친 뒤의 재처리도 같은 태스크가 맡는다.
        from sqlalchemy import and_, delete, func, or_, select
        from sqlalchemy.dialects.postgresql import insert
        from tables import kma_warning_area as area_t
        from tables import kma_warning_item as item_t
        from tables import kma_warning_msg_parse as parse_t
        from tables import kma_warning_msg_raw as raw_t
        from tables import metadata

        eng = engine()
        with eng.begin() as conn:
            metadata.create_all(conn, tables=[item_t, area_t, parse_t])  # 없을 때만 만든다
            todo = conn.execute(
                select(raw_t.c.stn_id, raw_t.c.tm_fc, raw_t.c.tm_seq,
                       raw_t.c.item["t2"].astext, raw_t.c.item["t3"].astext)  # item->>'t2'
                .outerjoin(parse_t, and_(*[raw_t.c[k] == parse_t.c[k] for k in ("stn_id", "tm_fc", "tm_seq")]))
                .where(or_(parse_t.c.stn_id.is_(None), parse_t.c.parser_version < PARSER_VERSION))
            ).all()

        # 파싱 상태 기록: 처음이면 넣고, 다시 파싱한 거면 버전·시각·오류를 덮어쓴다.
        status = insert(parse_t)
        status = status.on_conflict_do_update(
            index_elements=parse_t.primary_key.columns,
            set_={"parser_version": status.excluded.parser_version, "parsed_at": func.now(),
                  "error": status.excluded.error})

        ok = failed = 0
        with eng.connect() as conn:
            for stn, tm_fc, seq, t2, t3 in todo:
                key = {"stn_id": stn, "tm_fc": tm_fc, "tm_seq": seq}
                try:
                    items = parse_msg(t2, t3)
                    # 통보문 하나 = 트랜잭션 하나: 기존 결과 삭제 → 새로 넣기 → 상태 기록이 함께 성공하거나
                    # 함께 취소된다. conn.begin() 블록 안에서 예외가 나면 SQLAlchemy가 자동으로 롤백한다.
                    with conn.begin():
                        conn.execute(delete(item_t).filter_by(**key))  # 딸린 구역은 CASCADE로 같이 삭제
                        for n, wtype, level, action, at, area_text, areas in items:
                            conn.execute(insert(item_t), {**key, "item_no": n, "warn_type": wtype,
                                                          "warn_level": level, "action": action,
                                                          "effective_at": at, "area_text": area_text})
                            if areas:
                                conn.execute(insert(area_t), [{**key, "item_no": n, "area_no": i, "parent": p,
                                                               "area": a, "note": note}
                                                              for i, (p, a, note) in enumerate(areas, 1)])
                        conn.execute(status, {**key, "parser_version": PARSER_VERSION, "error": None})
                    ok += 1
                except Exception as e:
                    # 실패한 통보문은 (이미 롤백됐고) 원문은 그대로 둔 채 사유만 남긴다. 다른 통보문은 계속.
                    with conn.begin():
                        conn.execute(status, {**key, "parser_version": PARSER_VERSION,
                                              "error": "%s: %s" % (type(e).__name__, str(e)[:300])})
                    failed += 1
        print("파싱 대상 %d건: 성공 %d, 실패 %d" % (ok + failed, ok, failed))

    @task
    def publish():
        # 태스크 안에서 import: DAG 파일은 수시로 다시 읽히는데, 그때마다 Kafka 라이브러리를
        # 불러올 필요는 없다(Airflow 권장 방식).
        from confluent_kafka import Producer
        from sqlalchemy import text
        from sqlalchemy.dialects.postgresql import insert
        from tables import kma_warning_pub as pub_t

        eng = engine()
        with eng.begin() as conn:
            pub_t.create(conn, checkfirst=True)
            # 여러 테이블을 잇는 분석 쿼리는 Core로 옮기면 오히려 읽기 어려워 SQL 그대로 둔다(text).
            rows = conn.execute(text(EVENT_SQL), {"types": list(PUB_TYPES)}).all()
        if not rows:
            print("새로 보낼 이벤트 없음")
            return

        ensure_topic()
        # acks=all + 멱등 프로듀서: 브로커가 확실히 저장했을 때만 성공으로 보고,
        # 네트워크 재시도로 같은 메시지가 두 번 쌓이는 것도 막는다.
        producer = Producer({"bootstrap.servers": KAFKA, "acks": "all", "enable.idempotence": True})
        errors = []
        for stn, tm_fc, seq, n, wtype, level, action, at, sido, sgg, shelters, cap in rows:
            event = {
                # 컨슈머가 중복을 거르는 기준 (아래 "최소 한 번" 설명 참고)
                "event_id": "%s-%s-%s-%s-%s-%s" % (stn, tm_fc, seq, n, sido, sgg),
                "warn_type": wtype, "warn_level": level, "action": action,
                # DB 드라이버는 UTC로 돌려주므로 한국 시각으로 바꿔 보낸다(같은 시각, 읽기 쉽게).
                "effective_at": at.astimezone(KST).isoformat() if at else None, "tm_fc": tm_fc,
                "sido": sido, "sgg": sgg, "shelters": shelters, "capacity": int(cap),
            }
            # 키 = 시군구: 같은 키는 같은 파티션 → 한 시군구 안에서는 발표→해제 순서가 지켜진다.
            producer.produce(TOPIC, key=("%s %s" % (sido, sgg)).encode(),
                             value=json.dumps(event, ensure_ascii=False).encode(),
                             on_delivery=lambda err, msg: err and errors.append(str(err)))
        left = producer.flush(30)  # 버퍼에 쌓인 것을 다 보내고 확인 응답까지 기다린다
        if errors or left:
            # 하나라도 실패하면 기록하지 않는다 → 다음 실행에서 전부 다시 보낸다.
            raise RuntimeError("Kafka 전송 실패 %d건, 미전송 %d건: %s" % (len(errors), left, errors[:3]))

        # 최소 한 번(at-least-once): 전송 성공 후 여기서 DB 기록이 실패하면 다음 실행에서 또 보낸다.
        # 그래서 중복은 생길 수 있지만 누락은 없다. 중복은 event_id로 컨슈머가 거른다.
        with eng.begin() as conn:
            conn.execute(insert(pub_t).on_conflict_do_nothing(),
                         [{"stn_id": r[0], "tm_fc": r[1], "tm_seq": r[2], "item_no": r[3],
                           "sido": r[8], "sgg": r[9]} for r in rows])
        print("이벤트 %d건 발행 → %s" % (len(rows), TOPIC))

    load(fetch(stations())) >> parse() >> publish()


kma_warning_msg()
