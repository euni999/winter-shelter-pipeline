"""적재용 DB(shelter_db)의 테이블 정의를 SQLAlchemy Core로 모아 두는 곳.

왜 Core인가: SQL 문자열 대신 파이썬 객체로 테이블을 정의하면 컬럼 이름 오타를 코드가 잡아 주고,
DAG마다 흩어진 CREATE 문 없이 이 파일 하나가 스키마의 기준이 된다.
ORM(클래스+Session)은 행 하나씩 다루는 앱에 맞고, 수백~수만 건을 한 번에 넣는 적재엔 Core가 가볍다.

DAG 파일이 아니다: dags/ 폴더가 파이썬 경로에 잡혀 있어 DAG에서 `from tables import ...`로 쓴다.
테이블은 DAG를 하나씩 옮기면서 여기로 옮긴다(현재: kma_warning_list, shelter, 통보문 5개).
"""
from sqlalchemy import (BigInteger, Column, DateTime, Float, ForeignKeyConstraint, Integer,
                        MetaData, Table, Text, func)
from sqlalchemy.dialects.postgresql import JSONB

metadata = MetaData()

# 기존 SQL 문자열(CREATE TABLE)과 같은 구조 → 이미 있는 테이블을 그대로 쓴다.
# 기본키 세 개를 묶은 이유: tm_seq는 관서별·월별로 다시 시작해 단독으로는 겹칠 수 있다.
kma_warning_list = Table(
    "kma_warning_list", metadata,
    Column("stn_id", Text, primary_key=True),   # 발표 관서 코드 (구역 아님)
    Column("tm_fc", Text, primary_key=True),    # 발표시각 YYYYMMDDHHMI
    Column("tm_seq", Integer, primary_key=True),  # 발표 번호
    Column("title", Text, nullable=False),      # 종류·등급·발표/해제가 든 원문
    # server_default: 값을 안 넣으면 DB가 now()로 채운다 (처음 받은 시각)
    Column("fetched_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

# 한파쉼터 (shelter_list DAG). 자주 쓰는 값만 컬럼으로 꺼내고 나머지(운영시간 등)는 item 원문에 둔다.
# sido/sgg는 주소 앞 두 단어 → 특보구역 매핑표(warning_area_sgg)와 같은 이름 체계.
shelter = Table(
    "shelter", metadata,
    Column("fclt_no", BigInteger, primary_key=True),  # REARE_FCLT_NO (사전점검에서 중복 0 확인)
    Column("name", Text),
    Column("fclt_type", Text),        # 001 공공, 003 경로당, 004 민간
    Column("sido", Text),             # 주소가 없으면 NULL
    Column("sgg", Text),
    Column("capacity", Integer),      # 이용가능인원 UTZTN_PSBLTY_TNOP
    Column("lat", Float(precision=53)),  # precision=53 → DOUBLE PRECISION (기존 테이블과 같게)
    Column("lon", Float(precision=53)),
    Column("item", JSONB, nullable=False),  # API 원문 한 행
    Column("first_seen_at", DateTime(timezone=True), nullable=False, server_default=func.now()),  # 갱신 안 함
    Column("last_seen_at", DateTime(timezone=True), nullable=False, server_default=func.now()),   # 매주 갱신
)

# ── 통보문 (kma_warning_msg DAG) ─────────────────────────────
# 원문 → 특보 N개 → 구역 M개. 외래키로 원문 없는 특보, 특보 없는 구역이 생기지 않게 DB가 막는다.
MSG_KEY = ("stn_id", "tm_fc", "tm_seq")  # 목록 테이블과 같은 (관서, 발표시각, 발표번호)


def _key_cols():
    # 같은 키 컬럼 세 개를 테이블마다 새로 만든다(Column 객체는 한 테이블에만 붙을 수 있다).
    return [Column("stn_id", Text, primary_key=True), Column("tm_fc", Text, primary_key=True),
            Column("tm_seq", Integer, primary_key=True)]


# 항목 전체를 jsonb로: 지금 안 쓰는 필드(t4 해제 예고, t6 현재 특보 현황 등)도 나중에 쓸 수 있게.
kma_warning_msg_raw = Table(
    "kma_warning_msg_raw", metadata, *_key_cols(),
    Column("item", JSONB, nullable=False),  # API 응답 항목 원문 (t1 제목, t2 구역, t3·t5 시각 ...)
    Column("fetched_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

kma_warning_item = Table(
    "kma_warning_item", metadata, *_key_cols(),
    Column("item_no", Integer, primary_key=True),   # t2·t3의 (1), (2) ...
    Column("warn_type", Text, nullable=False),      # 한파 / 강풍 / 풍랑 ...
    Column("warn_level", Text, nullable=False),     # 주의보 / 경보
    Column("action", Text, nullable=False),         # 발표 / 변경 / 해제 / 연장
    Column("effective_at", DateTime(timezone=True)),  # t3의 시각 (t5는 통보문당 하나라 못 씀)
    Column("area_text", Text, nullable=False),      # t2의 ':' 뒤 원문 (구역 파싱 검증용)
    ForeignKeyConstraint(MSG_KEY, ["kma_warning_msg_raw." + c for c in MSG_KEY]),
)

# ON DELETE CASCADE: 특보 행을 지우면 딸린 구역도 같이 지워져, 재파싱 때 지우기가 한 줄로 끝난다.
kma_warning_area = Table(
    "kma_warning_area", metadata, *_key_cols(),
    Column("item_no", Integer, primary_key=True),
    Column("area_no", Integer, primary_key=True),  # 문장 안 순서
    Column("parent", Text),                 # 경기도 / 동해남부앞바다 / NULL(울릉도.독도처럼 단독)
    Column("area", Text, nullable=False),   # 가평 / 경북북부앞바다 / 울릉도.독도
    Column("note", Text),                   # '여서도 제외' 같은 괄호 부연
    ForeignKeyConstraint(MSG_KEY + ("item_no",),
                         ["kma_warning_item." + c for c in MSG_KEY + ("item_no",)], ondelete="CASCADE"),
)

kma_warning_msg_parse = Table(
    "kma_warning_msg_parse", metadata, *_key_cols(),
    Column("parser_version", Integer, nullable=False),
    Column("parsed_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("error", Text),  # 실패 사유 (성공이면 NULL). 실패해도 원문은 지우지 않는다.
    ForeignKeyConstraint(MSG_KEY, ["kma_warning_msg_raw." + c for c in MSG_KEY]),
)

# Kafka로 보낸 이벤트 기록. 특보 테이블에 외래키를 걸지 않은 이유:
# 재파싱 때 특보 행을 지웠다 다시 넣으므로 FK가 있으면 막힌다.
kma_warning_pub = Table(
    "kma_warning_pub", metadata, *_key_cols(),
    Column("item_no", Integer, primary_key=True),
    Column("sido", Text, primary_key=True),
    Column("sgg", Text, primary_key=True),
    Column("published_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)
