"""적재용 DB(shelter_db)의 테이블 정의를 SQLAlchemy Core로 모아 두는 곳.

왜 Core인가: SQL 문자열 대신 파이썬 객체로 테이블을 정의하면 컬럼 이름 오타를 코드가 잡아 주고,
DAG마다 흩어진 CREATE 문 없이 이 파일 하나가 스키마의 기준이 된다.
ORM(클래스+Session)은 행 하나씩 다루는 앱에 맞고, 수백~수만 건을 한 번에 넣는 적재엔 Core가 가볍다.

DAG 파일이 아니다: dags/ 폴더가 파이썬 경로에 잡혀 있어 DAG에서 `from tables import ...`로 쓴다.
테이블은 DAG를 하나씩 옮기면서 여기로 옮긴다(현재: kma_warning_list).
"""
from sqlalchemy import Column, DateTime, Integer, MetaData, Table, Text, func

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
