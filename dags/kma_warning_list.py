"""기상청 특보 목록(getWthrWrnList)을 하루 1번 받아 적재용 Postgres에 중복 없이 쌓는 DAG.

왜 매일 쌓나: 이 API는 "오늘 기준 6일 전까지"만 조회된다(사전점검에서 확인).
지나간 특보는 API에서 사라지므로, 우리 DB에 계속 모아 두는 것이 이 DAG의 목적이다.
"""
import pendulum
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Variable, dag, task

URL = "https://apis.data.go.kr/1360000/WthrWrnInfoService/getWthrWrnList"
KST = pendulum.timezone("Asia/Seoul")
# 테이블 구조는 dags/tables.py(SQLAlchemy Core)에 정의돼 있다.


@dag(
    # 스케줄 기준 시간대를 한국으로: 자정 실행이 한국 자정이 되게 한다.
    start_date=pendulum.datetime(2026, 11, 13, tz=KST),
    schedule="@daily",
    # 과거 날짜를 몰아서 실행하지 않는다: 어차피 API가 6일 전까지만 줘서 의미가 없다.
    catchup=False,
    # 일시적인 네트워크/API 오류는 5분 뒤 두 번까지 다시 시도.
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=5)},
    tags=["kma", "warning"],
)
def kma_warning_list():
    @task
    def fetch():
        # 실행 날짜(logical date)가 아니라 "지금" 기준 최근 6일을 받는다.
        # API가 오늘 기준으로만 열려 있어, 예전 실행을 다시 돌려도 항상 유효한 범위가 되게 하려고.
        # 매일 6일치를 받으니 겹치는 부분이 생기지만, 그건 INSERT의 ON CONFLICT가 걸러낸다.
        # 덕분에 며칠(최대 5일) 실행이 빠져도 다음 실행이 빈 구간을 메운다.
        today = pendulum.now(KST)
        params = {
            "ServiceKey": Variable.get("KMA_API_KEY"),
            "pageNo": 1,
            "numOfRows": 1000,  # 6일치가 보통 수백 건이라 대부분 한 페이지로 끝난다.
            "dataType": "JSON",
            "fromTmFc": today.subtract(days=6).format("YYYYMMDD"),
            "toTmFc": today.format("YYYYMMDD"),
        }
        rows = []
        for page in range(1, 11):  # 혹시 모를 무한 호출 방지
            params["pageNo"] = page
            try:
                r = requests.get(URL, params=params, timeout=30)
                r.raise_for_status()
                data = r.json()
            except (requests.RequestException, ValueError) as e:
                # 원래 예외 메시지엔 키가 든 URL이 찍힐 수 있어 오류 종류만 남기고 원인 연결도 끊는다.
                raise RuntimeError("특보 API 호출 실패: " + type(e).__name__) from None
            header = data["response"]["header"]
            if header["resultCode"] == "03":  # NO_DATA: 특보가 없던 기간 → 정상, 0건
                break
            if header["resultCode"] != "00":
                raise RuntimeError("특보 API 오류: %s %s" % (header["resultCode"], header["resultMsg"]))
            body = data["response"]["body"]
            items = body["items"]["item"]
            items = [items] if isinstance(items, dict) else items  # 1건이면 dict로 온다
            # 컬럼 이름을 키로 둔 dict: SQLAlchemy insert에 그대로 넘긴다(순서 실수가 없다).
            rows += [{"stn_id": str(i["stnId"]), "tm_fc": str(i["tmFc"]), "tm_seq": int(i["tmSeq"]),
                      "title": i["title"]} for i in items]
            if len(rows) >= int(body["totalCount"]):
                break
        print("받은 특보 %d건 (%s~%s)" % (len(rows), params["fromTmFc"], params["toTmFc"]))
        # 태스크 사이에 넘기는 값(XCom)은 메타DB에 저장된다. 수백 건 텍스트라 부담 없는 크기.
        return rows

    @task
    def load(rows):
        # 태스크 안에서 import: tables.py는 dags/ 폴더에 있어 DAG 파일을 읽을 때마다 불러올 필요가 없다.
        from sqlalchemy import func, select
        from sqlalchemy.dialects.postgresql import insert
        from tables import kma_warning_list as t

        engine = PostgresHook(postgres_conn_id="shelter_db").get_sqlalchemy_engine()
        # engine.begin(): 블록이 끝나면 커밋, 실패하면 롤백하고 연결도 반납한다.
        # → 예전처럼 드라이버(psycopg 2/3)마다 다른 `with conn` 동작을 신경 쓸 필요가 없다.
        with engine.begin() as conn:
            t.create(conn, checkfirst=True)  # CREATE TABLE IF NOT EXISTS와 같다
            if rows:  # 빈 목록을 넘기면 값 없는 INSERT 한 번으로 바뀌어 NOT NULL 오류가 난다
                # 이미 있는 특보(기본키 충돌)는 건너뛴다 → 몇 번을 다시 돌려도 결과가 같다(멱등).
                conn.execute(insert(t).on_conflict_do_nothing(index_elements=t.primary_key.columns), rows)
            total = conn.scalar(select(func.count()).select_from(t))
        print("받은 %d건 처리, 테이블 전체 %d건" % (len(rows), total))

    load(fetch())


kma_warning_list()
