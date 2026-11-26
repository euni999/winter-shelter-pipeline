"""kma.warning.sgg 토픽의 한파 이벤트를 받아 warning_event 테이블에 쌓는 컨슈머.

전달 보장: 프로듀서(Airflow publish)는 "최소 한 번"이라 같은 이벤트가 두 번 올 수 있다.
→ event_id를 기본키로 두고 ON CONFLICT DO NOTHING: 중복은 DB가 걸러서 결과는 한 번만 남는다.
오프셋은 DB 커밋이 끝난 뒤에 직접 커밋한다: 그 사이에 죽으면 같은 메시지를 다시 받지만(→ 위에서 걸러짐)
반대 순서면 "받은 걸로 표시됐는데 DB엔 없는" 유실이 생길 수 있다.
"""
import json
import os
import signal

import psycopg
from confluent_kafka import Consumer, KafkaException

TOPIC = "kma.warning.sgg"

DDL = """
CREATE TABLE IF NOT EXISTS warning_event (
    event_id        TEXT PRIMARY KEY,  -- 프로듀서가 붙인 고유 ID (중복 판별 기준)
    warn_type       TEXT NOT NULL,
    warn_level      TEXT NOT NULL,
    action          TEXT NOT NULL,     -- 발표 / 변경 / 해제 / 연장
    effective_at    TIMESTAMPTZ,
    sido            TEXT NOT NULL,
    sgg             TEXT NOT NULL,
    shelters        INTEGER NOT NULL,  -- 이벤트 발행 시점의 쉼터 수
    capacity        INTEGER NOT NULL,  -- 이벤트 발행 시점의 수용인원 합
    kafka_partition INTEGER NOT NULL,  -- 어디서 읽었는지 (디버깅용)
    kafka_offset    BIGINT  NOT NULL,
    consumed_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""
INSERT = """
INSERT INTO warning_event (event_id, warn_type, warn_level, action, effective_at, sido, sgg,
                           shelters, capacity, kafka_partition, kafka_offset)
VALUES (%(event_id)s, %(warn_type)s, %(warn_level)s, %(action)s, %(effective_at)s, %(sido)s, %(sgg)s,
        %(shelters)s, %(capacity)s, %(partition)s, %(offset)s)
ON CONFLICT (event_id) DO NOTHING
"""

running = True


def stop(*_):
    # docker compose stop은 SIGTERM을 보낸다 → 처리 중인 묶음을 끝내고 깔끔하게 나가게 한다.
    global running
    running = False


def main():
    signal.signal(signal.SIGTERM, stop)
    db = psycopg.connect(host="db", dbname=os.environ["POSTGRES_DB"],
                         user=os.environ["POSTGRES_USER"], password=os.environ["POSTGRES_PASSWORD"])
    with db.cursor() as cur:
        cur.execute(DDL)
    db.commit()

    consumer = Consumer({
        "bootstrap.servers": "kafka:9092",
        # 그룹 ID: Kafka가 이 이름으로 "어디까지 읽었는지(오프셋)"를 기억한다.
        # 컨테이너를 다시 띄워도 이어서 읽고, 같은 그룹으로 여러 개 띄우면 파티션을 나눠 받는다.
        "group.id": "warning-event-writer",
        "auto.offset.reset": "earliest",  # 처음 붙는 그룹이면 토픽의 맨 처음부터 읽는다
        "enable.auto.commit": False,      # 오프셋은 DB 커밋 뒤에 직접 커밋 (위 설명)
    })
    consumer.subscribe([TOPIC])
    print("구독 시작:", TOPIC)
    try:
        while running:
            # 최대 100개씩 묶어서 받는다 → DB 커밋·오프셋 커밋을 메시지마다 하지 않아도 된다.
            msgs = consumer.consume(num_messages=100, timeout=1.0)
            if not msgs:
                continue
            new = 0
            with db.cursor() as cur:
                for m in msgs:
                    if m.error():
                        raise KafkaException(m.error())  # 죽으면 compose가 재시작, 오프셋은 안 넘어감
                    e = json.loads(m.value())
                    cur.execute(INSERT, {**e, "partition": m.partition(), "offset": m.offset()})
                    new += cur.rowcount  # 중복이면 0
            db.commit()
            consumer.commit(asynchronous=False)  # DB에 확실히 들어간 뒤에 "여기까지 읽음" 표시
            print("받은 %d건: 새 이벤트 %d, 중복 %d" % (len(msgs), new, len(msgs) - new))
    finally:
        consumer.close()  # 그룹에서 빠진다고 알려 다른 컨슈머가 바로 파티션을 넘겨받게 한다
        db.close()


if __name__ == "__main__":
    main()
