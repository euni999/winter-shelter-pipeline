# Kafka 이벤트를 받아 DB에 쓰는 컨슈머 이미지.
# python 3.11.17: 3.11.18(2026-11-24)은 나온 지 이틀이라 피하고 한 단계 아래 패치를 쓴다.
# slim: 컴파일러 등이 빠진 작은 이미지. 두 라이브러리 모두 미리 빌드된 wheel이 있어 컴파일이 필요 없다.
FROM python:3.11.17-slim-bookworm

# confluent-kafka는 Airflow 쪽(프로듀서)과 같은 2.16.1,
# psycopg는 Airflow 이미지 안의 드라이버와 같은 3.3.5로 맞춘다(같은 DB를 쓰는 두 쪽의 동작을 같게).
RUN pip install --no-cache-dir "confluent-kafka==2.16.1" "psycopg[binary]==3.3.5"

# print가 버퍼에 쌓이지 않고 바로 docker logs에 보이게 한다.
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY consumer.py .
CMD ["python", "consumer.py"]
