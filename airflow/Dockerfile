# 공식 Airflow 이미지에 Kafka 클라이언트만 얹은 이미지.
# 왜 Dockerfile인가: _PIP_ADDITIONAL_REQUIREMENTS 환경변수로도 설치되지만, 컨테이너가 뜰 때마다
# 다시 설치해 느리고 공식 문서도 시험용으로만 권한다. 이미지에 한 번 구워 두면 재시작이 빠르다.
FROM apache/airflow:3.3.2-python3.12

# apache-airflow도 같은 버전으로 함께 적는 이유: pip가 의존성을 맞추다 Airflow를
# 다른 버전으로 바꿔 버리는 사고를 막는다(공식 문서 권장 방식).
# confluent-kafka 2.16.1: 2026-10-28 패치 버전. 2.16.2는 아직 후보판(rc)이라 쓰지 않는다.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" "confluent-kafka==2.16.1"
