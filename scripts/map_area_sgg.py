"""특보구역 이름 → 쉼터 주소의 (시도, 시군구) 매핑표(warning_area_sgg)를 만드는 스크립트.

입력(표준입력, psql 한 번으로 뽑음):
  A|상위구역|구역   ← kma_warning_area의 특보구역
  S|시도|시군구     ← shelter 테이블의 시군구 (이름을 맞춰 볼 정답 목록)
출력: 매핑 결과와 미매핑 목록. --sql을 주면 매핑표와 미매핑 뷰를 다시 만드는 SQL을 출력 → psql로 적재.
(로컬은 requests만 쓰는 규칙이라 DB 드라이버 대신 psql로 주고받는다)

왜 이름으로 맞추나: 쉼터 데이터엔 시군구 코드가 없고 도로명주소 문자열만 있다.
경계 지도로 좌표를 겹치는 방법이 더 정확하지만 지도 라이브러리가 필요해 지금은 쓰지 않는다.
"""
import sys

# 통보문은 옛 시도 이름을, 쉼터 주소는 현재 이름을 쓴다 → 쉼터 쪽 이름으로 통일.
# 전남·광주는 쉼터 주소에서 '전남광주통합특별시'로 합쳐져 있어 둘 다 그쪽으로 보낸다.
SIDO = {
    "서울": "서울특별시", "부산": "부산광역시", "대구": "대구광역시", "인천": "인천광역시",
    "광주": "전남광주통합특별시", "대전": "대전광역시", "울산": "울산광역시", "세종": "세종특별자치시",
    "경기도": "경기도", "강원도": "강원특별자치도", "충청북도": "충청북도", "충청남도": "충청남도",
    "전라북도": "전북특별자치도", "전북자치도": "전북특별자치도",  # 11/19부터 '전북자치도' 표기도 나옴
    "전라남도": "전남광주통합특별시",
    "경상북도": "경상북도", "경상남도": "경상남도", "제주도": "제주특별자치도",
}

# 규칙으로 못 푸는 것만 손으로 적는다. 섬·읍면은 이름만 봐서는 소속 시군구를 알 수 없다.
# 값이 목록인 이유: 서울 권역처럼 구역 하나가 시군구 여러 개에 걸칠 수 있어서.
# 서울 4개 권역은 아직 넣지 않았다: 통보문 표기와 구 목록을 실제 특보로 확인한 뒤 추가(추측 금지).
MANUAL = {
    "흑산도.홍도": [("전남광주통합특별시", "신안군")],
    "거문도.초도": [("전남광주통합특별시", "여수시")],
    "완도여서도": [("전남광주통합특별시", "완도군")],
    "영광낙월면": [("전남광주통합특별시", "영광군")],
    "추자도": [("제주특별자치도", "제주시")],
    "인천영종": [("인천광역시", "영종구")],  # 2026 인천 개편으로 중구 → 영종구
    "울릉도.독도": [("경상북도", "울릉군")],  # 상위구역 없이 나와 시도까지 지정
    "서해5도": [("인천광역시", "옹진군")],
    "연평도.우도": [("인천광역시", "옹진군")],  # 서해5도(...)의 하위 구역으로도 나온다
    "백령도.대청도": [("인천광역시", "옹진군")],
    "군산어청도": [("전북특별자치도", "군산시")],
    "군산옥도면": [("전북특별자치도", "군산시")],
    "부안위도면": [("전북특별자치도", "부안군")],
    # 한라산 일대라 제주시·서귀포시 양쪽에 걸친다 → 다중 매핑이 필요했던 첫 사례
    "제주도산지": [("제주특별자치도", "제주시"), ("제주특별자치도", "서귀포시")],
    # '제주도(제주시북부, 서귀포시남부 제외)' = 제주 전역에서 일부 구역 제외(11/20 강풍).
    # 제외 구역은 시군구보다 작아서 시군구 단위로는 두 시 모두 해당한다.
    "제주도": [("제주특별자치도", "제주시"), ("제주특별자치도", "서귀포시")],
}

# 시군구를 더 잘게 나눈 꼬리말 (강릉산지, 양평서부, 하동남부 …)
SUFFIXES = ["산지", "평지", "도서", "산간", "중산간", "동부", "서부", "남부", "북부", "중부",
            "동남부", "서남부", "서북부", "동북부", "중북부"]

# 매핑표는 이 스크립트의 규칙이 정답 → 매번 지우고 새로 만든다(규칙에서 뺀 행이 남지 않게).
# 뷰가 표를 참조하므로 뷰부터 지운다. 전체를 BEGIN/COMMIT으로 묶어 중간 상태가 안 보이게 한다.
# parent가 없는 구역(울릉도.독도 등)은 ''로 저장: 기본키 컬럼엔 NULL을 넣을 수 없어서.
TABLE_SQL = """DROP VIEW IF EXISTS warning_area_unmapped;
DROP TABLE IF EXISTS warning_area_sgg;
CREATE TABLE warning_area_sgg (
    parent TEXT NOT NULL,  -- 통보문 상위구역 (없으면 '')
    area   TEXT NOT NULL,  -- 통보문 특보구역 이름
    sido   TEXT NOT NULL,  -- 쉼터 주소 기준 시도
    sgg    TEXT NOT NULL,  -- 쉼터 주소 기준 시군구 (세종은 시 이름)
    PRIMARY KEY (parent, area, sgg)  -- 구역 하나가 여러 시군구에 걸칠 수 있어 sgg까지 키
);"""
INSERT_SQL = "INSERT INTO warning_area_sgg VALUES ('%s','%s','%s','%s');"
# 통보문에 새 육상 구역이 나왔는데 매핑표에 없으면 여기 나타난다 → 규칙/예외를 추가할 신호.
# 바다 판정은 is_sea와 같은 규칙.
VIEW_SQL = """CREATE VIEW warning_area_unmapped AS
SELECT DISTINCT coalesce(a.parent, '') AS parent, a.area
FROM kma_warning_area a
LEFT JOIN warning_area_sgg m ON m.parent = coalesce(a.parent, '') AND m.area = a.area
WHERE m.area IS NULL
  AND coalesce(a.parent, '') || a.area NOT LIKE '%바다%'
  AND coalesce(a.parent, '') || a.area NOT LIKE '%평수구역%'
  AND coalesce(a.parent, '') || a.area NOT LIKE '%해상%';"""

# 이 스크립트에 넣을 입력을 뽑는 쿼리 (README 대신 여기 둔다: 스크립트와 항상 같이 바뀌어야 해서)
INPUT_QUERY = """SELECT DISTINCT 'A', coalesce(parent, '-'), area FROM kma_warning_area
UNION ALL SELECT DISTINCT 'S', sido, sgg FROM shelter WHERE sgg IS NOT NULL"""


def is_sea(parent, area):
    # 바다 구역은 쉼터와 무관하므로 매핑 대상에서 뺀다.
    return any(w in (parent or "") + area for w in ("바다", "평수구역", "해상"))  # 해상: 동해중부전해상


def stems(area):
    # 꼬리말 하나만 골라 떼면 '하동남부'가 '하|동남부'로 잘린다(실제는 '하동|남부').
    # 그래서 가능한 자르기를 모두 후보로 내고, 실제 시군구와 맞는 것만 match에서 고른다.
    return {area} | {area[: -len(s)] for s in SUFFIXES if area.endswith(s) and len(area) > len(s)}


def match(sido, area, sgg):
    """[(시도, 시군구), ...]를 돌려준다. 못 찾으면 빈 목록."""
    if area in MANUAL:
        return MANUAL[area]
    if sido is None:
        return []
    if sido == "세종특별자치시":
        return [(sido, sido)]
    names = stems(area)
    # '제주시동부'처럼 행정단위까지 붙은 이름은 그대로, '춘천'처럼 없으면 시/군/구를 붙여 찾는다.
    hits = [g for g in sgg.get(sido, ()) if g in names or g[:-1] in names]
    return [(sido, hits[0])] if len(hits) == 1 else []  # 여러 개면 사람이 봐야 한다


def read_input():
    areas, sgg = [], {}
    for line in sys.stdin:
        kind, a, b = line.rstrip("\n").split("|", 2)
        if kind == "A":
            areas.append((None if a == "-" else a, b))
        else:
            sgg.setdefault(a, set()).add(b)
    if not sgg:  # shelter 테이블이 비면 모든 구역이 미매핑으로 나와 표가 비어 버린다 → 멈춤
        raise SystemExit("시군구 목록(S|)이 없습니다. shelter 테이블을 먼저 채우세요.")
    return areas, sgg


def to_sql(mapped):
    q = lambda s: s.replace("'", "''")  # 작은따옴표가 든 이름에 대비한 이스케이프
    print("BEGIN;")
    print(TABLE_SQL)
    for p, a, ms in mapped:
        for sido, g in ms:
            print(INSERT_SQL % (q(p or ""), q(a), q(sido), q(g)))
    print(VIEW_SQL)
    print("COMMIT;")


def main():
    if "--query" in sys.argv:  # 입력 쿼리만 출력 (psql -c 에 넘기기용)
        return print(INPUT_QUERY)
    areas, sgg = read_input()
    mapped, missing, sea = [], [], 0
    for parent, area in areas:
        if is_sea(parent, area):
            sea += 1
            continue
        ms = match(SIDO.get(parent), area, sgg)
        (mapped if ms else missing).append((parent, area, ms))
    if "--sql" in sys.argv:  # psql로 바로 넘길 SQL만 출력 (점검 결과는 출력하지 않음)
        return to_sql(mapped)
    for p, a, ms in mapped:
        print("%s %s → %s" % (p or "-", a, ", ".join("%s %s" % m for m in ms)))
    print("\n매핑 %d / 미매핑 %d / 바다 제외 %d" % (len(mapped), len(missing), sea))
    for p, a, _ in missing:
        print("미매핑: %s %s" % (p or "-", a))


if __name__ == "__main__":
    main()
