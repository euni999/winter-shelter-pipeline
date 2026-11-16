"""특보구역 이름 → 쉼터 주소의 (시도, 시군구) 매핑 규칙 점검.

입력: 표준입력으로 "상위구역|구역" 줄 (kma_warning_area에서 psql로 뽑은 것)
출력: 매핑 결과와 미매핑 목록. --sql을 주면 매핑표(warning_area_sgg)와 미매핑 뷰를
      만드는 SQL을 출력한다 → psql로 넘겨 적재. (로컬은 requests만 쓰는 규칙이라 DB 드라이버 대신 psql)

왜 이름으로 맞추나: 쉼터 데이터엔 시군구 코드가 없고 도로명주소 문자열만 있다.
경계 지도로 좌표를 겹치는 방법이 더 정확하지만 지도 라이브러리가 필요해 지금은 쓰지 않는다.
"""
import json
import sys

# 통보문은 옛 시도 이름을, 쉼터 주소는 현재 이름을 쓴다 → 쉼터 쪽 이름으로 통일.
# 전남·광주는 쉼터 주소에서 '전남광주통합특별시'로 합쳐져 있어 둘 다 그쪽으로 보낸다.
SIDO = {
    "서울": "서울특별시", "부산": "부산광역시", "대구": "대구광역시", "인천": "인천광역시",
    "광주": "전남광주통합특별시", "대전": "대전광역시", "울산": "울산광역시", "세종": "세종특별자치시",
    "경기도": "경기도", "강원도": "강원특별자치도", "충청북도": "충청북도", "충청남도": "충청남도",
    "전라북도": "전북특별자치도", "전라남도": "전남광주통합특별시",
    "경상북도": "경상북도", "경상남도": "경상남도", "제주도": "제주특별자치도",
}

# 규칙으로 못 푸는 것만 손으로 적는다. 섬·읍면은 이름만 봐서는 소속 시군구를 알 수 없다.
# 상위구역이 없는 줄(울릉도.독도, 서해5도)도 여기서 시도까지 정해 준다.
MANUAL = {
    "흑산도.홍도": ("전남광주통합특별시", "신안군"),
    "거문도.초도": ("전남광주통합특별시", "여수시"),
    "완도여서도": ("전남광주통합특별시", "완도군"),
    "영광낙월면": ("전남광주통합특별시", "영광군"),
    "추자도": ("제주특별자치도", "제주시"),
    "인천영종": ("인천광역시", "영종구"),  # 2026 인천 개편으로 중구 → 영종구
    "울릉도.독도": ("경상북도", "울릉군"),
    "서해5도": ("인천광역시", "옹진군"),
}

# 시군구를 더 잘게 나눈 꼬리말.
SUFFIXES = ["산지", "평지", "도서", "동부", "서부", "남부", "북부", "중부",
            "동남부", "서남부", "서북부", "동북부"]


def is_sea(parent, area):
    # 바다 구역은 쉼터와 무관하므로 매핑 대상에서 뺀다.
    return any(w in (parent or "") + area for w in ("바다", "평수구역"))


def load_sgg(path="data/samples/shelter_raw.json"):
    """쉼터 주소 앞 두 단어로 {시도: {시군구, ...}}를 만든다."""
    out = {}
    for r in json.load(open(path, encoding="utf-8")):
        a = (r.get("RONA_DADDR") or r.get("DADDR") or "").split()
        if len(a) < 2:
            continue  # 주소 없는 쉼터는 시군구를 알 수 없어 건너뜀
        # 세종은 시군구가 없어 두 번째 단어가 도로명 → 시 전체를 하나로 본다.
        out.setdefault(a[0], set()).add(a[0] if a[0] == "세종특별자치시" else a[1])
    return out


def stems(area):
    # 꼬리말 하나만 골라 떼면 '하동남부'가 '하|동남부'로 잘린다(실제는 '하동|남부').
    # 그래서 가능한 자르기를 모두 후보로 내고, 실제 시군구와 맞는 것만 match에서 고른다.
    return {area} | {area[: -len(s)] for s in SUFFIXES if area.endswith(s) and len(area) > len(s)}


def match(sido, area, sgg):
    if area in MANUAL:
        return MANUAL[area]
    if sido is None:
        return None
    if sido == "세종특별자치시":
        return (sido, sido)
    names = stems(area)
    # '제주시동부'처럼 행정단위까지 붙은 이름은 그대로, '춘천'처럼 없으면 시/군/구를 붙여 찾는다.
    hits = [g for g in sgg.get(sido, ()) if g in names or g[:-1] in names]
    return (sido, hits[0]) if len(hits) == 1 else None  # 0개·여러 개면 사람이 봐야 한다


# parent가 없는 구역(울릉도.독도 등)은 ''로 저장: 기본키 컬럼엔 NULL을 넣을 수 없어서.
# DO UPDATE: 이 스크립트의 규칙이 정답이므로, 규칙을 고쳐 다시 넣으면 기존 행도 바뀌어야 한다.
TABLE_SQL = """CREATE TABLE IF NOT EXISTS warning_area_sgg (
    parent TEXT NOT NULL,  -- 통보문 상위구역 (없으면 '')
    area   TEXT NOT NULL,  -- 통보문 특보구역 이름
    sido   TEXT NOT NULL,  -- 쉼터 주소 기준 시도
    sgg    TEXT NOT NULL,  -- 쉼터 주소 기준 시군구 (세종은 시 이름)
    PRIMARY KEY (parent, area)
);"""
UPSERT_SQL = ("INSERT INTO warning_area_sgg VALUES ('%s','%s','%s','%s') "
              "ON CONFLICT (parent, area) DO UPDATE SET sido = EXCLUDED.sido, sgg = EXCLUDED.sgg;")
# 통보문에 새 육상 구역이 나왔는데 매핑표에 없으면 여기 나타난다 → 규칙/예외를 추가할 신호.
# 바다 판정은 is_sea와 같은 규칙.
VIEW_SQL = """CREATE OR REPLACE VIEW warning_area_unmapped AS
SELECT DISTINCT coalesce(a.parent, '') AS parent, a.area
FROM kma_warning_area a
LEFT JOIN warning_area_sgg m ON m.parent = coalesce(a.parent, '') AND m.area = a.area
WHERE m.area IS NULL
  AND coalesce(a.parent, '') || a.area NOT LIKE '%바다%'
  AND coalesce(a.parent, '') || a.area NOT LIKE '%평수구역%';"""


def to_sql(mapped):
    q = lambda s: s.replace("'", "''")  # 작은따옴표가 든 이름에 대비한 이스케이프
    print("BEGIN;")  # 전부 들어가거나 하나도 안 들어가게
    print(TABLE_SQL)
    for p, a, m in mapped:
        print(UPSERT_SQL % (q(p or ""), q(a), q(m[0]), q(m[1])))
    print(VIEW_SQL)
    print("COMMIT;")


def main():
    sgg = load_sgg()
    mapped, missing, sea = [], [], 0
    for line in sys.stdin:
        parent, _, area = line.strip().partition("|")
        parent = None if parent in ("", "-") else parent
        if is_sea(parent, area):
            sea += 1
            continue
        m = match(SIDO.get(parent), area, sgg)
        (mapped if m else missing).append((parent, area, m))
    if "--sql" in sys.argv:  # psql로 바로 넘길 SQL만 출력 (점검 결과는 출력하지 않음)
        return to_sql(mapped)
    for p, a, m in mapped:
        print("%s %s → %s %s" % (p or "-", a, m[0], m[1]))
    print("\n매핑 %d / 미매핑 %d / 바다 제외 %d" % (len(mapped), len(missing), sea))
    for p, a, _ in missing:
        print("미매핑: %s %s" % (p or "-", a))


if __name__ == "__main__":
    main()
