"""행정안전부 한파쉼터 API(재난안전데이터공유플랫폼 DSSP-IF-10804)가 쓸 만한지 확인하는 1회성 점검 스크립트.

실행: python3.9 scripts/check_shelter.py
"""
import json
from collections import Counter
from pathlib import Path

import requests

# 스크립트 위치 기준으로 경로를 잡아, 어느 폴더에서 실행해도 .env/data를 찾게 한다.
ROOT = Path(__file__).resolve().parent.parent
# 발급 내역의 "URL 주소"(/V2/api/DSSP-IF-10804)에 플랫폼 도메인을 붙인 것. 시험 호출로 정상 응답 확인함.
URL = "https://www.safetydata.go.kr/V2/api/DSSP-IF-10804"
OUT = ROOT / "data" / "samples" / "shelter_raw.json"
# 시험 호출에서 5000을 요청해도 1000건만 와서 1000이 최대로 확인됨.
# 전체 약 5.6만 건 ÷ 1000 = 56회 호출 → 일일 한도(1000회) 안에서 충분.
ROWS = 1000
MAX_PAGES = 100  # 실수로 한도를 다 쓰지 않도록 거는 안전장치


def load_key():
    # python-dotenv 대신 직접 읽는다: 외부 라이브러리는 requests만 쓰기로 했기 때문.
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        if line.startswith("SHELTER_API_KEY="):
            key = line.split("=", 1)[1].strip()
            if key:
                return key
    raise SystemExit(".env에 SHELTER_API_KEY 값이 없습니다.")


def fetch_page(key, page):
    params = {"serviceKey": key, "pageNo": page, "numOfRows": ROWS}
    try:
        r = requests.get(URL, params=params, timeout=60)
        data = r.json()
    except (requests.RequestException, ValueError) as e:
        # 예외 메시지에는 키가 포함된 URL이 찍힐 수 있어 오류 종류만 출력한다.
        raise SystemExit("호출 실패(page %d): %s" % (page, type(e).__name__))
    header = data.get("header", {})
    if header.get("resultCode") != "00":
        raise SystemExit("API 오류: %s %s" % (header.get("resultCode"), header.get("resultMsg")))
    return data


def empty(v):
    # 값 없음 = None 또는 공백 문자열. "0000"(자정)은 실제 시간일 수 있어 비어 있다고 보지 않는다.
    return v is None or str(v).strip() == ""


def rate(rows, *fields):
    """fields 중 하나라도 비어 있는 행의 비율(%)."""
    if not rows:
        return 0.0
    return 100.0 * sum(any(empty(r.get(f)) for f in fields) for r in rows) / len(rows)


def is_seoul(r):
    # 도로명주소(RONA_DADDR)를 우선 쓰고, 없으면 지번주소(DADDR)로 판별한다.
    addr = r.get("RONA_DADDR") or r.get("DADDR") or ""
    return addr.startswith("서울")


def main():
    key = load_key()
    first = fetch_page(key, 1)
    total = int(first.get("totalCount") or 0)
    rows = list(first.get("body") or [])
    pages = min(-(-total // ROWS), MAX_PAGES)  # 올림 나눗셈
    for page in range(2, pages + 1):
        rows += fetch_page(key, page).get("body") or []
        print("\r받는 중 %d/%d 페이지" % (page, pages), end="", flush=True)
    print()

    # 원본은 페이지 구분 없이 행 목록으로 저장한다(다음 단계에서 다루기 쉽게).
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")

    print("== 1. 건수 ==")
    print("  API totalCount=%d, 실제 받은 행=%d" % (total, len(rows)))
    # 5.6만 건은 서울 1,500곳 기준으로 보면 많은 편이라, 여러 해 데이터가 섞였는지 연도별로 나눠 본다.
    by_year = Counter(r.get("YR") for r in rows)
    for yr in sorted(by_year, key=str):
        yr_rows = [r for r in rows if r.get("YR") == yr]
        print("  YR=%s: 전체 %d건, 서울 %d건" % (yr, len(yr_rows), sum(map(is_seoul, yr_rows))))
    print("  시설유형(FCLT_TYPE) 분포:", dict(Counter(r.get("FCLT_TYPE") for r in rows)))

    # 채움률은 가장 최근 연도 기준으로 본다: 실제로 쓸 데이터가 최신 연도이기 때문.
    latest = max(by_year, key=str)
    cur = [r for r in rows if r.get("YR") == latest]
    seoul = [r for r in cur if is_seoul(r)]
    print("\n== 2. 빈 값 비율 (YR=%s 기준, 전국 / 서울) ==" % latest)
    for label, fields in [("평일 운영 시작/종료", ("WKDY_OPER_BGNG_HR", "WKDY_OPER_END_HR")),
                          ("위도/경도", ("LAT", "LOT")),
                          ("이용가능인원", ("UTZTN_PSBLTY_TNOP",))]:
        print("  %s: %.1f%% / %.1f%%" % (label, rate(cur, *fields), rate(seoul, *fields)))

    print("\n== 3. 기준일(갱신일) ==")
    for f in ("MDFCN_HR", "INPT_HR"):
        vals = sorted(str(r[f]) for r in cur if not empty(r.get(f)))
        if vals:
            print("  %s: 최소 %s ~ 최대 %s" % (f, vals[0], vals[-1]))

    print("\n== 4. 필드 목록 ==")
    fields = sorted({k for r in rows for k in r})
    print(" ", fields)
    # STDY=토요일, SNDY=일요일, LHLDY=공휴일로 보이는 필드를 골라 채움률을 함께 본다.
    weekend = [f for f in fields if f.startswith(("STDY", "SNDY", "LHLDY"))]
    print("  주말/공휴일 관련 필드:")
    for f in weekend:
        print("    %s: 빈 값 %.1f%% (서울 %.1f%%)" % (f, rate(cur, f), rate(seoul, f)))

    print("\n원본 저장:", OUT.relative_to(ROOT))


if __name__ == "__main__":
    main()
