"""기상청 기상특보 목록(getWthrWrnList)이 프로젝트에 쓸 만한지 확인하는 1회성 점검 스크립트.

실행: python3 scripts/check_warning.py
"""
import json
from datetime import date, timedelta
from pathlib import Path

import requests

# 스크립트 위치 기준으로 경로를 잡아, 어느 폴더에서 실행해도 .env/data를 찾게 한다.
ROOT = Path(__file__).resolve().parent.parent
URL = "https://apis.data.go.kr/1360000/WthrWrnInfoService/getWthrWrnList"
OUT = ROOT / "data" / "samples" / "warning_raw.json"

# 겨울 = 12월 1일 ~ 다음 해 2월 말. 최근 겨울부터 거꾸로 4개를 조회해
# "과거를 몇 년 전까지 조회할 수 있나"를 한 번에 본다.
WINTERS = [
    ("2025-26 겨울", "20251201", "20260228"),
    ("2024-25 겨울", "20241201", "20250228"),
    ("2023-24 겨울", "20231201", "20240229"),
    ("2022-23 겨울", "20221201", "20230228"),
]
# 한 번에 조회 가능한 기간을 알아보려고, 오늘을 끝으로 기간을 점점 늘려 본다.
# 5·6일은 오류 메시지가 말한 "6일" 경계가 실제로 맞는지 확인하려고 넣었다.
PROBE_DAYS = [5, 6, 7, 30, 365]


def load_key():
    # python-dotenv 대신 직접 읽는다: 외부 라이브러리는 requests만 쓰기로 했기 때문.
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        if line.startswith("KMA_API_KEY="):
            key = line.split("=", 1)[1].strip()
            if key:
                return key
    raise SystemExit(".env에 KMA_API_KEY 값이 없습니다.")


def call(key, frm, to, page=1, rows=1000):
    params = {
        "ServiceKey": key,  # 64자 16진수 키라 인코딩 걱정 없이 requests에 맡긴다.
        "pageNo": page,
        "numOfRows": rows,  # 크게 잡아 호출 횟수(일일 트래픽)를 아낀다.
        "dataType": "JSON",
        "fromTmFc": frm,
        "toTmFc": to,
    }
    try:
        r = requests.get(URL, params=params, timeout=30)
    except requests.RequestException as e:
        # 예외 메시지에는 키가 포함된 전체 URL이 찍힐 수 있어 오류 종류만 출력한다.
        raise SystemExit("네트워크 오류: " + type(e).__name__)
    try:
        return r.json()
    except ValueError:
        # 키 오류 등은 dataType=JSON이어도 XML로 오는 경우가 많아 원문 앞부분을 남긴다.
        return {"_text": r.text[:500], "_status": r.status_code}


def parse(resp):
    """응답 → (결과코드, 결과메시지, 전체건수, item 목록)."""
    if "_text" in resp:
        return "NOT_JSON", resp["_text"], 0, []
    res = resp.get("response", {})
    header, body = res.get("header", {}), res.get("body") or {}
    # 결과가 없으면 items가 빈 문자열로 오기도 해서 dict일 때만 꺼낸다.
    items = body.get("items")
    items = items.get("item", []) if isinstance(items, dict) else []
    # 결과가 1건이면 리스트가 아니라 dict 하나로 오는 경우가 있어 리스트로 맞춘다.
    if isinstance(items, dict):
        items = [items]
    return header.get("resultCode"), header.get("resultMsg"), int(body.get("totalCount") or 0), items


def fetch_all(key, frm, to):
    raws, items = [], []
    for page in range(1, 21):  # 실수로 무한 호출하지 않도록 최대 20페이지로 제한
        resp = call(key, frm, to, page)
        raws.append(resp)
        code, msg, total, got = parse(resp)
        items += got
        if code != "00" or not got or len(items) >= total:
            return code, msg, total, items, raws
    return code, msg, total, items, raws


def main():
    key = load_key()
    saved, all_items = {}, []

    today = date.today()
    # 1차 실행에서 "최대 조회 기간은 오늘 기준 6일 전까지" 오류가 나와,
    # 응답 필드·title 형식은 허용 범위인 최근 6일 데이터로 확인한다.
    recent = ("최근 6일", (today - timedelta(days=6)).strftime("%Y%m%d"), today.strftime("%Y%m%d"))

    print("== 1. 기간별 조회 ==")
    for label, frm, to in [recent] + WINTERS:
        code, msg, total, items, raws = fetch_all(key, frm, to)
        saved[label] = raws
        all_items += items
        cold = [i for i in items if "한파" in str(i.get("title", ""))]
        print("\n[%s] %s~%s  결과=%s %s  전체=%d건  한파=%d건"
              % (label, frm, to, code, msg, total, len(cold)))
        if code == "NOT_JSON":
            continue
        # title 안에 종류/등급/구역/해제가 들어 있는지 눈으로 확인하려고 예시를 보여준다.
        # 한파가 없는 시기(예: 11월)에도 title 형식은 볼 수 있게 다른 특보로 대신 보여준다.
        for i in (cold or items)[:3]:
            print("   예시:", i.get("tmFc"), "|", i.get("title"))
        for word in ["주의보", "경보", "해제", "변경"]:
            print("   title에 '%s' 포함: %d건" % (word, sum(word in str(i.get("title")) for i in cold)))

    print("\n== 2. 한 번에 조회 가능한 기간 (오늘 기준 과거 N일) ==")
    for days in PROBE_DAYS:
        frm = (today - timedelta(days=days)).strftime("%Y%m%d")
        # 기간 허용 여부만 보면 되므로 1건만 요청해 트래픽을 아낀다.
        resp = call(key, frm, today.strftime("%Y%m%d"), rows=1)
        code, msg, total, _ = parse(resp)
        saved["probe_%dd" % days] = resp
        print("  %4d일: 결과=%s %s  전체=%d건" % (days, code, msg, total))

    print("\n== 3. 응답 item 필드 이름 ==")
    fields = sorted({k for i in all_items for k in i})
    print(" ", fields if fields else "(item 없음 — 위 결과코드 확인)")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(saved, ensure_ascii=False, indent=2), encoding="utf-8")
    print("\n원본 저장:", OUT.relative_to(ROOT))


if __name__ == "__main__":
    main()
