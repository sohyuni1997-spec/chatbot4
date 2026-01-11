"""
hybrid.py
- 2026년 1월 대응용 하이브리드 엔진 (Python 분석 + AI 전략 + Python 검증 + Python 폴백)
- app (3).py 호출 시그니처 완전 호환

✅ FIX 1) step3_analyze_destination_capacity()
- "같은날 같은라인" (예: 2026-01-21_조립1) CAPA도 capa_status에 포함
  → increase 시 목적지가 question_date_target_line인 경우 "목적지 CAPA 정보 없음"으로 전량 탈락하던 문제 해결

✅ FIX 2) generate_full_report()
- 최종 조치 계획 출력 시, 동일 item/from/to는 합산해서 1줄로 표시
  → 같은 내용이 1PLT씩 여러 줄로 쪼개져 보이던 문제 개선(표시만 변경, 계산 로직 불변)
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple, Optional
from copy import deepcopy

import pandas as pd
import google.generativeai as genai


# ========================================================================
# 전역 변수 (앱에서 넘겨준 today/capa_limits를 여기서 세팅)
# ========================================================================
TODAY = None
CAPA_LIMITS = None



# 사람 같은 분산: T6 '같은날 타라인 이송'은 우선 1회, 최대 5PLT까지만 사용
MAX_T6_SAMEDAY_SHIFT_PLTS = 5
ENGINE_VERSION = "HUMANPLAN_V5"
def initialize_globals(today, capa_limits):
    global TODAY, CAPA_LIMITS
    TODAY = today
    CAPA_LIMITS = capa_limits


# ========================================================================
# 유틸
# ========================================================================

def _safe_date(s: str) -> datetime.date:
    return datetime.strptime(str(s)[:10], "%Y-%m-%d").date()


def _safe_str_date(d) -> str:
    if isinstance(d, str):
        return d[:10]
    return d.strftime("%Y-%m-%d")


def _coerce_is_workday(v: Any) -> bool:
    """is_workday 값이 bool/str/숫자 등으로 섞여 들어와도 안전하게 True/False로 변환"""
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    s = str(v).strip().lower()
    if s in ("true", "t", "1", "y", "yes"):
        return True
    if s in ("false", "f", "0", "n", "no", "", "none", "null"):
        return False
    try:
        return bool(int(float(s)))
    except Exception:
        return False


def is_workday_in_db(plan_df: pd.DataFrame, date_str: str) -> bool:
    """특정 날짜가 가동일인지 확인 (is_workday 컬럼 사용)"""
    if plan_df.empty or "is_workday" not in plan_df.columns:
        # is_workday가 없으면 "가동일 체크 불가"로 보고 True 처리(운영 정책에 따라 False로 바꿔도 됨)
        return True

    date_info = plan_df[plan_df["plan_date"] == date_str]
    if date_info.empty:
        return False
    return _coerce_is_workday(date_info.iloc[0]["is_workday"])
def get_workdays_from_db(plan_df: pd.DataFrame, start_date_str: str, direction="future", days_count=10) -> List[str]:
    """DB의 is_workday 기반으로 가동일 리스트 반환"""
    if plan_df.empty or "is_workday" not in plan_df.columns:
        return []

    db_dates = plan_df[["plan_date", "is_workday"]].drop_duplicates().sort_values("plan_date").copy()
    db_dates["__workday"] = db_dates["is_workday"].apply(_coerce_is_workday)

    if direction == "future":
        available = db_dates[(db_dates["plan_date"] >= start_date_str) & (db_dates["__workday"] == True)]
        return available["plan_date"].head(days_count).tolist()

    # 과거: TODAY 이후만 (고정기간/정책에 맞게 조정 가능)
    today_str = TODAY.strftime("%Y-%m-%d") if TODAY else "1900-01-01"
    available = db_dates[
        (db_dates["plan_date"] < start_date_str)
        & (db_dates["plan_date"] > today_str)
        & (db_dates["__workday"] == True)
    ]
    return available["plan_date"].tail(days_count).tolist()
def _normalize_line_guess(question: str) -> Optional[str]:
    if "조립1" in question:
        return "조립1"
    if "조립2" in question:
        return "조립2"
    if "조립3" in question:
        return "조립3"
    return None


# ========================================================================
# CAPA 이벤트(잔업/특근) 자동 제안 유틸 (데모용)
# - '생산량 이동(Δ)'과 섞이지 않도록, 이벤트는 보고서에만 별도 표시 (방법 A)
# ========================================================================

def _round_up_to_multiple(x: int, base: int) -> int:
    if base <= 0:
        return x
    return ((x + base - 1) // base) * base

def _suggest_capa_events_auto(
    plan_df: pd.DataFrame,
    question_date: str,
    target_line: str,
    shortfall_qty: int,
    plt_base: int,
    max_days: int = 2,
) -> List[Dict[str, Any]]:
    """달성률이 너무 낮고(CAPA 부족) 미달이 남을 때, 잔업/특근 CAPA 상향을 자동으로 제안.
    - 이벤트는 '추가 생산'이 아니라 '수용 CAPA 증가'로만 처리(Δ 표에 넣지 않음).
    """
    if shortfall_qty <= 0:
        return []

    workdays = get_workdays_from_db(plan_df, start_date_str=question_date, direction="future", days_count=50)
    candidates = [d for d in workdays if d > question_date][: max_days]
    if not candidates:
        return []

    need = _round_up_to_multiple(int(shortfall_qty), max(plt_base, 1))

    events: List[Dict[str, Any]] = []
    if len(candidates) == 1:
        events.append({"date": candidates[0], "line": target_line, "type": "특근", "delta_capa": need})
        return events

    first = _round_up_to_multiple(need // 2, max(plt_base, 1))
    second = max(0, need - first)

    if first > 0:
        events.append({"date": candidates[0], "line": target_line, "type": "특근", "delta_capa": first})
    if second > 0:
        events.append({"date": candidates[1], "line": target_line, "type": "잔업", "delta_capa": second})
    return events

def _apply_capa_events_to_status(
    capa_status: Dict[str, Dict[str, Any]],
    events: List[Dict[str, Any]],
    capa_limits: Dict[str, int],
):
    """capa_status에 이벤트를 반영(유효 CAPA = max/remaining 증가)."""
    for ev in events:
        d = ev["date"]
        ln = ev["line"]
        inc = int(ev.get("delta_capa", 0) or 0)
        if inc <= 0:
            continue
        key = f"{d}_{ln}"
        if key not in capa_status:
            base = int(capa_limits.get(ln, 0) or 0)
            capa_status[key] = {"max": base, "remaining": base}
        capa_status[key]["max"] = int(capa_status[key].get("max", 0) or 0) + inc
        capa_status[key]["remaining"] = int(capa_status[key].get("remaining", 0) or 0) + inc

def _format_capa_events_md(events: List[Dict[str, Any]]) -> str:
    if not events:
        return ""
    out = ["## 🛠 CAPA 이벤트(잔업/특근) 적용"]
    for ev in events:
        out.append(f"- {ev['date']} {ev['line']}: **{ev['type']}**으로 유효 CAPA **+{int(ev['delta_capa']):,}개**")
    return "\n".join(out) + "\n\n"

def _infer_target_line(question: str, plan_df: pd.DataFrame, question_date: str) -> Optional[str]:
    """질문에 라인 명시가 없으면, 품목 키워드/당일 최대 물량 라인으로 추론"""
    direct = _normalize_line_guess(question)
    if direct:
        return direct

    if plan_df.empty:
        return None

    date_data = plan_df[plan_df["plan_date"] == question_date]
    if date_data.empty:
        return None

    q_up = question.upper()

    # 특정 키워드가 있으면 해당 품목이 찍히는 라인을 우선
    for key in ["T6", "A2XX", "J9", "BERGSTROM"]:
        if key in q_up:
            lines = date_data[date_data["product_name"].str.contains(key, case=False, na=False)]["line"].unique()
            if len(lines) > 0:
                return str(lines[0])

    # 그 외: 당일 qty_1차 합이 가장 큰 라인
    if "qty_1차" in date_data.columns:
        line_qty = date_data.groupby("line")["qty_1차"].sum()
        if not line_qty.empty:
            return str(line_qty.idxmax())

    return None


# ========================================================================
# 1~3단계: 데이터 수사
# ========================================================================

def step1_list_current_stock(plan_df: pd.DataFrame, target_date: str, target_line: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    current = plan_df[(plan_df["plan_date"] == target_date) & (plan_df["line"] == target_line)].copy()
    if current.empty:
        return None, "해당 날짜/라인에 생산 계획이 없습니다."

    if "qty_1차" not in current.columns or "plt" not in current.columns:
        return None, "plan_df에 qty_1차 또는 plt 컬럼이 없습니다."

    total = int(current["qty_1차"].sum())
    items = []
    for _, row in current.iterrows():
        q = int(row.get("qty_1차", 0) or 0)
        if q <= 0:
            continue
        items.append(
            {
                "name": row.get("product_name", ""),
                "qty_1차": q,
                "plt": int(row.get("plt", 1) or 1),
            }
        )

    return {"date": target_date, "line": target_line, "total": total, "items": items}, None


def step2_calculate_cumulative_slack(plan_df: pd.DataFrame, stock_result: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    각 품목의 누적 납기 여유 계산
    - cumsum 기준: qty_0차 vs qty_1차
    - 이동가능 max_movable 산출
    """
    items_with_slack = []
    target_date = stock_result["date"]

    needed_cols = {"product_name", "plan_date", "qty_0차", "qty_1차", "plt"}
    if not needed_cols.issubset(set(plan_df.columns)):
        # 최소한 돌아가게: 이동 가능성 판단을 conservative 하게 처리
        for item in stock_result["items"]:
            items_with_slack.append(
                {
                    "name": item["name"],
                    "qty_1차": item["qty_1차"],
                    "plt": item["plt"],
                    "cumsum_target": 0,
                    "cumsum_actual": 0,
                    "max_movable": 0,
                    "last_due": "미확인",
                    "buffer_days": 0,
                    "movable": False,
                }
            )
        return items_with_slack

    for item in stock_result["items"]:
        name = item["name"]
        series = plan_df[plan_df["product_name"] == name].sort_values("plan_date").copy()
        if series.empty:
            continue

        series["cumsum_0차"] = series["qty_0차"].cumsum()
        series["cumsum_1차"] = series["qty_1차"].cumsum()

        today_row = series[series["plan_date"] == target_date]
        if today_row.empty:
            continue
        today_row = today_row.iloc[0]

        cumsum_target = int(today_row["cumsum_0차"])
        cumsum_actual = int(today_row["cumsum_1차"])
        max_movable_cumsum = cumsum_actual - cumsum_target

        future_demand = int(series[series["plan_date"] > target_date]["qty_0차"].sum())
        future_prod = int(series[series["plan_date"] > target_date]["qty_1차"].sum())
        future_slack = future_prod - future_demand

        if max_movable_cumsum > 0:
            max_movable = max_movable_cumsum
        else:
            if future_slack >= 0:
                max_movable = int(item["qty_1차"])
            else:
                max_movable = max(0, int(item["qty_1차"]) + future_slack)

        due_dates = series[series["qty_0차"] > 0]["plan_date"].tolist()
        last_due = max(due_dates) if due_dates else "미확인"

        if last_due != "미확인":
            last_due_dt = _safe_date(last_due)
            target_dt = _safe_date(target_date)
            buffer_days = (last_due_dt - target_dt).days
        else:
            buffer_days = 999

        plt = int(item["plt"])
        items_with_slack.append(
            {
                "name": name,
                "qty_1차": int(item["qty_1차"]),
                "plt": plt,
                "cumsum_target": cumsum_target,
                "cumsum_actual": cumsum_actual,
                "max_movable": int(max_movable),
                "last_due": last_due,
                "buffer_days": int(buffer_days),
                "movable": int(max_movable) >= plt,
            }
        )

    return items_with_slack


def step3_analyze_destination_capacity(
    plan_df: pd.DataFrame,
    target_date: str,
    target_line: str,
    capa_limits: Dict[str, int],
) -> Dict[str, Dict[str, Any]]:
    """
    CAPA 현황:
    - ✅ 같은날: 조립1/2/3 모두 (target_line 포함)
    - ✅ 동일라인 미래 가동일(최대 N개)  (단, 전체 납기/데이터 범위 밖으로는 확장하지 않음)
    - ✅ (옵션) 동일라인 과거 가동일(소수)  (단, TODAY(질문일) 이전/당일은 금지)
    """
    capa_status: Dict[str, Dict[str, Any]] = {}

    # -------------------------------
    # (A) 데이터 기반 "미래 확장 상한" = 마지막 납기일(=qty_0차가 있는 마지막 날짜)
    #     - qty_0차가 없다면, plan_date 최대값을 상한으로 사용
    # -------------------------------
    horizon_end = None
    if (not plan_df.empty) and ("plan_date" in plan_df.columns):
        if "qty_0차" in plan_df.columns:
            tmp = plan_df.copy()
            tmp["qty_0차"] = pd.to_numeric(tmp["qty_0차"], errors="coerce").fillna(0)
            due_df = tmp[tmp["qty_0차"] > 0]
            if not due_df.empty:
                horizon_end = str(due_df["plan_date"].max())[:10]
        if not horizon_end:
            horizon_end = str(plan_df["plan_date"].max())[:10]

    # -------------------------------
    # (B) 같은날 CAPA: 모든 라인 포함
    # -------------------------------
    for line in ["조립1", "조립2", "조립3"]:
        cur = plan_df[(plan_df["plan_date"] == target_date) & (plan_df["line"] == line)]["qty_1차"].sum()
        cur = int(cur) if pd.notna(cur) else 0
        remaining = int(capa_limits[line] - cur)
        capa_status[f"{target_date}_{line}"] = {
            "date": target_date,
            "line": line,
            "current": cur,
            "remaining": remaining,
            "max": capa_limits[line],
            "usage_rate": (cur / capa_limits[line] * 100) if capa_limits[line] else 0,
        }

    # -------------------------------
    # (C) 동일라인 미래 가동일 후보
    #     - 미래로 "많이 미루기"가 목적이 아니라,
    #       가능한 후보를 넓게 보되 납기/데이터 상한(horizon_end) 안에서만,
    #       그리고 최대 max_future_workdays개까지만 사용 (horizon_end가 가까우면 자동 축소)
    # -------------------------------
    max_future_workdays = 10

    # DB is_workday 기준으로 horizon_end까지 가능한 미래 가동일을 넉넉히 모은 뒤
    # max_future_workdays개까지만 사용
    future_candidates = get_workdays_from_db(plan_df, target_date, direction="future", days_count=400)
    future_candidates = [d for d in future_candidates if str(d)[:10] != target_date]
    if horizon_end:
        future_candidates = [d for d in future_candidates if str(d)[:10] <= horizon_end]
    future_workdays = future_candidates[:max_future_workdays]

    # 보강: is_workday가 없거나 리스트가 빈 경우, 달력으로 탐색(그래도 horizon_end 바깥은 금지)
    if not future_workdays:
        base = _safe_date(target_date)
        for i in range(1, 60):  # 넉넉히 보되 아래에서 잘라냄
            d = (base + timedelta(days=i)).strftime("%Y-%m-%d")
            if horizon_end and d > horizon_end:
                break
            if is_workday_in_db(plan_df, d):
                future_workdays.append(d)
            if len(future_workdays) >= max_future_workdays:
                break

    for d in future_workdays:
        cur = plan_df[(plan_df["plan_date"] == d) & (plan_df["line"] == target_line)]["qty_1차"].sum()
        cur = int(cur) if pd.notna(cur) else 0
        remaining = int(capa_limits[target_line] - cur)
        capa_status[f"{d}_{target_line}"] = {
            "date": d,
            "line": target_line,
            "current": cur,
            "remaining": remaining,
            "max": capa_limits[target_line],
            "usage_rate": (cur / capa_limits[target_line] * 100) if capa_limits[target_line] else 0,
        }

    # -------------------------------
    # (D) 동일라인 과거 가동일 후보 (선행 생산)
    #     - 너무 많이 당기는 것을 방지: 5개 가동일만
    #     - get_workdays_from_db가 "TODAY 이후만" 보장 (plan_date > today_str)
    # -------------------------------
    past_workdays = get_workdays_from_db(plan_df, target_date, direction="past", days_count=5)

    for d in past_workdays:
        # 안전: target_date보다 과거만
        if str(d)[:10] >= target_date:
            continue

        cur = plan_df[(plan_df["plan_date"] == d) & (plan_df["line"] == target_line)]["qty_1차"].sum()
        cur = int(cur) if pd.notna(cur) else 0
        remaining = int(capa_limits[target_line] - cur)
        capa_status[f"{d}_{target_line}"] = {
            "date": d,
            "line": target_line,
            "current": cur,
            "remaining": remaining,
            "max": capa_limits[target_line],
            "usage_rate": (cur / capa_limits[target_line] * 100) if capa_limits[target_line] else 0,
        }

    return capa_status
# ========================================================================
# 4단계: 물리 제약 정리
# ========================================================================

def step4_prepare_constraint_info(items_with_slack: List[Dict[str, Any]], target_line: str) -> List[Dict[str, Any]]:
    constraint_info = []
    for item in items_with_slack:
        if not item.get("movable"):
            continue

        name = item["name"]
        is_t6 = "T6" in str(name).upper()
        is_a2xx = "A2XX" in str(name).upper()

        if is_t6:
            possible_lines = [l for l in ["조립1", "조립2", "조립3"] if l != target_line]
            constraint = "조립1, 2, 3 모두 가능"
            priority = "타라인 이동(분산) 우선"
        elif is_a2xx:
            possible_lines = [l for l in ["조립1", "조립2"] if l != target_line]
            constraint = "조립1, 2만 가능 (조립3 절대 금지)"
            priority = "조립2 이송 우선"
        else:
            possible_lines = []
            constraint = f"{target_line} 내 날짜 이동만 가능"
            priority = "동일라인 날짜 이동(연기/당김)"

        constraint_info.append(
            {
                "name": name,
                "qty_1차": int(item["qty_1차"]),
                "plt": int(item["plt"]),
                "max_movable": int(item["max_movable"]),
                "buffer_days": int(item["buffer_days"]),
                "constraint": constraint,
                "possible_lines": possible_lines,
                "priority": priority,
                "is_t6": is_t6,
                "is_a2xx": is_a2xx,
            }
        )
    return constraint_info


# ========================================================================
# 5단계: AI 전략 (reduce/increase 공통)
# ========================================================================

def build_ai_fact_report(
    constraint_info: List[Dict[str, Any]],
    capa_status: Dict[str, Dict[str, Any]],
    target_date: str,
    target_line: str,
    operation_mode: str,
    operation_qty: int,
) -> str:
    op_kr = "감축" if operation_mode == "reduce" else "증량"

    fact = []
    fact.append("### 📊 Python 수사 완료 (검증된 팩트)")
    fact.append(f"- 대상: {target_date} {target_line}")
    fact.append(f"- 목표: {op_kr} {operation_qty:,}개")
    fact.append("")
    fact.append("**이동 가능 품목 목록** (누적 납기 여유 검증 완료):")
    for i, item in enumerate(constraint_info, 1):
        fact.append(
            f"{i}. {item['name']} | 현재:{item['qty_1차']:,} | 이동최대:{item['max_movable']:,} | PLT:{item['plt']} | 여유:{item['buffer_days']}일 | 제약:{item['constraint']}"
        )

    fact.append("")
    fact.append("**목적지/출발지 CAPA 현황:**")
    for _, st in capa_status.items():
        fact.append(f"- {st['date']} {st['line']}: 잔여 {st['remaining']:,}개 (가동률 {st['usage_rate']:.1f}%)")

    return "\n".join(fact)


def _extract_json_from_text(text: str) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    cleaned = re.sub(r"```json\s*|\s*```", "", text.strip())
    start = cleaned.find("{")
    end = cleaned.rfind("}") + 1
    if start == -1 or end <= start:
        return None
    try:
        return json.loads(cleaned[start:end])
    except Exception:
        return None


def step5_ask_ai_strategy(
    fact_report: str,
    operation_mode: str,
    operation_qty: int,
    target_line: str,
    target_date: str,
    today_str: str,
    capa_target_pct: int,
    genai_key: str,
) -> Tuple[Optional[Dict[str, Any]], Optional[str], str]:
    """
    Returns: (ai_strategy or None, error or None, strategy_source)
    """
    genai.configure(api_key=genai_key)

    if operation_mode == "reduce":
        operation_desc = "감축"
        strategy_hint = """
우선순위:
1) 같은 날 타라인 이송 (remaining > 0인 곳만)
   - T6: 타라인 가능
   - A2XX: 조립3 금지
2) 같은 라인 미래 날짜 연기 (납기 여유(buffer_days) 범위 내)
3) (필요시) 같은 라인 과거 선행 생산 (고정기간 정책 고려)
"""
    else:
        operation_desc = "증량"
        strategy_hint = """
우선순위:
1) 같은 날 타라인에서 가져오기 (T6만 타라인 이동 가능)
2) 같은 라인 미래 날짜에서 당기기 (납기 위반 없는 범위)
"""

    prompt = f"""{fact_report}

위 데이터를 바탕으로 이동 조치 계획을 아래 JSON 형식으로 작성하라:

{{
  "strategy": "전략 요약 (한 문장)",
  "explanation": "전략 설명 (2-3문장)",
  "moves": [
    {{
      "item": "품목명",
      "qty": 수량,
      "plt": PLT수,
      "from": "출발지날짜_출발지라인",
      "to": "목적지날짜_목적지라인",
      "reason": "이유"
    }}
  ]
}}

중요 규칙:
- "from", "to" 형식: 반드시 "YYYY-MM-DD_라인명"
- qty는 반드시 PLT의 정수배
- 목적지 remaining 초과 금지
- A2XX는 조립3 절대 금지
- 전용 모델(비 T6/A2XX)은 타라인 이동 금지(동일라인 날짜 이동만)

현재:
- 대상 라인: {target_line}
- 작업 모드: {operation_desc}
- 목표 {operation_desc}량: {operation_qty:,}개
- 사용자 요청 CAPA 목표: {capa_target_pct}%

{strategy_hint}
"""

    try:
        model = genai.GenerativeModel("gemini-2.0-flash-exp")
        resp = model.generate_content(prompt)
        raw = (resp.text or "").strip()
        parsed = _extract_json_from_text(raw)
        if not parsed:
            return None, "AI 응답에서 JSON 파싱 실패", "AI 실패"
        return parsed, None, "AI 하이브리드 전략 (Gemini 2.0 Flash)"
    except Exception as e:
        return None, f"AI 오류: {str(e)}", "AI 실패"


# ========================================================================
# 6단계: Python 검증 (AI moves를 안전하게 필터/조정)
# ========================================================================

def step6_validate_ai_strategy(
    ai_strategy: Dict[str, Any],
    constraint_info: List[Dict[str, Any]],
    capa_status: Dict[str, Dict[str, Any]],
    plan_df: pd.DataFrame,
    target_line: str,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    if not ai_strategy or "moves" not in ai_strategy:
        return [], ["❌ AI 전략 형식 오류: 'moves' 키가 없습니다."]

    name_to_item = {x["name"]: x for x in constraint_info}
    validated: List[Dict[str, Any]] = []
    violations: List[str] = []

    t6_sameday_shift_used = False  # T6 같은날 타라인 이송은 1회만 허용

    today_str = TODAY.strftime("%Y-%m-%d") if TODAY else None

    def _get_item_last_due(item_name: str) -> Optional[str]:
        if plan_df.empty or ("qty_0차" not in plan_df.columns):
            return None
        df = plan_df[plan_df["product_name"] == item_name].copy()
        if df.empty:
            return None
        df["qty_0차"] = pd.to_numeric(df["qty_0차"], errors="coerce").fillna(0)
        due = df[df["qty_0차"] > 0]["plan_date"]
        if due.empty:
            return None
        return str(due.max())[:10]

    def _check_due_cumsum_after_move(item_name: str, from_date: str, to_date: str, qty_move: int) -> Tuple[bool, Optional[str]]:
        """이동을 적용했을 때 품목별 누적 납기(cumsum1>=cumsum0)가 모든 날짜에서 유지되는지 검증"""
        needed = {"product_name", "plan_date", "qty_0차", "qty_1차"}
        if plan_df.empty or not needed.issubset(set(plan_df.columns)):
            return True, None

        df = plan_df[plan_df["product_name"] == item_name].copy()
        if df.empty:
            return True, None

        daily = (
            df.groupby("plan_date")[["qty_0차", "qty_1차"]]
            .sum()
            .reset_index()
            .copy()
        )
        daily["qty_0차"] = pd.to_numeric(daily["qty_0차"], errors="coerce").fillna(0).astype(int)
        daily["qty_1차"] = pd.to_numeric(daily["qty_1차"], errors="coerce").fillna(0).astype(int)

        # from/to 날짜가 daily에 없을 수 있으니 보강
        for d in [from_date, to_date]:
            if d not in set(daily["plan_date"].astype(str).str[:10].tolist()):
                daily = pd.concat([daily, pd.DataFrame([{"plan_date": d, "qty_0차": 0, "qty_1차": 0}])], ignore_index=True)

        # plan_date 정규화
        daily["plan_date"] = daily["plan_date"].astype(str).str[:10]
        daily = daily.sort_values("plan_date").reset_index(drop=True)

        # 이동 적용 (생산량만 이동, 수요(qty_0차)는 그대로)
        # from에서 빼기
        idx_from = daily.index[daily["plan_date"] == from_date]
        idx_to = daily.index[daily["plan_date"] == to_date]
        if len(idx_from) == 0 or len(idx_to) == 0:
            return True, None

        f = int(idx_from[0]); t = int(idx_to[0])

        daily.loc[f, "qty_1차"] -= int(qty_move)
        daily.loc[t, "qty_1차"] += int(qty_move)

        # 음수 생산량은 불가
        if daily.loc[f, "qty_1차"] < 0:
            return False, from_date

        daily["c0"] = daily["qty_0차"].cumsum()
        daily["c1"] = daily["qty_1차"].cumsum()
        bad = daily[daily["c1"] < daily["c0"]]
        if bad.empty:
            return True, None
        return False, str(bad.iloc[0]["plan_date"])

    for idx, move in enumerate(ai_strategy.get("moves", []), 1):
        item_name = str(move.get("item", "") or "")
        qty = int(move.get("qty", 0) or 0)
        to_loc = str(move.get("to", "") or "")
        from_loc = str(move.get("from", "") or "")
        reason = str(move.get("reason", "미지정") or "미지정")

        if not item_name or item_name not in name_to_item:
            violations.append(f"❌ [{idx}] {item_name}: 이동 가능 품목 목록에 없음")
            continue

        item = name_to_item[item_name]

        if qty <= 0:
            violations.append(f"❌ [{idx}] {item_name}: qty가 0 이하")
            continue

        # 누적 납기 여유 기준 검증: qty는 max_movable 이하
        if qty > int(item["max_movable"]):
            violations.append(f"❌ [{idx}] {item_name}: 누적 여유 초과 (요청 {qty:,} > 최대 {item['max_movable']:,})")
            continue

        # PLT 단위
        if qty % int(item["plt"]) != 0:
            violations.append(f"❌ [{idx}] {item_name}: PLT 단위 아님 (qty {qty:,}, plt {item['plt']})")
            continue

        # 목적지 파싱
        if "_" not in to_loc:
            violations.append(f"❌ [{idx}] {item_name}: 목적지 형식 오류 (to='{to_loc}')")
            continue
        to_date = to_loc.split("_", 1)[0].strip()
        to_line = to_loc.split("_", 1)[1].strip()

        # 출발지 파싱(가능하면 수량 존재 검증에 사용)
        from_date = None
        from_line = None
        if "_" in from_loc:
            from_date = from_loc.split("_", 1)[0].strip()
            from_line = from_loc.split("_", 1)[1].strip()

        # -----------------------
        # (0) no-op 이동 방지 (같은 날짜/같은 라인으로의 이동은 의미 없음)
        # - 이런 move가 들어오면 Δ 표에는 변화가 없는데, 감축량/달성률 집계가 왜곡될 수 있음
        # -----------------------
        if from_date and from_line and (from_date == to_date) and (from_line == to_line):
            violations.append(f"❌ [{idx}] {item_name}: 같은 날짜/라인({to_date}_{to_line})로 이동(no-op) 불가")
            continue

        # -----------------------
        # (0.5) 사람 같은 분산: T6 같은날 타라인 이송은 1회만, 최대 5PLT까지만 우선 허용
        # - 과대 이동(예: 6PLT, 1,050개)을 막고, 남는 감축은 다른 품목/날짜 이동을 우선 시도
        # -----------------------
        if item.get("is_t6") and from_date and from_line and (from_date == to_date) and (from_line != to_line):
            if t6_sameday_shift_used:
                violations.append(f"❌ [{idx}] {item_name}: T6 같은날 타라인 이송은 1회만 허용(사람 같은 분산)")
                continue
            max_qty = int(MAX_T6_SAMEDAY_SHIFT_PLTS) * int(item["plt"])
            if qty > max_qty:
                # qty/plt 정합성은 유지되도록 5PLT로 캡
                original_qty = qty
                qty = max_qty
                move["qty"] = qty
                violations.append(f"ℹ️ [{idx}] {item_name}: T6 과대 이동 방지(원본 {original_qty:,} → {qty:,}, {MAX_T6_SAMEDAY_SHIFT_PLTS}PLT 캡)")

        # -----------------------
        # (1) 물리 제약
        # -----------------------
        if item["is_a2xx"] and to_line == "조립3":
            violations.append(f"❌ [{idx}] {item_name}: A2XX는 조립3 이동 불가")
            continue

        if (not item["is_t6"]) and (not item["is_a2xx"]) and to_line != target_line:
            violations.append(f"❌ [{idx}] {item_name}: 전용 모델은 타라인 이동 불가 (요청 {to_line})")
            continue

        # -----------------------
        # (2) 가동일 (휴무일이면 즉시 컷)
        # -----------------------
        if not is_workday_in_db(plan_df, to_date):
            violations.append(f"❌ [{idx}] {item_name}: {to_date}는 휴무일")
            continue

        # -----------------------
        # (3) TODAY(질문일) 이전/당일 선행생산 금지
        #     - '과거로 당기기'는 오늘 이후만 허용 (TODAY+1 ~ target_date-1)
        # -----------------------
        if today_str and to_date <= today_str:
            violations.append(f"❌ [{idx}] {item_name}: 목적지 날짜({to_date})가 오늘({today_str}) 이전/당일이라 선행생산 금지")
            continue

        # -----------------------
        # (4) 납기(=qty_0차) 기반 상한: last_due 이후로는 이동 금지
        # -----------------------
        last_due = _get_item_last_due(item_name)
        if last_due and to_date > last_due:
            violations.append(f"❌ [{idx}] {item_name}: 납기 이후 날짜로 이동 불가 (to {to_date} > last_due {last_due})")
            continue

        # -----------------------
        # (5) 출발지 수량 존재 검증 (가능한 경우)
        # -----------------------
        if from_date and from_line:
            src_qty = plan_df[
                (plan_df["plan_date"] == from_date)
                & (plan_df["line"] == from_line)
                & (plan_df["product_name"] == item_name)
            ]["qty_1차"].sum()
            src_qty = int(src_qty) if pd.notna(src_qty) else 0
            if src_qty < qty:
                violations.append(f"❌ [{idx}] {item_name}: 출발지 수량 부족 (from {from_loc} 보유 {src_qty:,} < 요청 {qty:,})")
                continue

        # -----------------------
        # (6) 목적지 CAPA 확인/조정
        # -----------------------
        capa_key = f"{to_date}_{to_line}"
        if capa_key not in capa_status:
            violations.append(f"⚠️ [{idx}] {item_name}: 목적지 CAPA 정보 없음 ({capa_key})")
            continue

        dest = capa_status[capa_key]
        final_qty = qty
        adjusted = False
        original_qty = None

        if final_qty > int(dest["remaining"]):
            # 남은 CAPA 내에서 PLT 정수배로 줄여서라도 반영
            if int(dest["remaining"]) >= int(item["plt"]):
                adj_plts = int(dest["remaining"]) // int(item["plt"])
                adj_qty = adj_plts * int(item["plt"])
                final_qty = adj_qty
                adjusted = True
                original_qty = qty
            else:
                violations.append(f"❌ [{idx}] {item_name}: CAPA 부족 및 조정 불가 (남은 {dest['remaining']:,})")
                continue

        # -----------------------
        # (7) 이동 적용 시 '누적 납기' 위반 여부 최종 검증
        # -----------------------
        if from_date:
            ok, bad_date = _check_due_cumsum_after_move(item_name, from_date, to_date, final_qty)
            if not ok:
                violations.append(f"❌ [{idx}] {item_name}: 납기 누적 위반(이동 후 {bad_date}까지 생산 부족) → 이동 불가")
                continue

        # ✅ 모든 검증 통과 후에만 CAPA 차감
        capa_status[capa_key]["remaining"] -= final_qty

        validated.append(
            {
                "item": item_name,
                "qty": final_qty,
                "plt": final_qty // int(item["plt"]),
                "from": from_loc,
                "to": to_loc if not adjusted else f"{to_date}_{to_line}",
                "reason": reason,
                "adjusted": adjusted,
                "original_qty": original_qty,
            }
        )


        if item.get('is_t6') and from_date and from_line and (from_date == to_date) and (from_line != to_line):
            t6_sameday_shift_used = True
        if adjusted:
            violations.append(f"✅ [{idx}] {item_name}: CAPA 부족으로 자동 조정 ({qty:,} → {final_qty:,})")

    return validated, violations
# ========================================================================
# Python 폴백 전략 (AI 실패/부족 시)
# ========================================================================

def _pick_qty_plts(qty: int, plt: int) -> int:
    if plt <= 0:
        return 0
    return (qty // plt) * plt


def python_fallback_reduce(
    plan_df: pd.DataFrame,
    constraint_info: List[Dict[str, Any]],
    capa_status: Dict[str, Dict[str, Any]],
    question_date: str,
    target_line: str,
    need_reduce: int,
    t6_sameday_already_used: bool = False,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    감축 폴백 (사람 같은 분산 우선순위):
    1) (우선 1회) T6 같은날 타라인 이송: 최대 5PLT까지만 사용 (과대 이동 방지)
       - A2XX는 같은날 타라인 이송 가능(조립3 금지)하되, 전용(기타)은 타라인 금지
    2) 남은 감축은 '비 T6' 품목의 동일라인 날짜 이동(미래 연기)부터 우선 시도
    3) 그래도 부족하면 마지막에 T6의 동일라인 날짜 이동(미래 연기)
    4) 그래도 부족하면 과거(선행생산)로 당기기 (TODAY 이전/당일 금지, 마지막 수단)
    """
    moves: List[Dict[str, Any]] = []
    notes: List[str] = []

    remain = int(need_reduce or 0)
    if remain <= 0:
        return [], []

    # buffer_days 큰 순(납기 여유가 큰 품목 우선)
    candidates = sorted(constraint_info, key=lambda x: x.get("buffer_days", 0), reverse=True)

    # -------------------------------
    # (0) 미래 확장 상한(horizon_end) 계산 (qty_0차 > 0인 마지막 납기일)
    # -------------------------------
    horizon_end = None
    if (not plan_df.empty) and ("plan_date" in plan_df.columns) and ("qty_0차" in plan_df.columns):
        try:
            tmp = plan_df.copy()
            tmp["qty_0차"] = pd.to_numeric(tmp["qty_0차"], errors="coerce").fillna(0)
            due = tmp[tmp["qty_0차"] > 0]["plan_date"]
            if not due.empty:
                horizon_end = str(due.max())[:10]
        except Exception:
            horizon_end = None

    # ======================================================
    # [1] 같은날 타라인 이송 (T6는 1회/5PLT 상한)
    # ======================================================
    t6_used_sameday = bool(t6_sameday_already_used)

    for item in candidates:
        if remain <= 0:
            break

        name = item["name"]
        plt = int(item["plt"])
        movable = int(item["max_movable"])
        if movable < plt:
            continue

        is_t6 = bool(item.get("is_t6"))
        is_a2xx = bool(item.get("is_a2xx"))

        if is_t6 and t6_used_sameday:
            continue

        if is_t6:
            possible_lines = [l for l in ["조립1", "조립2", "조립3"] if l != target_line]
        elif is_a2xx:
            possible_lines = [l for l in ["조립1", "조립2"] if l != target_line]  # 조립3 금지
        else:
            continue  # 전용은 타라인 금지

        # 목적지 후보(같은날)
        dests = []
        for dl in possible_lines:
            key = f"{question_date}_{dl}"
            if key in capa_status and int(capa_status[key]["remaining"]) > 0:
                dests.append((dl, int(capa_status[key]["remaining"])))
        dests.sort(key=lambda x: x[1], reverse=True)
        if not dests:
            continue

        for dl, rem_capa in dests:
            if remain <= 0:
                break
            if rem_capa < plt:
                continue

            take = min(remain, movable, rem_capa)

            # T6는 같은날 타라인 이송을 '최대 5PLT'까지만 우선 사용
            if is_t6:
                take = min(take, int(MAX_T6_SAMEDAY_SHIFT_PLTS) * plt)

            take = _pick_qty_plts(take, plt)
            if take <= 0:
                continue

            # 가동일 체크
            if not is_workday_in_db(plan_df, question_date):
                continue
            if not is_workday_in_db(plan_df, question_date):
                continue

            capa_status[f"{question_date}_{dl}"]["remaining"] -= take
            remain -= take
            moves.append(
                {
                    "item": name,
                    "qty": take,
                    "plt": take // plt,
                    "from": f"{question_date}_{target_line}",
                    "to": f"{question_date}_{dl}",
                    "reason": f"[폴백] 타라인 이송으로 감축 ({dl} 잔여 활용)",
                }
            )

            if is_t6:
                t6_used_sameday = True
            break  # 같은 품목은 1건만(사람 같은 플로우)

    # ======================================================
    # [2] 동일라인 미래로 연기 (비 T6 먼저)
    # ======================================================
    if remain > 0:
        max_future_days = 10

        future_candidates = get_workdays_from_db(plan_df, question_date, direction="future", days_count=400)
        future_candidates = [d for d in future_candidates if str(d)[:10] != question_date]
        if horizon_end:
            future_candidates = [d for d in future_candidates if str(d)[:10] <= horizon_end]
        future_days = future_candidates[:max_future_days]

        if not future_days:
            notes.append("⚠️ [폴백] 미래 가동일 정보를 찾지 못했습니다 (is_workday 없음/데이터 범위 부족).")
        else:
            # (2-a) 비 T6 먼저
            for item in [x for x in candidates if not x.get("is_t6")]:
                if remain <= 0:
                    break

                name = item["name"]
                plt = int(item["plt"])
                movable = int(item["max_movable"])
                if movable < plt:
                    continue

                for d in future_days:
                    if remain <= 0:
                        break
                    key = f"{d}_{target_line}"
                    if key not in capa_status:
                        continue
                    rem_capa = int(capa_status[key]["remaining"])
                    if rem_capa < plt:
                        continue

                    take = min(remain, movable, rem_capa)
                    take = _pick_qty_plts(take, plt)
                    if take <= 0:
                        continue

                    capa_status[key]["remaining"] -= take
                    remain -= take
                    moves.append(
                        {
                            "item": name,
                            "qty": take,
                            "plt": take // plt,
                            "from": f"{question_date}_{target_line}",
                            "to": f"{d}_{target_line}",
                            "reason": f"[폴백] 동일라인 미래 연기로 감축 ({d})",
                        }
                    )
                    break  # 한 품목은 1건만

            # (2-b) 그래도 부족하면 마지막에 T6 동일라인 미래 연기
            if remain > 0:
                for item in [x for x in candidates if x.get("is_t6")]:
                    if remain <= 0:
                        break

                    name = item["name"]
                    plt = int(item["plt"])
                    movable = int(item["max_movable"])
                    if movable < plt:
                        continue

                    for d in future_days:
                        if remain <= 0:
                            break
                        key = f"{d}_{target_line}"
                        if key not in capa_status:
                            continue
                        rem_capa = int(capa_status[key]["remaining"])
                        if rem_capa < plt:
                            continue

                        take = min(remain, movable, rem_capa)
                        take = _pick_qty_plts(take, plt)
                        if take <= 0:
                            continue

                        capa_status[key]["remaining"] -= take
                        remain -= take
                        moves.append(
                            {
                                "item": name,
                                "qty": take,
                                "plt": take // plt,
                                "from": f"{question_date}_{target_line}",
                                "to": f"{d}_{target_line}",
                                "reason": f"[폴백] (보조) T6 동일라인 미래 연기로 감축 ({d})",
                            }
                        )
                        break

    # ======================================================
    # [3] 과거(선행생산)로 당기기 (마지막 수단)
    # ======================================================
    if remain > 0:
        past_days = get_workdays_from_db(plan_df, question_date, direction="past", days_count=5)

        for item in candidates:
            if remain <= 0:
                break

            name = item["name"]
            plt = int(item["plt"])
            movable = int(item["max_movable"])
            if movable < plt:
                continue

            for d in past_days:
                if remain <= 0:
                    break
                key = f"{d}_{target_line}"
                if key not in capa_status:
                    continue
                rem_capa = int(capa_status[key]["remaining"])
                if rem_capa < plt:
                    continue

                take = min(remain, movable, rem_capa)
                take = _pick_qty_plts(take, plt)
                if take <= 0:
                    continue

                capa_status[key]["remaining"] -= take
                remain -= take
                moves.append(
                    {
                        "item": name,
                        "qty": take,
                        "plt": take // plt,
                        "from": f"{question_date}_{target_line}",
                        "to": f"{d}_{target_line}",
                        "reason": f"[폴백] 과거 선행생산으로 당기기 ({d})",
                    }
                )
                break

    return moves, notes

def python_fallback_increase(
    plan_df: pd.DataFrame,
    constraint_info: List[Dict[str, Any]],
    capa_status: Dict[str, Dict[str, Any]],
    question_date: str,
    target_line: str,
    need_increase: int,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    증량 폴백:
    1) 같은날 타라인에서 가져오기 (T6만)
    2) 같은라인 미래 날짜에서 당기기
    """
    moves = []
    notes = []

    remain = need_increase
    if remain <= 0:
        return [], []

    # [1] 같은날 타라인 -> target_line (T6만)
    date_df = plan_df[plan_df["plan_date"] == question_date].copy()
    if not date_df.empty:
        for src_line in ["조립1", "조립2", "조립3"]:
            if src_line == target_line:
                continue
            src = date_df[(date_df["line"] == src_line) & (date_df["qty_1차"] > 0)]
            if src.empty:
                continue

            for _, row in src.iterrows():
                if remain <= 0:
                    break
                name = str(row.get("product_name", ""))
                if "T6" not in name.upper():
                    continue
                plt = int(row.get("plt", 1) or 1)
                src_qty = int(row.get("qty_1차", 0) or 0)

                take = min(remain, src_qty)
                take = _pick_qty_plts(take, plt)
                if take <= 0:
                    continue

                remain -= take
                moves.append(
                    {
                        "item": name,
                        "qty": take,
                        "plt": take // plt,
                        "from": f"{question_date}_{src_line}",
                        "to": f"{question_date}_{target_line}",
                        "reason": f"[폴백] 같은날 타라인({src_line})에서 T6 가져오기",
                    }
                )

    # [2] 미래 동일라인에서 당기기
    if remain > 0:
        base = _safe_date(question_date)
        for i in range(1, 11):
            if remain <= 0:
                break
            d = (base + timedelta(days=i)).strftime("%Y-%m-%d")
            if not is_workday_in_db(plan_df, d):
                continue

            future = plan_df[
                (plan_df["plan_date"] == d)
                & (plan_df["line"] == target_line)
                & (plan_df["qty_1차"] > 0)
            ]
            if future.empty:
                continue

            movable_map = {x["name"]: x for x in constraint_info}
            for _, row in future.iterrows():
                if remain <= 0:
                    break

                name = str(row.get("product_name", ""))
                if name not in movable_map:
                    continue
                item = movable_map[name]
                plt = int(item["plt"])
                max_movable = int(item["max_movable"])

                src_qty = int(row.get("qty_1차", 0) or 0)
                take = min(remain, src_qty, max_movable)
                take = _pick_qty_plts(take, plt)
                if take <= 0:
                    continue

                remain -= take
                moves.append(
                    {
                        "item": name,
                        "qty": take,
                        "plt": take // plt,
                        "from": f"{d}_{target_line}",
                        "to": f"{question_date}_{target_line}",
                        "reason": f"[폴백] 미래({d}) 동일라인 물량 당기기",
                    }
                )

    if remain > 0:
        notes.append(f"⚠️ [폴백] 증량 미달: 추가로 {remain:,}개 더 필요")

    return moves, notes


# ========================================================================
# 보고서 생성 (reduce/increase 공통)
# ========================================================================

def generate_full_report(
    stock_result: Dict[str, Any],
    items_with_slack: List[Dict[str, Any]],
    capa_status: Dict[str, Dict[str, Any]],
    constraint_info: List[Dict[str, Any]],
    ai_strategy: Dict[str, Any],
    final_moves: List[Dict[str, Any]],
    violations: List[str],
    target_qty: int,
    capa_target: float,
    operation_mode: str,
    operation_qty: int,
    strategy_source: str,
    ai_failed: bool,
    ai_error: str,
    today_str: str,
    question_date: str,
    target_line: str,
    extra_notes: List[str],
) -> str:
    def _merge_moves(moves: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        같은 item/from/to 이동은 합산해서 1줄로 보여주기 (표시용)
        - qty 합산
        - plt(팔레트 수) 합산
        - reason이 다르면 '; '로 합침(중복 방지)
        - adjusted/original_qty는 하나라도 있으면 표시(원본_qty는 합산)
        """
        if not moves:
            return []

        merged: Dict[tuple, Dict[str, Any]] = {}

        for m in moves:
            key = (m.get("item"), m.get("from"), m.get("to"))
            qty = int(m.get("qty", 0) or 0)
            plt = int(m.get("plt", 0) or 0)
            reason = str(m.get("reason", "") or "")
            adjusted = bool(m.get("adjusted", False))
            original_qty = m.get("original_qty", None)

            if key not in merged:
                merged[key] = {
                    "item": m.get("item"),
                    "from": m.get("from"),
                    "to": m.get("to"),
                    "qty": qty,
                    "plt": plt,
                    "reason": reason,
                    "adjusted": adjusted,
                    "original_qty": int(original_qty) if original_qty is not None else None,
                }
            else:
                merged[key]["qty"] += qty
                merged[key]["plt"] += plt

                if reason and reason not in (merged[key]["reason"] or ""):
                    if merged[key]["reason"]:
                        merged[key]["reason"] += "; " + reason
                    else:
                        merged[key]["reason"] = reason

                merged[key]["adjusted"] = merged[key]["adjusted"] or adjusted
                if original_qty is not None:
                    if merged[key]["original_qty"] is None:
                        merged[key]["original_qty"] = int(original_qty)
                    else:
                        merged[key]["original_qty"] += int(original_qty)

        return sorted(merged.values(), key=lambda x: int(x.get("qty", 0)), reverse=True)

    op_kr = "감축" if operation_mode == "reduce" else "증량"
    moved_total = sum(int(m["qty"]) for m in final_moves) if final_moves else 0
    achievement = (moved_total / operation_qty * 100) if operation_qty > 0 else 0

    current_qty = int(stock_result["total"])
    final_qty = current_qty - moved_total if operation_mode == "reduce" else current_qty + moved_total

    report = []
    report.append(f"# 📊 {question_date} {target_line} 하이브리드 수사 보고서")
    report.append(f"🔧 엔진 버전: {ENGINE_VERSION}")
    report.append("")
    report.append("## 🔍 수사 방식")
    report.append(f"- 전략 수립: {strategy_source}")
    report.append(f"- 분석 기준일: {today_str}")
    report.append("")

    report.append("## 📋 [1단계] 현황 파악")
    report.append(f"- 현재 생산량: **{current_qty:,}개**")
    report.append(f"- 목표 생산량: **{target_qty:,}개** ({int(capa_target*100)}% CAPA)")
    report.append(f"- 필요 {op_kr}량: **{operation_qty:,}개**")
    report.append("")

    report.append(f"### 품목 목록 ({len(stock_result.get('items', []))}개)")
    for i, it in enumerate(stock_result.get("items", [])[:15], 1):
        report.append(f"{i}. {it['name']}: {it['qty_1차']:,}개 ({it['qty_1차']//it['plt']}PLT, 단위 {it['plt']})")
    if len(stock_result.get("items", [])) > 15:
        report.append(f"... 외 {len(stock_result['items']) - 15}개")

    report.append("")
    report.append("## 🔍 [2단계] 누적 납기 여유 분석")
    movable = [x for x in items_with_slack if x.get("movable")]
    report.append(f"- 이동 가능 품목: {len(movable)}개")
    report.append("")

    report.append("## 🎯 [3단계] CAPA 현황")
    for _, st in list(capa_status.items())[:12]:
        report.append(f"- {st['date']} {st['line']}: 잔여 {st['remaining']:,}개 (가동률 {st['usage_rate']:.1f}%)")
    report.append("")

    report.append("## 🔒 [4단계] 물리 제약 요약")
    report.append("- T6: 조립1/2/3 가능")
    report.append("- A2XX: 조립3 금지")
    report.append("- 전용(기타): 동일라인 날짜 이동만")
    report.append("")

    report.append(f"## 🤖 [5단계] AI 전략 ({'실패→폴백' if ai_failed else '성공'})")
    if ai_failed:
        report.append(f"- 오류: {ai_error}")
    report.append(f"- 전략 요약: {ai_strategy.get('strategy', 'N/A')}")
    report.append(f"- 설명: {ai_strategy.get('explanation', 'N/A')}")
    report.append("")

    report.append("## ✅ [6단계] Python 검증 결과")
    if violations:
        report.append(f"⚠️ 검증 메시지 {len(violations)}건")
        for v in violations[:20]:
            report.append(f"- {v}")
        if len(violations) > 20:
            report.append(f"... 외 {len(violations)-20}건")
    else:
        report.append("✅ 검증 항목 통과")
    report.append("")

    # ✅ [FIX] 최종 조치 계획: 동일 move 합산 표시
    merged_moves = _merge_moves(final_moves)

    report.append(f"## 🧾 최종 조치 계획 ({len(merged_moves)}개)")
    if merged_moves:
        for i, m in enumerate(merged_moves, 1):
            adj = ""
            if m.get("adjusted"):
                oq = m.get("original_qty", 0) or 0
                adj = f" ⚠️(조정: {oq:,}→{m['qty']:,})"
            report.append(
                f"{i}) {m['item']} | {m['qty']:+,}개({m.get('plt','?')}PLT){adj} | "
                f"{m.get('from','-')} → {m.get('to','-')} | {m.get('reason','-')}"
            )
    else:
        report.append("❌ 승인된 조치 없음")
    report.append("")

    report.append("## 🎯 최종 결과")
    report.append(f"- 실제 {op_kr}량: **{moved_total:,}개**")
    report.append(f"- 최종 생산량: **{final_qty:,}개**")
    report.append(f"- 목표 달성률: **{achievement:.1f}%**")
    if extra_notes:
        report.append("")
        report.append("## 📝 추가 메모")
        for n in extra_notes:
            report.append(f"- {n}")

    return "\n".join(report)


# ========================================================================
# 메인 엔진 (app (3).py 호환)
# ========================================================================

def ask_professional_scheduler(
    question: str,
    plan_df: pd.DataFrame,
    hist_df: pd.DataFrame,
    product_map: Dict[str, Any],
    plt_map: Dict[str, Any],
    question_date: str,
    mode: str = "hybrid",
    today=None,
    capa_limits: Optional[Dict[str, int]] = None,
    genai_key: str = "",
) -> Tuple[str, bool, List[Any], str, List[Dict[str, Any]]]:
    """
    Returns: (report, success, charts, status, validated_moves)_message)
    """
    if today is None:
        today = datetime(2026, 1, 5).date()
    if capa_limits is None:
        capa_limits = {"조립1": 3300, "조립2": 3700, "조립3": 3600}

    initialize_globals(today, capa_limits)
    today_str = today.strftime("%Y-%m-%d")

    # 0) 대상 라인 탐색
    target_line = _infer_target_line(question, plan_df, question_date)
    if not target_line:
        return (
            "❌ 질문에서 대상 라인을 찾을 수 없습니다. (예: '조립1/조립2/조립3' 또는 품목 키워드 포함)",
            False,
            [],
            "[ERROR] 라인 미지정",
            [],
        )

    # 1) stock
    stock_res, err = step1_list_current_stock(plan_df, question_date, target_line)
    if err:
        return f"❌ [1단계 실패] {err}", False, [], "[ERROR] 품목 조회 실패", []

    # 2) slack
    items_with_slack = step2_calculate_cumulative_slack(plan_df, stock_res)
    if not items_with_slack:
        return "❌ [2단계 실패] 이동 가능한 품목이 없습니다.", False, [], "[ERROR] 품목 분석 실패", []

    # 3) capa
    capa_status = step3_analyze_destination_capacity(plan_df, question_date, target_line, capa_limits)

    # 4) constraint
    constraint_info = step4_prepare_constraint_info(items_with_slack, target_line)
    if not constraint_info:
        return "❌ [4단계 실패] 이동 가능한 품목(1PLT 이상)이 없습니다.", False, [], "[ERROR] 제약정보 없음", []

    # 5) 목표치 파싱: % or 샘플/추가 N
    capa_match = re.search(r"(\d+)\s*%", question)
    sample_match = re.search(r"샘플\s*(\d+)", question)
    add_match = re.search(r"추가\s*(\d+)", question) or re.search(r"(\d+)\s*추가", question)

    current_total = int(stock_res["total"])
    if sample_match or add_match:
        add_qty = int((sample_match or add_match).group(1))
        target_qty = current_total + add_qty
        diff = target_qty - current_total  # +면 증량
        capa_target = target_qty / int(capa_limits[target_line])
    elif capa_match:
        capa_target = int(capa_match.group(1)) / 100
        target_qty = int(int(capa_limits[target_line]) * capa_target)
        diff = target_qty - current_total
    else:
        # 기본 75% (기존 정책 유지)
        capa_target = 0.75
        target_qty = int(int(capa_limits[target_line]) * capa_target)
        diff = target_qty - current_total

    if diff == 0:
        return "✅ 이미 목표 생산량과 동일합니다. 조치 불필요.", True, [], "[OK] 조치 불필요", []

    operation_mode = "increase" if diff > 0 else "reduce"
    operation_qty = abs(diff)

    # 5) AI 전략
    ai_failed = False
    ai_error_msg = ""
    extra_notes: List[str] = []
    report_prefix: str = ""

    fact_report = build_ai_fact_report(
        constraint_info=constraint_info,
        capa_status=capa_status,
        target_date=question_date,
        target_line=target_line,
        operation_mode=operation_mode,
        operation_qty=operation_qty,
    )

    ai_strategy, ai_err, strategy_source = step5_ask_ai_strategy(
        fact_report=fact_report,
        operation_mode=operation_mode,
        operation_qty=operation_qty,
        target_line=target_line,
        target_date=question_date,
        today_str=today_str,
        capa_target_pct=int(capa_target * 100),
        genai_key=genai_key,
    )

    if ai_strategy is None:
        ai_failed = True
        ai_error_msg = ai_err or "AI 전략 수립 실패"
        ai_strategy = {"strategy": "AI 실패 → Python 폴백", "explanation": "AI 오류로 기본 로직 적용", "moves": []}
        strategy_source = "Python 폴백 (AI 오류)"

    # 6) 검증
    final_moves, violations = step6_validate_ai_strategy(
        ai_strategy=ai_strategy,
        constraint_info=constraint_info,
        capa_status=capa_status,
        plan_df=plan_df,
        target_line=target_line,
    )

    # 6.5) AI가 부족하면 Python 폴백으로 채우기
    # - 폴백은 capa_status를 직접 깎지 않고(deepcopy로 시뮬레이션), 검증 통과분만 원본 capa_status에 반영
    # - 검증 후 remaining을 다시 계산하여 최대 2회까지 재시도
    def _sum_qty(moves: List[Dict[str, Any]]) -> int:
        return sum(int(m.get("qty", 0) or 0) for m in (moves or []))

    op_kr = "증량" if operation_mode == "increase" else "감축"

    remaining = max(0, operation_qty - _sum_qty(final_moves))
    fb_notes_all: List[str] = []

    fb_attempts = 0
    while remaining > 0 and fb_attempts < 2:
        fb_attempts += 1

        sim_capa = deepcopy(capa_status)

        if operation_mode == "reduce":
            t6_sameday_used_now = any(
                (str(x.get('item')) == 'T6 (P703) 수원(U725)')
                and (str(x.get('from','')) == f"{question_date}_{target_line}")
                and str(x.get('to','')).startswith(f"{question_date}_")
                and (str(x.get('to','')).split('_',1)[1] != target_line)
                for x in final_moves
            )
            fb_moves, fb_notes = python_fallback_reduce(
                plan_df=plan_df,
                constraint_info=constraint_info,
                capa_status=sim_capa,
                question_date=question_date,
                target_line=target_line,
                need_reduce=remaining,
                t6_sameday_already_used=t6_sameday_used_now,
            )
        else:
            fb_moves, fb_notes = python_fallback_increase(
                plan_df=plan_df,
                constraint_info=constraint_info,
                capa_status=sim_capa,
                question_date=question_date,
                target_line=target_line,
                need_increase=remaining,
            )

        # 폴백 내부의 "미달" 숫자는 검증 탈락/재시도 때문에 어긋날 수 있으므로,
        # 여기서는 "미달" 문구는 버리고 최종 remaining 기준으로 마지막에 1번만 출력한다.
        fb_notes_all.extend([n for n in (fb_notes or []) if "미달" not in n])

        if fb_moves:
            fb_strategy = {"strategy": "Python 폴백 채움", "explanation": "AI 부족분을 기본 로직으로 보완", "moves": fb_moves}
            fb_valid, fb_viol = step6_validate_ai_strategy(
                ai_strategy=fb_strategy,
                constraint_info=constraint_info,
                capa_status=capa_status,
                plan_df=plan_df,
                target_line=target_line,
            )
            final_moves.extend(fb_valid)
            violations.extend([f"[폴백검증] {x}" for x in fb_viol])
        else:
            break

        remaining = max(0, operation_qty - _sum_qty(final_moves))

    extra_notes.extend(fb_notes_all)
    if remaining > 0:
        extra_notes.append(f"⚠️ [폴백] {op_kr} 미달: 추가로 {remaining:,}개 더 {op_kr} 필요")

    # 6.6) (데모용) 달성률이 너무 낮고, 실패 원인이 CAPA 부족일 때 '잔업/특근'으로 CAPA를 상향한 개선안을 한 번 더 시뮬레이션
    baseline_done = _sum_qty(final_moves)
    baseline_achievement = (baseline_done / operation_qty * 100) if operation_qty else 0
    baseline_shortfall = max(0, operation_qty - baseline_done)

    auto_threshold = 85.0  # 데모용: 달성률이 낮으면(기본 85% 미만) 운영 대안(잔업/특근) 시뮬레이션
    if operation_mode == "reduce" and baseline_shortfall > 0 and baseline_achievement < auto_threshold:
        capa_related_fail = any(("CAPA 부족" in v or "조정 불가" in v) for v in violations)
        if capa_related_fail:
            plts = [int(it.get("plt", 0) or 0) for it in stock_res.get("items", []) if int(it.get("plt", 0) or 0) > 0]
            plt_base = min(plts) if plts else 1

            capa_events = _suggest_capa_events_auto(
                plan_df=plan_df,
                question_date=question_date,
                target_line=target_line,
                shortfall_qty=baseline_shortfall,
                plt_base=plt_base,
                max_days=2,
            )

            if capa_events:
                capa_status2 = step3_analyze_destination_capacity(plan_df, question_date, target_line, capa_limits)
                _apply_capa_events_to_status(capa_status2, capa_events, capa_limits)

                final2, viol2 = step6_validate_ai_strategy(
                    ai_strategy=ai_strategy,
                    constraint_info=constraint_info,
                    capa_status=capa_status2,
                    plan_df=plan_df,
                    target_line=target_line,
                )

                remaining2 = max(0, operation_qty - _sum_qty(final2))
                fb_notes2: List[str] = []
                fb_attempts2 = 0
                while remaining2 > 0 and fb_attempts2 < 2:
                    fb_attempts2 += 1
                    sim2 = deepcopy(capa_status2)

                    t6_sameday_used_now2 = any(
                        (str(x.get('item')) == 'T6 (P703) 수원(U725)')
                        and (str(x.get('from','')) == f"{question_date}_{target_line}")
                        and str(x.get('to','')).startswith(f"{question_date}_")
                        and (str(x.get('to','')).split('_',1)[1] != target_line)
                        for x in final2
                    )
                    fb_moves2, fb_notes_tmp = python_fallback_reduce(
                        plan_df=plan_df,
                        constraint_info=constraint_info,
                        capa_status=sim2,
                        question_date=question_date,
                        target_line=target_line,
                        need_reduce=remaining2,
                        t6_sameday_already_used=t6_sameday_used_now2,
                    )

                    fb_notes2.extend([n for n in (fb_notes_tmp or []) if "미달" not in n])

                    if fb_moves2:
                        fb_strategy2 = {"strategy": "Python 폴백 채움", "explanation": "AI 부족분을 기본 로직으로 보완", "moves": fb_moves2}
                        fb_valid2, fb_viol2 = step6_validate_ai_strategy(
                            ai_strategy=fb_strategy2,
                            constraint_info=constraint_info,
                            capa_status=capa_status2,
                            plan_df=plan_df,
                            target_line=target_line,
                        )
                        final2.extend(fb_valid2)
                        viol2.extend([f"[폴백검증] {x}" for x in fb_viol2])
                    else:
                        break

                    remaining2 = max(0, operation_qty - _sum_qty(final2))

                done2 = _sum_qty(final2)
                ach2 = (done2 / operation_qty * 100) if operation_qty else 0

                if ach2 > baseline_achievement + 0.1:
                    report_prefix = _format_capa_events_md(capa_events)
                    report_prefix += f"### 이벤트 적용 전 결과\n- 달성률: **{baseline_achievement:.1f}%** (미달 **{baseline_shortfall:,}개**)\n\n"
                    report_prefix += f"### 이벤트 적용 후 결과(재계산)\n- 달성률: **{ach2:.1f}%**\n\n"

                    final_moves = final2
                    violations = viol2
                    capa_status = capa_status2
                    extra_notes = fb_notes2[:]
                    if remaining2 > 0:
                        extra_notes.append(f"⚠️ [폴백] 감축 미달: 추가로 {remaining2:,}개 더 감축 필요")
    # 최종 달성률 기반 success/status
    moved_total = sum(int(m["qty"]) for m in final_moves) if final_moves else 0
    achievement = (moved_total / operation_qty * 100) if operation_qty else 0

    if achievement >= 90:
        status = "[OK] 하이브리드 수사 완료 (목표 90% 이상)"
        success = True
    else:
        status = f"[WARN] 조치 완료(미달) - 달성률 {achievement:.1f}%"
        success = False

    # 보고서
    report = (report_prefix or "") + generate_full_report(
        stock_result=stock_res,
        items_with_slack=items_with_slack,
        capa_status=capa_status,
        constraint_info=constraint_info,
        ai_strategy=ai_strategy,
        final_moves=final_moves,
        violations=violations,
        target_qty=target_qty,
        capa_target=capa_target,
        operation_mode=operation_mode,
        operation_qty=operation_qty,
        strategy_source=strategy_source,
        ai_failed=ai_failed,
        ai_error=ai_error_msg,
        today_str=today_str,
        question_date=question_date,
        target_line=target_line,
        extra_notes=extra_notes,
    )

    return report, success, [], status, final_moves