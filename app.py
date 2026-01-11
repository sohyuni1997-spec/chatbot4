import streamlit as st
import pandas as pd
from supabase import create_client, Client
import google.generativeai as genai
from datetime import datetime, timedelta
import plotly.graph_objects as go
import re
import base64
import os



# 분리된 모듈에서 함수 임포트 (legacy/hybrid 수정 없음)
from legacy import fetch_db_data_legacy, query_gemini_ai_legacy
from hybrid import ask_professional_scheduler



# ==================== 환경 설정 ====================
st.set_page_config(page_title="orcHatStra", page_icon="🎯", layout="wide")
st.markdown(
    """
    <style>
      /* Streamlit 기본 UI 숨김 */
      #MainMenu {visibility: hidden;}
      header {visibility: hidden;}
      footer {visibility: hidden;}



      /* 버전에 따라 하단 배지/툴바가 따로 있을 수 있어서 같이 숨김(안전빵) */
      [data-testid="stToolbar"] {display:none !important;}
      [data-testid="stDecoration"] {display:none !important;}
      [data-testid="stStatusWidget"] {display:none !important;}
    </style>
    """,
    unsafe_allow_html=True
)



# ==================== 이미지 파일 Base64 로더 ====================
def get_base64_of_bin_file(bin_file):
    possible_paths = [
        bin_file,
        os.path.join(os.path.dirname(__file__), bin_file) if "__file__" in globals() else bin_file,
        os.path.join(os.getcwd(), bin_file),
    ]
    for path in possible_paths:
        try:
            if os.path.exists(path):
                with open(path, "rb") as f:
                    return base64.b64encode(f.read()).decode()
        except Exception:
            continue
    return None




logo_base64 = get_base64_of_bin_file("HSE.svg")
ai_avatar_base64 = get_base64_of_bin_file("ai 아바타.png")
user_avatar_base64 = get_base64_of_bin_file("이력서 사진.v카툰.png")




# ==================== CSS ====================
st.markdown(
    f"""
<style>
    :root {{
        --bg-primary: #F5F5F7;
        --bg-secondary: #FFFFFF;
        --text-primary: #000000;
        --text-secondary: #1C1C1E;
        --border-color: #E5E5EA;
        --shadow-light: rgba(0, 0, 0, 0.1);
        --shadow-medium: rgba(0, 0, 0, 0.15);
        --user-gradient-start: #007AFF;
        --user-gradient-end: #0051D5;
        --ai-gradient-start: #34C759;
        --ai-gradient-end: #30D158;
        --input-bg: #FFFFFF;
        --header-bg: #FFFFFF;
        --header-text: #000000;
    }}



    @media (prefers-color-scheme: dark) {{
        :root {{
            --bg-primary: #000000;
            --bg-secondary: #1C1C1E;
            --text-primary: #FFFFFF;
            --text-secondary: #F5F5F7;
            --border-color: #38383A;
            --shadow-light: rgba(255, 255, 255, 0.1);
            --shadow-medium: rgba(255, 255, 255, 0.15);
            --user-gradient-start: #0A84FF;
            --user-gradient-end: #0066CC;
            --ai-gradient-start: #30D158;
            --ai-gradient-end: #28A745;
            --input-bg: #1C1C1E;
            --header-bg: #1C1C1E;
            --header-text: #FFFFFF;
        }}
    }}



    .stApp {{
        background-color: var(--bg-primary);
    }}



    .main {{
        background-color: var(--bg-primary);
        padding-top: 100px !important;
    }}



    [data-testid="stHeader"] {{
        display: none;
    }}



    .fixed-header {{
        position: fixed;
        top: 0;
        left: 0;
        right: 0;
        height: 60px;
        background-color: var(--header-bg);
        border-bottom: 1px solid var(--border-color);
        z-index: 9999;
        display: flex;
        align-items: center;
        justify-content: center;
        padding: 0 2.5rem;
        box-shadow: 0 2px 10px var(--shadow-light);
        backdrop-filter: blur(20px);
        -webkit-backdrop-filter: blur(20px);
    }}



    .header-content {{
        width: 100%;
        max-width: 1400px;
        display: flex;
        align-items: center;
        gap: 15px;
    }}



    .header-logo {{
        height: 40px;
        width: auto;
        flex-shrink: 0;
    }}



    .header-title-text {{
        font-size: 28px;
        font-weight: 600;
        color: var(--header-text);
        font-family: -apple-system, BlinkMacSystemFont, "SF Pro Display", "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
        letter-spacing: -0.5px;
        line-height: 1;
        margin: 0;
        padding: 0;
        flex-shrink: 0;
    }}



    [data-testid="stChatMessage"] {{
        display: none !important;
    }}



    .chat-container {{
        max-width: 56.25rem;
        margin: 0 auto;
        padding: 1.25rem;
    }}



    .message-row {{
        display: flex;
        margin-bottom: 1rem;
        align-items: flex-start;
        animation: fadeIn 0.3s ease-in;
    }}



    @keyframes fadeIn {{
        from {{ opacity: 0; transform: translateY(0.625rem); }}
        to {{ opacity: 1; transform: translateY(0); }}
    }}



    .message-row.user {{
        flex-direction: row-reverse;
        justify-content: flex-start;
    }}



    .message-row.assistant {{
        flex-direction: row;
        justify-content: flex-start;
    }}



    .avatar {{
        width: 2.5rem;
        height: 2.5rem;
        min-width: 2.5rem;
        min-height: 2.5rem;
        border-radius: 50%;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 1.25rem;
        flex-shrink: 0;
        box-shadow: 0 0.1875rem 0.625rem var(--shadow-medium);
        overflow: hidden;
    }}



    .avatar.user {{
        background: transparent;
        margin-left: 0.75rem;
        padding: 0;
        box-shadow: 0 0.1875rem 0.625rem var(--shadow-medium);
    }}



    .avatar.user img {{
        width: 100%;
        height: 100%;
        object-fit: cover;
        border-radius: 50%;
        display: block;
    }}



    .avatar.assistant {{
        background: transparent;
        margin-right: 0.75rem;
        padding: 0;
        box-shadow: 0 0.1875rem 0.625rem var(--shadow-medium);
    }}



    .avatar.assistant img {{
        width: 100%;
        height: 100%;
        object-fit: cover;
        border-radius: 50%;
        display: block;
    }}



    .message-bubble {{
        max-width: 75%;
        padding: 0.75rem 1.125rem;
        border-radius: 1.25rem;
        font-size: 0.9375rem;
        line-height: 1.6;
        word-wrap: break-word;
        overflow-wrap: break-word;
    }}



    .message-bubble.user {{
        background: linear-gradient(135deg, var(--user-gradient-start) 0%, var(--user-gradient-end) 100%);
        color: white;
        border-top-right-radius: 0.25rem;
        box-shadow: 0 0.1875rem 0.75rem rgba(0, 122, 255, 0.25);
    }}



    .message-bubble.assistant {{
        background-color: var(--bg-secondary);
        color: var(--text-primary);
        border-top-left-radius: 0.25rem;
        box-shadow: 0 0.125rem 0.5rem var(--shadow-light);
        border: 1px solid var(--border-color);
    }}



    .loading-bubble {{
        max-width: 75%;
        padding: 1rem 1.125rem;
        border-radius: 1.25rem;
        background-color: var(--bg-secondary);
        border-top-left-radius: 0.25rem;
        box-shadow: 0 0.125rem 0.5rem var(--shadow-light);
        border: 1px solid var(--border-color);
        display: flex;
        align-items: center;
        gap: 0.375rem;
    }}



    .loading-dot {{
        width: 0.5rem;
        height: 0.5rem;
        border-radius: 50%;
        background-color: #8E8E93;
        animation: loadingPulse 1.4s ease-in-out infinite;
    }}



    .loading-dot:nth-child(1) {{ animation-delay: 0s; }}
    .loading-dot:nth-child(2) {{ animation-delay: 0.2s; }}
    .loading-dot:nth-child(3) {{ animation-delay: 0.4s; }}



    @keyframes loadingPulse {{
        0%, 60%, 100% {{ opacity: 0.3; transform: scale(0.8); }}
        30% {{ opacity: 1; transform: scale(1.1); }}
    }}



    .message-bubble.assistant table {{
        width: 100%;
        border-collapse: collapse;
        margin: 0.5rem 0;
        font-size: 0.9rem;
        display: block;
        overflow-x: auto;
        border-radius: 0.75rem;
    }}



    .message-bubble.assistant th,
    .message-bubble.assistant td {{
        border: 1px solid var(--border-color);
        padding: 0.375rem 0.625rem;
        text-align: left;
        vertical-align: top;
        white-space: nowrap;
    }}



    .message-bubble.assistant th {{
        background: rgba(0,0,0,0.03);
        font-weight: 600;
        position: sticky;
        top: 0;
        z-index: 10;
    }}



    .message-bubble.assistant td:first-child,
    .message-bubble.assistant th:first-child {{
        white-space: normal;
        min-width: 8rem;
    }}



    .message-bubble.assistant tbody tr:nth-child(even) {{
        background: rgba(0,0,0,0.02);
    }}



    .message-bubble.assistant tbody tr:hover {{
        background: rgba(0,0,0,0.05);
        transition: background 0.2s ease;
    }}



    [data-testid="stDataFrame"] {{
        background-color: var(--bg-secondary);
        border-radius: 0.75rem;
        overflow: hidden;
        border: 1px solid var(--border-color);
    }}



    .streamlit-expanderHeader {{
        background-color: var(--bg-secondary) !important;
        border-radius: 1rem !important;
        color: var(--text-primary) !important;
        font-weight: 500 !important;
        border: 1px solid var(--border-color) !important;
        padding: 0.75rem 1rem !important;
        box-shadow: 0 0.125rem 0.375rem var(--shadow-light) !important;
    }}



    @media (max-width: 768px) {{
        .message-bubble {{
            max-width: 85%;
            padding: 0.625rem 0.875rem;
            font-size: 0.875rem;
        }}



        .message-bubble.assistant table {{
            font-size: 0.8rem;
        }}



        .message-bubble.assistant th,
        .message-bubble.assistant td {{
            padding: 0.3125rem 0.5rem;
        }}



        .avatar {{
            width: 2rem;
            height: 2rem;
            min-width: 2rem;
            min-height: 2rem;
        }}



        .header-logo {{
            height: 32px;
        }}



        .header-title-text {{
            font-size: 22px;
        }}



        .fixed-header {{
            padding: 0 1rem;
        }}



        .loading-bubble {{
            max-width: 85%;
        }}
    }}
</style>
""",
    unsafe_allow_html=True,
)




# ==================== 고정 헤더 (로고 + 텍스트 제목) ====================
if logo_base64:
    header_html = f"""
    <div class="fixed-header">
        <div class="header-content">
            <img src="data:image/svg+xml;base64,{logo_base64}" class="header-logo" alt="HSE Logo">
            <div class="header-title-text">orcHatStra</div>
        </div>
    </div>
    """
else:
    header_html = """
    <div class="fixed-header">
        <div class="header-content">
            <div class="header-title-text">orcHatStra</div>
        </div>
    </div>
    """


st.markdown(header_html, unsafe_allow_html=True)




# ==================== Secrets ====================
try:
    URL = st.secrets.get("SUPABASE_URL", "https://qipphcdzlmqidhrjnjtt.supabase.co")
    KEY = st.secrets.get("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InFpcHBoY2R6bG1xaWRocmpuanR0Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjY5NTIwMTIsImV4cCI6MjA4MjUyODAxMn0.AsuvjVGCLUJF_IPvQevYASaM6uRF2C6F-CjwC3eCNVk")
    GENAI_KEY = st.secrets.get("GEMINI_API_KEY", "AIzaSyAQaiwm46yOITEttdr0ify7duXCW3TwGRo")
except Exception:
    URL = "https://qipphcdzlmqidhrjnjtt.supabase.co"
    KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InFpcHBoY2R6bG1xaWRocmpuanR0Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjY5NTIwMTIsImV4cCI6MjA4MjUyODAxMn0.AsuvjVGCLUJF_IPvQevYASaM6uRF2C6F-CjwC3eCNVk"
    GENAI_KEY = "AIzaSyAQaiwm46yOITEttdr0ify7duXCW3TwGRo"




@st.cache_resource
def init_supabase():
    return create_client(URL, KEY)




supabase: Client = init_supabase()
genai.configure(api_key=GENAI_KEY)



CAPA_LIMITS = {"조립1": 3300, "조립2": 3700, "조립3": 3600}
TEST_MODE = True
TODAY = datetime(2026, 1, 5).date() if TEST_MODE else datetime.now().date()




# ==================== 데이터 로드 ====================
@st.cache_data(ttl=600)
def fetch_data(target_date=None):
    try:
        if target_date:
            dt = datetime.strptime(target_date, "%Y-%m-%d")
            start_date = (dt - timedelta(days=10)).strftime("%Y-%m-%d")
            end_date = (dt + timedelta(days=10)).strftime("%Y-%m-%d")
            plan_res = (
                supabase.table("production_plan_2026_01")
                .select("*")
                .gte("plan_date", start_date)
                .lte("plan_date", end_date)
                .execute()
            )
        else:
            plan_res = supabase.table("production_plan_2026_01").select("*").execute()



        plan_df = pd.DataFrame(plan_res.data) if plan_res.data else pd.DataFrame()
        hist_res = supabase.table("production_investigation").select("*").execute()
        hist_df = pd.DataFrame(hist_res.data) if hist_res.data else pd.DataFrame()



        if not plan_df.empty:
            plan_df["name_clean"] = plan_df["product_name"].apply(lambda x: re.sub(r"\s+", "", str(x)).strip())
            plt_map = plan_df.groupby("name_clean")["plt"].first().to_dict()
            product_map = plan_df.groupby("name_clean")["line"].unique().to_dict()
            for k in product_map:
                if "T6" in str(k).upper():
                    product_map[k] = ["조립1", "조립2", "조립3"]
            return plan_df, hist_df, product_map, plt_map



        return pd.DataFrame(), pd.DataFrame(), {}, {}
    except Exception as e:
        st.error(f"데이터 로드 실패: {e}")
        return pd.DataFrame(), pd.DataFrame(), {}, {}




def extract_date(text):
    if not text:
        return None
    patterns = [r"(\d{1,2})/(\d{1,2})", r"(\d{1,2})월\s*(\d{1,2})일", r"(202[56])-(\d{1,2})-(\d{1,2})"]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            g = match.groups()
            if len(g) == 2:
                m, d = g
                return f"2026-{int(m):02d}-{int(d):02d}"
            else:
                y, m, d = g
                return f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
    return None




# ==================== HTML 렌더 도구들 ====================
def clean_content(text):
    if not text:
        return ""
    text = re.sub(r"\n\n\n+", "\n\n", text)
    lines = text.split("\n")
    cleaned_lines = [line.rstrip() for line in lines]
    return "\n".join(cleaned_lines)




def detect_table(text):
    if not text:
        return [("text", "")]
    lines = text.split("\n")
    table_lines = []
    result_parts = []
    current_text = []
    for line in lines:
        if line.strip().startswith("|") and line.strip().endswith("|"):
            if current_text:
                result_parts.append(("text", "\n".join(current_text)))
                current_text = []
            table_lines.append(line)
        else:
            if table_lines:
                result_parts.append(("table", table_lines[:]))
                table_lines = []
            current_text.append(line)
    if current_text:
        result_parts.append(("text", "\n".join(current_text)))
    if table_lines:
        result_parts.append(("table", table_lines))
    return result_parts




def parse_table_to_html(table_lines):
    if not table_lines:
        return ""
    html_parts = ["<table>"]
    is_header = True
    header_written = False
    for line in table_lines:
        stripped = line.strip()
        if re.match(r"^\|[\s\-:]+\|[\s\-:|\s]*$", stripped):
            continue
        if not stripped or stripped == "|":
            continue
        cells = [cell.strip() for cell in stripped.split("|")]
        cells = [c for c in cells if c]
        if not cells:
            continue
        if all(re.match(r"^[\-:]+$", cell.strip()) for cell in cells):
            continue
        if is_header and not header_written:
            html_parts.append("<thead><tr>")
            for cell in cells:
                html_parts.append(f"<th>{cell}</th>")
            html_parts.append("</tr></thead><tbody>")
            header_written = True
            is_header = False
        else:
            html_parts.append("<tr>")
            for cell in cells:
                html_parts.append(f"<td>{cell}</td>")
            html_parts.append("</tr>")
    html_parts.append("</tbody></table>")
    return "".join(html_parts)




def markdown_to_html(text):
    import html



    if not text:
        return ""



    text = clean_content(text)
    parts = detect_table(text)
    result_html = []



    for part_type, content in parts:
        if part_type == "table":
            table_html = parse_table_to_html(content)
            result_html.append(table_html)
        else:
            code_blocks = []



            def save_code_block(match):
                code_blocks.append(match.group(0))
                return f"__CODE_BLOCK_{len(code_blocks)-1}__"



            content = re.sub(r"```[\s\S]*?```", save_code_block, content)



            inline_codes = []



            def save_inline_code(match):
                inline_codes.append(match.group(0))
                return f"__INLINE_CODE_{len(inline_codes)-1}__"



            content = re.sub(r"`[^`]+`", save_inline_code, content)



            content = html.escape(content)



            content = re.sub(r"^### (.+)$", r"<h3>\1</h3>", content, flags=re.MULTILINE)
            content = re.sub(r"^## (.+)$", r"<h2>\1</h2>", content, flags=re.MULTILINE)
            content = re.sub(r"^# (.+)$", r"<h3>\1</h3>", content, flags=re.MULTILINE)



            content = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", content)
            content = re.sub(r"__(.+?)__", r"<strong>\1</strong>", content)
            content = re.sub(r"\*(.+?)\*", r"<em>\1</em>", content)
            content = re.sub(r"_(.+?)_", r"<em>\1</em>", content)



            content = re.sub(r"^[\-\*] (.+)$", r"• \1", content, flags=re.MULTILINE)



            for i, code in enumerate(inline_codes):
                code_content = code[1:-1]
                content = content.replace(f"__INLINE_CODE_{i}__", f"<code>{html.escape(code_content)}</code>")



            for i, block in enumerate(code_blocks):
                match = re.match(r"```(\w*)\n?([\s\S]*?)```", block)
                if match:
                    lang, code_content = match.groups()
                    content = content.replace(
                        f"__CODE_BLOCK_{i}__", f"<pre><code>{html.escape(code_content)}</code></pre>"
                    )



            paragraphs = content.split("\n\n")
            formatted_paragraphs = []
            for para in paragraphs:
                para = para.strip()
                if para and not para.startswith("<") and not para.startswith("•"):
                    formatted_paragraphs.append(f"<p>{para}</p>")
                else:
                    formatted_paragraphs.append(para)



            content = "\n".join(formatted_paragraphs)
            content = re.sub(r"(?<!>)\n(?!<)", "<br>", content)
            result_html.append(content)



    return "".join(result_html)




def display_message(role, content):
    if not content:
        return



    if role == "user":
        avatar_html = f'<img src="data:image/png;base64,{user_avatar_base64}" alt="User Avatar">' if user_avatar_base64 else ""
    else:
        avatar_html = f'<img src="data:image/png;base64,{ai_avatar_base64}" alt="AI Avatar">' if ai_avatar_base64 else ""



    html_content = markdown_to_html(content)



    html_output = f"""
    <div class="message-row {role}">
        <div class="avatar {role}">{avatar_html}</div>
        <div class="message-bubble {role}">{html_content}</div>
    </div>
    """
    st.markdown(html_output, unsafe_allow_html=True)




def display_message_html(role: str, html_inner: str):
    if not html_inner:
        return
    if role == "user":
        avatar_html = f'<img src="data:image/png;base64,{user_avatar_base64}" alt="User Avatar">' if user_avatar_base64 else ""
    else:
        avatar_html = f'<img src="data:image/png;base64,{ai_avatar_base64}" alt="AI Avatar">' if ai_avatar_base64 else ""



    html_output = f"""
    <div class="message-row {role}">
        <div class="avatar {role}">{avatar_html}</div>
        <div class="message-bubble {role}">{html_inner}</div>
    </div>
    """
    st.markdown(html_output, unsafe_allow_html=True)




def display_loading():
    avatar_html = f'<img src="data:image/png;base64,{ai_avatar_base64}" alt="AI Avatar">' if ai_avatar_base64 else ""
    html_output = f"""
    <div class="message-row assistant">
        <div class="avatar assistant">{avatar_html}</div>
        <div class="loading-bubble">
            <div class="loading-dot"></div>
            <div class="loading-dot"></div>
            <div class="loading-dot"></div>
        </div>
    </div>
    """
    st.markdown(html_output, unsafe_allow_html=True)




# ==================== hybrid 전용 ====================
def split_report_sections(report_md: str) -> dict:
    if not report_md:
        return {}
    parts = re.split(r"\n##\s+", report_md.strip())
    sections = {"__FULL__": report_md.strip()}
    for p in parts[1:]:
        lines = p.splitlines()
        title = lines[0].strip()
        body = "\n".join(lines[1:]).strip()
        sections[title] = body
    return sections




def build_action_md(report_md: str) -> str:
    sections = split_report_sections(report_md)
    action_key = next((k for k in sections.keys() if "최종 조치 계획" in k), None)
    action_body = sections.get(action_key, "").strip()



    if not action_body:
        return "## 🧾 최종 조치 계획\n(조치계획 없음)"



    if ("|---" in action_body) and re.search(r"^\s*\|.*\|\s*$", action_body, re.MULTILINE):
        filtered = []
        for ln in action_body.splitlines():
            if re.search(r"^\s*\|.*\|\s*$", ln):
                continue
            if re.search(r"^\s*\|\s*-{3,}", ln):
                continue
            filtered.append(ln)
        action_body = "\n".join(filtered).strip()



    return "## 🧾 최종 조치 계획\n" + action_body




def build_delta_html(validated_moves: list | None) -> str:
    if not validated_moves:
        return "<h3>📊 생산계획 변경량 요약(Δ)</h3><p>이동 내역이 없습니다.</p>"



    records = []
    for mv in validated_moves:
        item = str(mv.get("item", "")).strip()
        qty = int(mv.get("qty", 0) or 0)
        from_loc = str(mv.get("from", "") or "")
        to_loc = str(mv.get("to", "") or "")



        if not item or qty <= 0 or "_" not in from_loc or "_" not in to_loc:
            continue



        from_date, from_line = [x.strip() for x in from_loc.split("_", 1)]
        to_date, to_line = [x.strip() for x in to_loc.split("_", 1)]



        records.append({"date": from_date, "item": item, "line": from_line, "delta": -qty})
        records.append({"date": to_date, "item": item, "line": to_line, "delta": +qty})



    df = pd.DataFrame(records)
    if df.empty:
        return "<h3>📊 생산계획 변경량 요약(Δ)</h3><p>표시할 데이터가 없습니다.</p>"



    def _fmt_delta(x):
        if x is None or (isinstance(x, float) and pd.isna(x)) or x == 0:
            return ""
        try:
            n = int(x)
        except Exception:
            return str(x)
        return f"{n:+,}"



    html_parts = ['<h3>📊 생산계획 변경량 요약(Δ)</h3>']



    for date in sorted(df["date"].unique()):
        day = df[df["date"] == date].copy()
        pivot_num = (
            day.pivot_table(index="item", columns="line", values="delta", aggfunc="sum", fill_value=0)
            .reindex(columns=["조립1", "조립2", "조립3"])
            .fillna(0)
        )
        pivot_disp = pivot_num.applymap(_fmt_delta)
        pivot_disp = pivot_disp.loc[~(pivot_disp == "").all(axis=1)]



        html_parts.append(f"<h4>📅 {date} 기준 변경분</h4>")



        if pivot_disp.empty:
            html_parts.append("<p>(변경 없음)</p>")
            continue



        tmp = pivot_disp.copy()
        tmp.insert(0, "item", tmp.index)
        tmp = tmp.reset_index(drop=True)



        table_html = tmp.to_html(index=False, escape=False, border=0)
        html_parts.append(table_html)



    return "".join(html_parts)




def render_hybrid_details_tabs(report_md: str, plan_df: pd.DataFrame | None = None):
    sections = split_report_sections(report_md)



    with st.expander("🔎 상세 보기", expanded=False):
        t1, t2, t3, t4 = st.tabs(["✅ 검증", "📄 원문", "📊 CAPA(텍스트)", "📈 CAPA 그래프"])



        with t1:
            verify_key = next(
                (k for k in sections.keys() if "Python 검증" in k or "검증 결과" in k or "검증" in k),
                None,
            )
            st.markdown(sections.get(verify_key, "검증 섹션이 없습니다."))



        with t2:
            st.markdown(sections.get("__FULL__", report_md))



        with t3:
            capa_key = next((k for k in sections.keys() if "CAPA 현황" in k), None)
            st.markdown(sections.get(capa_key, "CAPA 섹션이 없습니다."))



        with t4:
            if isinstance(plan_df, pd.DataFrame) and (not plan_df.empty) and ("qty_1차" in plan_df.columns):
                daily = plan_df.groupby(["plan_date", "line"])["qty_1차"].sum().reset_index()
                daily.columns = ["plan_date", "line", "current_qty"]



                chart_data = daily.pivot(index="plan_date", columns="line", values="current_qty").fillna(0)



                fig = go.Figure()
                for line in ["조립1", "조립2", "조립3"]:
                    if line in chart_data.columns:
                        fig.add_trace(go.Bar(name=line, x=chart_data.index, y=chart_data[line]))



                for line, limit in CAPA_LIMITS.items():
                    fig.add_hline(y=limit, line_dash="dash", annotation_text=f"{line} 한계: {limit:,}", annotation_position="right")



                fig.update_layout(
                    barmode="group",
                    height=450,
                    xaxis_title="날짜",
                    yaxis_title="수량(개)",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                    hovermode="x unified",
                    plot_bgcolor="rgba(0,0,0,0)",
                    paper_bgcolor="rgba(0,0,0,0)",
                    margin=dict(l=20, r=20, t=40, b=20),
                )



                st.plotly_chart(fig, use_container_width=True)
                st.dataframe(daily, use_container_width=True)
            else:
                st.info("CAPA 그래프를 그릴 데이터가 없습니다.")




# ==================== 세션 상태 ====================
if "messages" not in st.session_state:
    st.session_state.messages = []
if "is_loading" not in st.session_state:
    st.session_state.is_loading = False




# ==================== 채팅 컨테이너 ====================
st.markdown('<div class="chat-container">', unsafe_allow_html=True)



for msg in st.session_state.messages:
    if not isinstance(msg, dict):
        continue



    role = msg.get("role")
    engine = msg.get("engine", "legacy")
    content = msg.get("content", "")



    if role == "user":
        display_message("user", content)
        continue



    if engine == "legacy":
        display_message("assistant", content)
    else:
        action_md = msg.get("action_md", "")
        delta_html = msg.get("delta_html", "")
        report_md = msg.get("report_md", "")
        plan_df = msg.get("plan_df", None)



        display_message("assistant", action_md or "## 🧾 최종 조치 계획\n(조치계획 없음)")
        display_message_html("assistant", delta_html or "<h3>📊 생산계획 변경량 요약(Δ)</h3><p>(변경 없음)</p>")



        if report_md:
            render_hybrid_details_tabs(report_md, plan_df=plan_df)



if st.session_state.is_loading:
    display_loading()



st.markdown("</div>", unsafe_allow_html=True)




# ==================== 사용자 입력 ====================
if prompt := st.chat_input("무엇을 도와드릴까요?"):
    st.session_state.messages.append({"role": "user", "content": prompt, "engine": "legacy"})
    st.session_state.is_loading = True
    st.rerun()




# ==================== 응답 생성 ====================
if st.session_state.is_loading:
    user_messages = [m for m in st.session_state.messages if isinstance(m, dict) and m.get("role") == "user"]
    prompt = user_messages[-1]["content"] if user_messages else ""
    target_date = extract_date(prompt)



    is_adjustment_mode = bool(target_date) and (
        any(line in prompt for line in ["조립1", "조립2", "조립3", "조립"])
        or re.search(r"\d+%", prompt) is not None
        or "CAPA" in prompt.upper()
        or any(k in prompt for k in ["줄여", "늘려", "추가", "증량", "감량", "생산하고"])
    )



    try:
        if is_adjustment_mode:
            plan_df, hist_df, product_map, plt_map = fetch_data(target_date)



            if plan_df.empty:
                st.session_state.messages.append(
                    {
                        "role": "assistant",
                        "engine": "hybrid",
                        "content": "",
                        "action_md": "## 🧾 최종 조치 계획\n❌ 데이터를 불러올 수 없습니다.",
                        "delta_html": "<h3>📊 생산계획 변경량 요약(Δ)</h3><p>데이터가 없습니다.</p>",
                        "validated_moves": None,
                        "report_md": "",
                    }
                )
            else:
                result = ask_professional_scheduler(
                    question=prompt,
                    plan_df=plan_df,
                    hist_df=hist_df,
                    product_map=product_map,
                    plt_map=plt_map,
                    question_date=target_date,
                    mode="hybrid",
                    today=TODAY,
                    capa_limits=CAPA_LIMITS,
                    genai_key=GENAI_KEY,
                )



                report, success, charts, status, validated_moves = "", False, None, "", None
                if isinstance(result, (tuple, list)):
                    if len(result) == 5:
                        report, success, charts, status, validated_moves = result
                    elif len(result) == 4:
                        report, success, charts, status = result
                        validated_moves = None
                    else:
                        report = str(result)
                        status = "생산계획 조정 결과 파싱 실패"
                else:
                    report = str(result)
                    status = "생산계획 조정 결과 파싱 실패"



                action_md = build_action_md(report)
                delta_html = build_delta_html(validated_moves)



                st.session_state.messages.append(
                    {
                        "role": "assistant",
                        "engine": "hybrid",
                        "content": "",
                        "action_md": action_md,
                        "delta_html": delta_html,
                        "validated_moves": validated_moves,
                        "report_md": report,
                        "plan_df": plan_df,
                    }
                )



        else:
            db_result = fetch_db_data_legacy(prompt, supabase)
            if "찾을 수 없습니다" in db_result or "오류" in db_result:
                answer = db_result
            else:
                answer = query_gemini_ai_legacy(prompt, db_result, GENAI_KEY)



            st.session_state.messages.append({"role": "assistant", "engine": "legacy", "content": answer})



    except Exception as e:
        error_msg = f"❌ **오류 발생**\n\n```\n{str(e)}\n```"
        st.session_state.messages.append({"role": "assistant", "engine": "legacy", "content": error_msg})
    finally:
        st.session_state.is_loading = False
        st.rerun()
