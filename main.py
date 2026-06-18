#!/usr/bin/env python3
"""
IR情報・WTI原油価格 自動通知スクリプト
毎朝 GitHub Actions で自動実行 → Resend でメール送信
"""

from __future__ import annotations

import os
import re
import io
import json
import time
import zipfile
import xml.etree.ElementTree as ET
import resend
import requests
import anthropic
from datetime import date, timedelta
from bs4 import BeautifulSoup

# ============================================================
# 設定（GitHub Secrets から自動的に読み込まれます）
# ============================================================
resend.api_key     = os.environ["RESEND_API_KEY"]          # Resend API キー
EMAIL_FROM         = os.environ["EMAIL_FROM"]               # 送信元アドレス
EMAIL_TO           = os.environ["EMAIL_TO"]                 # 受信先メールアドレス
EDINET_API_KEY     = os.environ.get("EDINET_API_KEY", "")  # EDINET API キー（任意）
ANTHROPIC_API_KEY  = os.environ.get("ANTHROPIC_API_KEY", "")  # Claude API キー（任意）

# ============================================================
# 監視銘柄リスト
# ============================================================
STOCKS = [
    {"name": "ジャパンマテリアル", "code": "6055", "next_earnings": "2026-05-13", "earnings_note": "本決算"},
    {"name": "エクシオグループ",   "code": "1951", "next_earnings": None,         "earnings_note": ""},
    {"name": "ダイダン",           "code": "1980", "next_earnings": None,         "earnings_note": ""},
    {"name": "キオクシア",         "code": "285A", "next_earnings": None,         "earnings_note": ""},
    {"name": "アズビル",           "code": "6845", "next_earnings": None,         "earnings_note": ""},
    {"name": "日本電子材料",       "code": "6855", "next_earnings": "2026-05-14", "earnings_note": "本決算★ダブルバガー判断日"},
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Upgrade-Insecure-Requests": "1",
}

_kabutan_session: requests.Session | None = None


def _get_kabutan_session() -> requests.Session:
    global _kabutan_session
    if _kabutan_session is not None:
        return _kabutan_session
    session = requests.Session()
    session.headers.update(HEADERS)
    try:
        session.get("https://kabutan.jp/", timeout=15)
    except Exception:
        pass
    _kabutan_session = session
    return session

# ============================================================
# バフェット指標スクリーニング 閾値
# ============================================================
THEME_INDICATORS = [
    {"name": "安川電機",         "code": "6506", "theme": "半導体・FA",     "note": "FA・ロボット先行指標"},
    {"name": "東京エレクトロン", "code": "8035", "theme": "半導体製造装置", "note": "半導体サイクル先行指標"},
    {"name": "レーザーテック",   "code": "6920", "theme": "半導体検査",     "note": "半導体高値圏の温度計"},
    {"name": "商船三井",         "code": "9104", "theme": "海運",           "note": "地政学リスク・ホルムズ指標"},
    {"name": "INPEX",            "code": "1605", "theme": "原油",           "note": "WTI連動・エネルギー指標"},
]

ROE_MIN          = 15.0
EQUITY_RATIO_MIN = 40.0


# ============================================================
# EDINET v2 API
# ============================================================
_EDINET_BASE = "https://disclosure.edinet-fsa.go.jp/api/v2"
_XBRL_NS     = "http://www.xbrl.org/2003/instance"
_XSI_NIL     = "{http://www.w3.org/2001/XMLSchema-instance}nil"

_XBRL_ELEMS = {
    "sales": {
        "NetSalesSummaryOfBusinessResults", "NetSales",
        "Revenues", "OperatingRevenues",
    },
    "op_income": {
        "OperatingIncomeLossSummaryOfBusinessResults",
        "OperatingIncome", "OperatingIncomeLoss",
    },
    "net_income": {
        "ProfitLossAttributableToOwnersOfParentSummaryOfBusinessResults",
        "ProfitLossAttributableToOwnersOfParent", "ProfitLoss",
    },
    "sales_fc": {"NetSalesForecastSummaryOfBusinessResults"},
    "op_fc": {
        "OperatingIncomeLossForecastSummaryOfBusinessResults",
        "OperatingIncomeForecastSummaryOfBusinessResults",
    },
    "net_fc": {
        "ProfitLossAttributableToOwnersOfParentForecastSummaryOfBusinessResults",
    },
}


def _edinet_headers() -> dict:
    return {"Ocp-Apim-Subscription-Key": EDINET_API_KEY}


def _edinet_doc_list(date_str: str) -> list:
    try:
        res = requests.get(
            f"{_EDINET_BASE}/documents.json",
            params={"date": date_str, "type": 2},
            headers=_edinet_headers(),
            timeout=30,
        )
        res.raise_for_status()
        data = res.json()
        if data.get("statusCode") == 401:
            return []
        return data.get("results") or []
    except Exception:
        return []


def _match_sec_code(raw: str, codes4: list) -> str | None:
    raw = (raw or "").strip()
    if len(raw) >= 4 and raw[:4] in codes4:
        return raw[:4]
    if len(raw) == 5 and raw[4] == "0" and raw[:4] in codes4:
        return raw[:4]
    return None


def _find_quarterly_docs(codes4: list) -> dict:
    TARGET_CODES = {"120", "130"}
    found, today = {}, date.today()
    for delta in range(90):
        if len(found) == len(codes4):
            break
        d = (today - timedelta(days=delta)).strftime("%Y-%m-%d")
        for doc in _edinet_doc_list(d):
            raw_sec = doc.get("secCode") or ""
            sec = _match_sec_code(raw_sec, codes4)
            if sec is None or sec in found:
                continue
            if doc.get("docTypeCode") in TARGET_CODES:
                print(f"    [EDINET] {sec} 書類発見: docID={doc.get('docID')} "
                      f"type={doc.get('docTypeCode')} date={d} secCode={raw_sec}")
                found[sec] = doc
    return found


def _fetch_xbrl_text(doc_id: str) -> str:
    res = requests.get(
        f"{_EDINET_BASE}/documents/{doc_id}",
        params={"type": 1},
        headers=_edinet_headers(),
        timeout=60,
    )
    res.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(res.content)) as z:
        candidates = [n for n in z.namelist()
                      if n.endswith(".xbrl") and "PublicDoc" in n]
        if not candidates:
            candidates = [n for n in z.namelist() if n.endswith(".xbrl")]
        if not candidates:
            return ""
        biggest = max(candidates, key=lambda n: z.getinfo(n).file_size)
        return z.read(biggest).decode("utf-8", errors="replace")


def _parse_xbrl(xbrl: str) -> dict:
    try:
        root = ET.fromstring(xbrl)
    except ET.ParseError:
        return {}

    actual_ctx, forecast_ctx = set(), set()
    for ctx in root.findall(f"{{{_XBRL_NS}}}context"):
        cid = ctx.get("id", "")
        if "Forecast" in cid:
            forecast_ctx.add(cid)
        elif re.search(r"CurrentAccumulated|CurrentYear", cid) and "Prior" not in cid:
            actual_ctx.add(cid)
    if not actual_ctx:
        for ctx in root.findall(f"{{{_XBRL_NS}}}context"):
            cid = ctx.get("id", "")
            if "Current" in cid and "Forecast" not in cid and "Prior" not in cid:
                actual_ctx.add(cid)

    quarter = None
    for cid in actual_ctx:
        m = re.search(r"Q([123])", cid)
        if m:
            quarter = f"Q{m.group(1)}"
            break

    out: dict = {"quarter": quarter}

    for elem in root.iter():
        if elem.get(_XSI_NIL) == "true":
            continue
        local   = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
        ctx_ref = elem.get("contextRef", "")
        try:
            val = float(elem.text.strip())
        except (ValueError, AttributeError, TypeError):
            continue

        for key, names in _XBRL_ELEMS.items():
            if key in out or local not in names:
                continue
            is_fc = key.endswith("_fc")
            if is_fc and ctx_ref in forecast_ctx:
                out[key] = val
                break
            elif not is_fc and ctx_ref in actual_ctx:
                out[key] = val
                break

    return out


def _judge(progress_pct: float, quarter: str) -> str:
    pace = {"Q1": 25.0, "Q2": 50.0, "Q3": 75.0}.get(quarter, 100.0)
    diff = progress_pct - pace
    if diff >= -5.0:
        return "○"
    elif diff >= -15.0:
        return "△"
    else:
        return "×"


def get_edinet_financials(stocks: list) -> list:
    if not EDINET_API_KEY:
        return []
    codes4 = [s["code"][:4] for s in stocks]
    print("  EDINET 四半期報告書を検索中 (最大90日遡り)...")
    docs_map = _find_quarterly_docs(codes4)

    results = []
    for stock in stocks:
        c4    = stock["code"][:4]
        doc   = docs_map.get(c4)
        entry: dict = {"stock": stock, "doc": doc}

        if not doc:
            entry["error"] = "四半期報告書が見つかりませんでした"
            results.append(entry)
            continue

        print(f"    [{stock['code']}] XBRL取得中 ({doc['docID']})...")
        try:
            xbrl_text = _fetch_xbrl_text(doc["docID"])
        except Exception as e:
            entry["error"] = f"XBRL取得失敗: {e}"
            results.append(entry)
            continue

        if not xbrl_text:
            entry["error"] = "XBRLファイルが見つかりませんでした"
            results.append(entry)
            continue

        entry["financials"] = _parse_xbrl(xbrl_text)
        results.append(entry)

    return results


# ============================================================
# アルジャジーラ RSS
# ============================================================
_WORLD_KEYWORDS = [
    "economy", "economic", "inflation", "recession", "gdp", "debt", "trade",
    "tariff", "sanction", "dollar", "currency", "market", "finance", "financial",
    "bank", "interest rate", "federal reserve", "central bank", "imf",
    "investment", "bond", "deficit", "surplus", "export", "import",
    "oil", "gas", "energy", "opec", "crude", "petroleum", "fuel", "nuclear",
    "pipeline", "lng", "electricity", "renewable",
    "war", "conflict", "tension", "crisis", "military", "attack", "ceasefire",
    "missile", "protest", "coup", "nato", "troops", "invasion", "occupation",
    "blockade", "embargo", "strait", "geopolit", "escalat", "airstrike",
    "sanction", "alliance", "treaty",
]

def get_aljazeera_news(max_items: int = 7) -> list:
    url = f"https://www.aljazeera.com/xml/rss/all.xml?_={int(time.time())}"
    try:
        res = requests.get(url, headers={
            "User-Agent": "Mozilla/5.0",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }, timeout=15)
        res.raise_for_status()
        root = ET.fromstring(res.content)

        scored = []
        for item in root.findall(".//item"):
            title    = (item.findtext("title")    or "").strip()
            link     = (item.findtext("link")     or "").strip()
            pub_date = (item.findtext("pubDate")  or "").strip()
            desc     = (item.findtext("description") or "").strip()

            text  = (title + " " + desc).lower()
            score = sum(1 for kw in _WORLD_KEYWORDS if kw in text)
            if score > 0:
                scored.append({
                    "score":    score,
                    "title":    title,
                    "url":      link,
                    "pub_date": pub_date,
                })

        scored.sort(key=lambda x: x["score"], reverse=True)
        return scored[:max_items]

    except Exception as e:
        print(f"    [警告] アルジャジーラRSS取得失敗: {e}")
    return []


def analyze_aljazeera_news(news_items: list) -> list:
    if not ANTHROPIC_API_KEY or not news_items:
        return news_items

    articles_text = "\n".join(
        f"[{i + 1}] {n['title']}"
        for i, n in enumerate(news_items)
    )
    prompt = (
        "以下のニュース記事が日本株市場に影響を与える可能性があるか判断してください。\n"
        "各記事について:\n"
        "- 影響がある場合: {\"impact\": \"high\", \"summary\": \"日本語要約1〜2行\"}\n"
        "- 影響がない場合: {\"impact\": \"low\"}\n\n"
        "記事:\n"
        f"{articles_text}\n\n"
        "回答は上記と同じ順序でJSONオブジェクトの配列のみを返してください。"
        "余分なテキストや```は不要です。例: [{\"impact\":\"high\",\"summary\":\"...\"},{\"impact\":\"low\"}]"
    )

    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = response.content[0].text.strip()
        raw = re.sub(r"^```[^\n]*\n?", "", raw)
        raw = re.sub(r"\n?```$", "", raw)
        results = json.loads(raw)

        analyzed = []
        for i, n in enumerate(news_items):
            r = results[i] if i < len(results) else {"impact": "low"}
            analyzed.append({
                **n,
                "impact":  r.get("impact", "low"),
                "summary": r.get("summary", ""),
            })
        print(f"    Claude分析完了: high={sum(1 for a in analyzed if a['impact']=='high')}件")
        return analyzed

    except Exception as e:
        print(f"    [警告] Claude API分析失敗: {e}")
        return news_items


# ============================================================
# Claude API — バフェット視点
# ============================================================
_BUFFETT_CRITERIA = """
バフェット投資哲学の基準：
1. ROE 15%以上を継続できるか（資本効率の高さ）
2. 自己資本比率が高く財務健全か（借金に頼らない経営）
3. 営業利益率 10%以上で競争優位性があるか（経済的な堀）
4. PEG≤1で成長に対して株価が割安か
5. EV/EBITDA≤10で企業価値が適正か
6. CFパターンが安定型・成長型か（キャッシュ創出力）
7. 来期利益予想が成長しているか（モメンタム）
8. netCashRatio≥0（純現金がプラス、財務的安全余裕）
""".strip()


def analyze_with_buffett_lens(stock_data: list) -> dict:
    if not ANTHROPIC_API_KEY or not stock_data:
        return {}

    lines = []
    for d in stock_data:
        f    = d.get("buffett", {})
        name = d["stock"]["name"]
        code = d["stock"]["code"]
        lines.append(
            f"[{code}] {name}: "
            f"ROE={f.get('roe')}% 自己資本比率={f.get('equity_ratio')}% "
            f"ROIC={f.get('roic')}% 営業利益率={f.get('op_margin')}% "
            f"売上成長率={f.get('sales_growth')}% CFパターン={f.get('cf_pattern')} "
            f"来期純利益予想={f.get('ni_forecast_yoy')}% "
            f"PEG={f.get('peg')} EV/EBITDA={f.get('ev_ebitda')} "
            f"netCashRatio={f.get('net_cash_ratio')} スコア={f.get('health_score')}/100"
        )
    stocks_text = "\n".join(lines)

    prompt = (
        f"{_BUFFETT_CRITERIA}\n\n"
        "上記の基準に基づき、以下の日本株について各銘柄のバフェット視点での評価を行ってください。\n\n"
        f"{stocks_text}\n\n"
        "verdictには必ず以下の4つのいずれかのみを使用してください（現在は全銘柄未保有）：\n"
        "・新規買い候補 → バフェット基準を満たし、新規買いを検討できる水準\n"
        "・押し目待ち　 → 質は高いが株価が割高、押し目を待ちたい\n"
        "・要観察　　　 → 条件が揃うまで待機、モニタリング継続\n"
        "・見送り　　　 → 現時点では買わない\n\n"
        "各銘柄について以下のJSON形式で回答してください：\n"
        '{"verdict": "新規買い候補|押し目待ち|要観察|見送り", "comment": "日本語1〜2行のコメント"}\n\n'
        "回答は銘柄コードをキーとするJSONオブジェクトのみを返してください。余分なテキストや```は不要です。\n"
        '例: {"6055": {"verdict": "押し目待ち", "comment": "..."}, "4890": {"verdict": "要観察", "comment": "..."}}'
    )

    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = response.content[0].text.strip()
        raw = re.sub(r"^```[^\n]*\n?", "", raw)
        raw = re.sub(r"\n?```$", "", raw)
        result = json.loads(raw)
        print(f"    Buffett分析完了: {len(result)}銘柄")
        return result
    except Exception as e:
        print(f"    [警告] Buffett分析失敗: {e}")
        return {}


# ============================================================
# 関税・地政学リスクアラート
# ============================================================
_TARIFF_KEYWORDS_EN = [
    "tariff", "trade war", "sanctions", "geopolitical", "export control",
    "trade restriction", "customs duty", "protectionism", "trump trade",
]

_TARIFF_KEYWORDS_JA = [
    "関税", "トランプ", "貿易摩擦", "経済制裁", "輸出規制",
    "地政学", "円安", "通商", "保護主義", "半導体規制",
]

_NHK_RSS_FEEDS = [
    ("経済", "https://www3.nhk.or.jp/rss/news/cat4.xml"),
    ("国際", "https://www3.nhk.or.jp/rss/news/cat6.xml"),
]


def get_nhk_risk_news(max_per_feed: int = 5) -> list:
    results = []
    for _category, url in _NHK_RSS_FEEDS:
        try:
            res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
            res.raise_for_status()
            root = ET.fromstring(res.content)
            count = 0
            for item in root.findall(".//item"):
                if count >= max_per_feed:
                    break
                title    = (item.findtext("title")   or "").strip()
                link     = (item.findtext("link")    or "").strip()
                pub_date = (item.findtext("pubDate") or "").strip()
                desc     = (item.findtext("description") or "").strip()
                text     = title + " " + desc
                if any(kw in text for kw in _TARIFF_KEYWORDS_JA):
                    results.append({
                        "title":    title,
                        "url":      link,
                        "pub_date": pub_date,
                    })
                    count += 1
        except Exception as e:
            print(f"    [警告] NHK RSS取得失敗 ({url}): {e}")
    return results


# ============================================================
# 世界ビジネスニュース RSS
# ============================================================
_WORLD_NEWS_KEYWORDS = [
    "economy", "economic", "inflation", "recession", "gdp", "trade",
    "tariff", "sanction", "dollar", "currency", "market", "finance",
    "bank", "interest rate", "federal reserve", "central bank", "imf",
    "investment", "bond", "deficit", "export", "import",
    "oil", "gas", "energy", "opec", "crude", "petroleum", "fuel",
    "pipeline", "lng", "nuclear",
    "war", "conflict", "tension", "crisis", "military", "ceasefire",
    "missile", "coup", "nato", "invasion", "embargo", "strait",
    "geopolit", "escalat", "sanction", "alliance",
    "earnings", "revenue", "profit", "stock", "shares", "ipo",
    "merger", "acquisition", "supply chain", "semiconductor",
]

_WORLD_NEWS_FEEDS = [
    ("BBC Business",    "https://feeds.bbci.co.uk/news/business/rss.xml"),
    ("AP Business",     "https://rsshub.app/ap/topics/business"),
    ("Financial Times", "https://www.ft.com/rss/home/uk"),
]

_WORLD_NEWS_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


def get_world_business_news(max_items: int = 5) -> tuple[list, str]:
    for source_name, url in _WORLD_NEWS_FEEDS:
        scored = []
        try:
            res = requests.get(url, headers={
                "User-Agent": _WORLD_NEWS_UA,
                "Accept": "application/rss+xml, application/xml, text/xml, */*",
                "Cache-Control": "no-cache",
            }, timeout=15)
            res.raise_for_status()
            root = ET.fromstring(res.content)

            for item in root.findall(".//item"):
                title    = (item.findtext("title")       or "").strip()
                link     = (item.findtext("link")        or "").strip()
                pub_date = (item.findtext("pubDate")     or "").strip()
                desc     = (item.findtext("description") or "").strip()

                text  = (title + " " + desc).lower()
                score = sum(1 for kw in _WORLD_NEWS_KEYWORDS if kw in text)
                if score > 0:
                    scored.append({
                        "score":    score,
                        "title":    title,
                        "url":      link,
                        "pub_date": pub_date,
                    })

            if scored:
                seen, unique = set(), []
                for n in sorted(scored, key=lambda x: x["score"], reverse=True):
                    if n["title"] not in seen:
                        seen.add(n["title"])
                        unique.append(n)
                print(f"    世界ビジネスニュース取得成功: {source_name} ({len(unique)}件)")
                return unique[:max_items], source_name

            print(f"    [情報] {source_name}: 条件合致ニュースなし、次のソースへ")

        except Exception as e:
            print(f"    [警告] {source_name} RSS取得失敗: {e}")

    print("    [警告] すべてのニュースソースで取得失敗")
    return [], ""


def get_reuters_news(max_items: int = 5) -> list:
    news, _ = get_world_business_news(max_items)
    return news


# ============================================================
# バフェット指標取得
# ============================================================
def _extract_col_value(table, col_keywords: list, exact: bool = False) -> float | None:
    rows = table.find_all("tr")
    if not rows:
        return None

    header_cells = [c.get_text(strip=True) for c in rows[0].find_all(["th", "td"])]
    col_idx = None
    for i, h in enumerate(header_cells):
        h_norm = h.replace("Ｒ", "R").replace("Ｏ", "O").replace("Ｅ", "E")
        if exact:
            match = any(kw == h or kw == h_norm for kw in col_keywords)
        else:
            match = any(kw in h or kw in h_norm for kw in col_keywords)
        if match:
            col_idx = i
            break
    if col_idx is None:
        return None

    for row in reversed(rows[1:]):
        cells = row.find_all(["th", "td"])
        if not cells or col_idx >= len(cells):
            continue
        first = cells[0].get_text(strip=True)
        if first.startswith("予") or not first:
            continue
        raw = (cells[col_idx].get_text(strip=True)
               .replace("%", "").replace(",", "")
               .replace("－", "").replace("―", "").strip())
        if not raw:
            continue
        try:
            return float(raw)
        except ValueError:
            pass
    return None


def _parse_ratio_float(s: str) -> float | None:
    raw = s.replace("倍", "").replace(",", "").replace("－", "").strip()
    try:
        return float(raw)
    except ValueError:
        return None


def _parse_mktcap_mn(s: str) -> float | None:
    s = s.replace(",", "").replace("円", "").strip()
    mn = 0.0
    if "兆" in s:
        parts = s.split("兆")
        mn += float(parts[0]) * 1_000_000
        s = parts[1]
    if "億" in s:
        mn += float(s.replace("億", "")) * 100
        return mn
    return None


def _classify_cf_pattern(op_cf: float, inv_cf: float, fin_cf: float) -> str:
    op_pos  = op_cf  > 0
    inv_neg = inv_cf < 0
    fin_pos = fin_cf > 0
    if op_pos and inv_neg and fin_pos:
        return "成長型"
    if op_pos and inv_neg and not fin_pos:
        return "安定型"
    if op_pos and not inv_neg and not fin_pos:
        return "収穫型"
    if op_pos and not inv_neg and fin_pos:
        return "キャッシュ蓄積型"
    if not op_pos and inv_cf > 0 and fin_pos:
        return "再建型"
    if not op_pos and inv_cf > 0 and not fin_pos:
        return "リストラ型"
    if not op_pos and inv_neg:
        return "危険型"
    return "その他"


def _calc_health_score(f: dict) -> int:
    score = 0
    roe = f.get("roe")
    if roe is not None:
        if roe >= 20:   score += 25
        elif roe >= 15: score += 20
        elif roe >= 10: score += 15
        elif roe >= 5:  score += 8

    roic = f.get("roic")
    if roic is not None:
        if roic >= 15:   score += 25
        elif roic >= 10: score += 20
        elif roic >= 5:  score += 10

    cf_scores = {
        "安定型": 20, "成長型": 15, "収穫型": 15,
        "キャッシュ蓄積型": 10, "再建型": 5, "リストラ型": 5, "危険型": 0,
    }
    score += cf_scores.get(f.get("cf_pattern") or "", 0)

    sg = f.get("sales_growth")
    if sg is not None:
        if sg >= 10:  score += 20
        elif sg >= 5: score += 15
        elif sg >= 0: score += 8

    om = f.get("op_margin")
    if om is not None:
        if om >= 20:   score += 10
        elif om >= 10: score += 8
        elif om >= 5:  score += 4

    return score


def _extract_bs_value(tables, *keywords: str) -> float | None:
    for table in tables:
        for row in table.find_all("tr"):
            cells = row.find_all(["th", "td"])
            if len(cells) < 2:
                continue
            label = cells[0].get_text(strip=True)
            if any(kw == label for kw in keywords):
                for i in range(1, min(5, len(cells))):
                    raw = (cells[i].get_text(strip=True)
                           .replace(",", "").replace("－", "").replace("―", "").strip())
                    if raw:
                        try:
                            return float(raw)
                        except ValueError:
                            continue
    return None


def get_financial_data(code: str) -> dict:
    url = f"https://kabutan.jp/stock/finance?code={code}"
    try:
        session = _get_kabutan_session()
        res = session.get(url, headers={"Referer": "https://kabutan.jp/"}, timeout=15)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")

        roe = equity_ratio = op_margin = None
        op_income = equity = debt_ratio = None
        op_cf = inv_cf = fin_cf = None
        actual_ni = forecast_ni = None
        sales_growth = None
        per = pbr = mktcap_mn = cash = None

        for table in soup.find_all("table"):
            tr = table.find("tr")
            if not tr:
                continue
            header_cells = [c.get_text(strip=True) for c in tr.find_all(["th", "td"])]
            header_text  = " ".join(header_cells)

            if per is None and "PER" in header_cells and "PBR" in header_cells:
                rows_t2 = table.find_all("tr")
                if len(rows_t2) >= 2:
                    val_cells = [c.get_text(strip=True) for c in rows_t2[1].find_all("td")]
                    per = _parse_ratio_float(val_cells[0]) if len(val_cells) > 0 else None
                    pbr = _parse_ratio_float(val_cells[1]) if len(val_cells) > 1 else None
                if len(rows_t2) >= 3:
                    mc_cells = [c.get_text(strip=True) for c in rows_t2[2].find_all(["th", "td"])]
                    if len(mc_cells) >= 2:
                        mktcap_mn = _parse_mktcap_mn(mc_cells[1])

            elif roe is None and ("ＲＯＥ" in header_text or "ROE" in header_text) and "総資産回転率" in header_text:
                roe       = _extract_col_value(table, ["ＲＯＥ", "ROE"])
                op_income = _extract_col_value(table, ["営業益"])
                op_margin = _extract_col_value(table, ["売上営業利益率"])

            elif equity_ratio is None and "自己資本比率" in header_text and "有利子負債倍率" in header_text:
                equity_ratio = _extract_col_value(table, ["自己資本比率"])
                equity       = _extract_col_value(table, ["自己資本"], exact=True)
                debt_ratio   = _extract_col_value(table, ["有利子負債倍率"])

            elif op_cf is None and "営業CF" in header_text and "投資CF" in header_text:
                op_cf  = _extract_col_value(table, ["営業CF"],     exact=True)
                inv_cf = _extract_col_value(table, ["投資CF"],     exact=True)
                fin_cf = _extract_col_value(table, ["財務CF"],     exact=True)
                cash   = _extract_col_value(table, ["現金等残高"], exact=True)

            elif actual_ni is None and "最終益" in header_cells and "修正1株配" in header_cells:
                ni_col    = header_cells.index("最終益")
                sales_col = header_cells.index("売上高") if "売上高" in header_cells else None

                sales_actual: list[float] = []
                for row in reversed(table.find_all("tr")[1:]):
                    cells = row.find_all(["th", "td"])
                    if not cells or ni_col >= len(cells):
                        continue
                    first = cells[0].get_text(strip=True)
                    if not first:
                        continue
                    period = first.lstrip("予連")
                    if not re.match(r'^\d{4}\.\d{2}$', period):
                        continue
                    val_raw = (cells[ni_col].get_text(strip=True)
                               .replace(",", "").replace("－", "").replace("―", "").strip())
                    try:
                        val = float(val_raw)
                    except ValueError:
                        val = None
                    if first.startswith("予"):
                        if forecast_ni is None and val is not None:
                            forecast_ni = val
                    else:
                        if actual_ni is None and val is not None:
                            actual_ni = val
                        if sales_col is not None and len(sales_actual) < 2:
                            s_raw = (cells[sales_col].get_text(strip=True)
                                     .replace(",", "").strip())
                            try:
                                sales_actual.append(float(s_raw))
                            except ValueError:
                                pass

                if len(sales_actual) >= 2 and sales_actual[1] != 0:
                    sales_growth = round(
                        (sales_actual[0] - sales_actual[1]) / sales_actual[1] * 100, 1
                    )

        roic = None
        if op_income is not None and equity is not None and equity > 0:
            dr              = debt_ratio if debt_ratio is not None else 0.0
            invested_capital = equity * (1 + dr) - (cash or 0)
            if invested_capital > 0:
                roic = round(op_income / invested_capital * 100, 1)

        cf_pattern = None
        if op_cf is not None and inv_cf is not None and fin_cf is not None:
            cf_pattern = _classify_cf_pattern(op_cf, inv_cf, fin_cf)

        ni_forecast_yoy = None
        if actual_ni is not None and forecast_ni is not None and actual_ni != 0:
            ni_forecast_yoy = round((forecast_ni - actual_ni) / abs(actual_ni) * 100, 1)

        peg = None
        if per is not None and sales_growth is not None and sales_growth > 0:
            peg = round(per / sales_growth, 2)

        graham = None
        if per is not None and pbr is not None:
            graham = round(per * pbr, 1)

        ev_ebitda = None
        _interest_debt = (equity or 0) * (debt_ratio if debt_ratio is not None else 0.0)
        if mktcap_mn is not None and op_income is not None and op_income > 0:
            ev = mktcap_mn + _interest_debt - (cash or 0)
            ev_ebitda = round(ev / op_income, 1)

        net_cash_ratio = None
        net_cash_ratio_approx = False
        all_tables = soup.find_all("table")
        _current_assets   = _extract_bs_value(all_tables, "流動資産", "流動資産合計")
        _invest_sec       = _extract_bs_value(all_tables, "投資有価証券")
        _total_liabilities = _extract_bs_value(all_tables, "負債合計")
        if _current_assets is not None and _total_liabilities is not None and mktcap_mn is not None and mktcap_mn > 0:
            if _invest_sec is not None:
                _net_cash = _current_assets + _invest_sec * 0.7 - _total_liabilities
            else:
                _net_cash = _current_assets - _total_liabilities
                net_cash_ratio_approx = True
            net_cash_ratio = round(_net_cash / mktcap_mn, 2)
        elif cash is not None and mktcap_mn is not None and mktcap_mn > 0:
            net_cash_ratio = round((cash - _interest_debt) / mktcap_mn, 2)
            net_cash_ratio_approx = True

        financials_for_score = {
            "roe": roe, "roic": roic, "cf_pattern": cf_pattern,
            "sales_growth": sales_growth, "op_margin": op_margin,
        }
        health_score = _calc_health_score(financials_for_score)

        print(
            f"    財務データ: ROE={roe} 自己資本比率={equity_ratio} ROIC={roic}% "
            f"営業利益率={op_margin}% 売上成長率={sales_growth}% CF={cf_pattern} "
            f"来期益={ni_forecast_yoy}% PEG={peg} グレアム={graham} EV/EBITDA={ev_ebitda} "
            f"netCashRatio={net_cash_ratio}{'(近似)' if net_cash_ratio_approx else ''} スコア={health_score}"
        )
        return {
            "roe": roe, "equity_ratio": equity_ratio,
            "roic": roic, "cf_pattern": cf_pattern,
            "ni_forecast_yoy": ni_forecast_yoy,
            "op_margin": op_margin, "sales_growth": sales_growth,
            "health_score": health_score,
            "per": per, "pbr": pbr,
            "peg": peg, "graham": graham, "ev_ebitda": ev_ebitda,
            "net_cash_ratio": net_cash_ratio,
            "net_cash_ratio_approx": net_cash_ratio_approx,
        }

    except Exception as e:
        print(f"    [警告] {code} の財務データ取得失敗: {e}")
        return {
            "roe": None, "equity_ratio": None,
            "roic": None, "cf_pattern": None, "ni_forecast_yoy": None,
            "op_margin": None, "sales_growth": None, "health_score": 0,
            "per": None, "pbr": None,
            "peg": None, "graham": None, "ev_ebitda": None,
            "net_cash_ratio": None, "net_cash_ratio_approx": False,
        }


def passes_buffett_screen(financials: dict) -> bool:
    roe = financials.get("roe")
    eq  = financials.get("equity_ratio")
    if roe is None or eq is None:
        return False
    return roe >= ROE_MIN and eq >= EQUITY_RATIO_MIN


# ============================================================
# IR・ニュース取得
# ============================================================
def get_stock_news(code: str, max_items: int = 5) -> list:
    results = []

    for path in [f"/stock/news?code={code}"]:
        url = f"https://kabutan.jp{path}"
        try:
            session = _get_kabutan_session()
            res = session.get(url, headers={"Referer": "https://kabutan.jp/"}, timeout=15)
            res.raise_for_status()
            soup = BeautifulSoup(res.text, "html.parser")

            SELECTORS = [
                "table.s-news-list tr",
                "table.news_list tr",
                "#newslist table tr",
                "div#news_list table tr",
                "div.news_box table tr",
                "table tr",
            ]
            rows = []
            for sel in SELECTORS:
                rows = soup.select(sel)
                if rows:
                    break

            for row in rows:
                tds = row.find_all("td")
                if len(tds) < 2:
                    continue

                date_text = tds[0].get_text(strip=True)
                if not date_text or date_text in ("日付", "日時", ""):
                    continue
                if not any(c.isdigit() for c in date_text):
                    continue

                a_tag = tds[-1].find("a")
                if not a_tag:
                    continue

                title = a_tag.get_text(strip=True)
                href  = a_tag.get("href", "")
                if href and not href.startswith("http"):
                    href = "https://kabutan.jp" + href

                results.append({
                    "date":     date_text,
                    "title":    title,
                    "url":      href,
                    "pub_date": date_text,
                })

                if len(results) >= max_items:
                    break

            if results:
                return results

        except requests.RequestException as e:
            print(f"    [警告] {code} のニュース取得失敗 ({url}): {e}")
        except Exception as e:
            print(f"    [警告] {code} のHTML解析失敗: {e}")

    return [{"date": "-", "title": "情報を取得できませんでした", "url": "", "pub_date": "-"}]


# ============================================================
# Yahoo Finance API
# ============================================================
_YF_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}

def _fetch_yahoo(symbol: str) -> list:
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=5d"
    res = requests.get(url, headers=_YF_HEADERS, timeout=15)
    res.raise_for_status()
    data = res.json()
    closes = data["chart"]["result"][0]["indicators"]["quote"][0]["close"]
    return [c for c in closes if c is not None]


def _fetch_yahoo_full(symbol: str, range_: str = "60d") -> tuple[list, list]:
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        f"?interval=1d&range={range_}"
    )
    res = requests.get(url, headers=_YF_HEADERS, timeout=15)
    res.raise_for_status()
    data  = res.json()
    quote = data["chart"]["result"][0]["indicators"]["quote"][0]
    pairs = [
        (c, v)
        for c, v in zip(quote.get("close", []), quote.get("volume", []))
        if c is not None and v is not None
    ]
    if not pairs:
        return [], []
    closes, volumes = zip(*pairs)
    return list(closes), list(volumes)


# ============================================================
# テクニカル指標
# ============================================================
def _calc_rsi(closes: list, period: int = 14) -> float | None:
    if len(closes) < period + 1:
        return None
    deltas   = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains    = [d if d > 0 else 0.0 for d in deltas]
    losses   = [-d if d < 0 else 0.0 for d in deltas]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 1)


def _calc_ma25_dev(closes: list) -> float | None:
    if len(closes) < 25:
        return None
    ma25 = sum(closes[-25:]) / 25
    return round((closes[-1] - ma25) / ma25 * 100, 1)


def _calc_vol_surge(volumes: list) -> float | None:
    if len(volumes) < 6:
        return None
    avg5 = sum(volumes[-6:-1]) / 5
    if avg5 == 0:
        return None
    return round(volumes[-1] / avg5, 2)


def _calc_ema(closes: list, period: int) -> list:
    if len(closes) < period:
        return []
    k = 2 / (period + 1)
    ema = [sum(closes[:period]) / period]
    for price in closes[period:]:
        ema.append(price * k + ema[-1] * (1 - k))
    return ema


def _calc_macd(closes: list) -> dict:
    if len(closes) < 35:
        return {}
    ema12 = _calc_ema(closes, 12)
    ema26 = _calc_ema(closes, 26)
    diff = len(ema12) - len(ema26)
    ema12 = ema12[diff:]
    macd_line = [e12 - e26 for e12, e26 in zip(ema12, ema26)]
    if len(macd_line) < 9:
        return {}
    signal_line = _calc_ema(macd_line, 9)
    diff2 = len(macd_line) - len(signal_line)
    macd_tail = macd_line[diff2:]
    if len(macd_tail) < 2 or len(signal_line) < 2:
        return {}
    prev_diff = macd_tail[-2] - signal_line[-2]
    curr_diff = macd_tail[-1] - signal_line[-1]
    golden = prev_diff < 0 and curr_diff >= 0
    dead   = prev_diff > 0 and curr_diff <= 0
    return {
        "macd":    round(macd_tail[-1], 4),
        "signal":  round(signal_line[-1], 4),
        "hist":    round(curr_diff, 4),
        "golden":  golden,
        "dead":    dead,
        "bullish": curr_diff > 0,
    }


def _calc_bollinger(closes: list, period: int = 25, sigma: float = 2.0) -> dict:
    if len(closes) < period:
        return {}
    window = closes[-period:]
    ma = sum(window) / period
    std = (sum((x - ma) ** 2 for x in window) / period) ** 0.5
    upper = ma + sigma * std
    lower = ma - sigma * std
    price = closes[-1]
    return {
        "upper": round(upper, 2),
        "lower": round(lower, 2),
        "ma":    round(ma, 2),
        "std":   round(std, 2),
        "above_upper": price >= upper,
        "below_lower": price <= lower,
        "pct_b": round((price - lower) / (upper - lower) * 100, 1) if upper != lower else 50.0,
    }


def _calc_golden_dead_cross(closes: list) -> dict:
    if len(closes) < 26:
        return {}
    ma5_prev  = sum(closes[-6:-1]) / 5
    ma25_prev = sum(closes[-26:-1]) / 25
    ma5_curr  = sum(closes[-5:]) / 5
    ma25_curr = sum(closes[-25:]) / 25
    golden = ma5_prev <= ma25_prev and ma5_curr > ma25_curr
    dead   = ma5_prev >= ma25_prev and ma5_curr < ma25_curr
    return {
        "ma5":    round(ma5_curr, 2),
        "ma25":   round(ma25_curr, 2),
        "golden": golden,
        "dead":   dead,
        "above":  ma5_curr > ma25_curr,
    }


def _calc_liquidity(closes: list, volumes: list) -> dict:
    if len(closes) < 5 or len(volumes) < 5:
        return {"avg_volume_yen": None, "judge": "unknown", "label": "データ不足"}
    daily_values = [c * v for c, v in zip(closes[-5:], volumes[-5:]) if c and v]
    if not daily_values:
        return {"avg_volume_yen": None, "judge": "unknown", "label": "データ不足"}
    avg = sum(daily_values) / len(daily_values)
    if avg >= 1_0000_0000:
        judge, label = "ok", f"{avg/1e8:.1f}億円/日"
    elif avg >= 3000_0000:
        judge, label = "warn", f"⚠️{avg/1e4:.0f}万円/日（流動性やや低）"
    else:
        judge, label = "low", f"❌{avg/1e4:.0f}万円/日（流動性不足）"
    return {"avg_volume_yen": round(avg), "judge": judge, "label": label}



# ============================================================
# 信用倍率取得（kabutan.jp）
# ============================================================
def get_margin_ratio(code: str):
    """kabutan.jpから信用倍率を取得する"""
    try:
        session = _get_kabutan_session()
        resp = session.get(
            f"https://kabutan.jp/stock/?code={code}",
            headers={"Referer": "https://kabutan.jp/"},
            timeout=15
        )
        import re as _re
        from bs4 import BeautifulSoup as _BS
        soup = _BS(resp.text, "html.parser")
        lines = [l.strip() for l in soup.get_text().split("\n")]
        for i, line in enumerate(lines):
            if line == "信用倍率":
                for j in range(i+1, min(i+15, len(lines))):
                    m = _re.search(r"([\d.]+)倍", lines[j])
                    if m:
                        return {"ratio": float(m.group(1))}
    except Exception as e:
        print(f"    [警告] {code} 信用倍率取得失敗: {e}")
    return None

def get_technical_signals(code: str) -> dict:
    try:
        closes, volumes = _fetch_yahoo_full(f"{code}.T", range_="90d")
        rsi       = _calc_rsi(closes)
        ma25_dev  = _calc_ma25_dev(closes)
        vol_surge = _calc_vol_surge(volumes)
        macd      = _calc_macd(closes)
        boll      = _calc_bollinger(closes)
        cross     = _calc_golden_dead_cross(closes)
        liquidity = _calc_liquidity(closes, volumes)

        signals = []
        buy_count = 0
        sell_count = 0

        if rsi is not None:
            if rsi <= 30:
                signals.append(f"[買] RSI売られ過ぎ({rsi:.1f})")
                buy_count += 1
            elif rsi >= 70:
                signals.append(f"[売] RSI買われ過ぎ({rsi:.1f})")
                sell_count += 1

        if ma25_dev is not None:
            if ma25_dev <= -5.0:
                signals.append(f"[買検討] 25MA下方乖離({ma25_dev:+.1f}%)")
                buy_count += 1
            elif ma25_dev >= 10.0:
                signals.append(f"[利確検討] 25MA上方乖離({ma25_dev:+.1f}%)")
                sell_count += 1

        if vol_surge is not None and vol_surge >= 2.0:
            if len(closes) >= 2 and closes[-1] > closes[-2]:
                signals.append(f"[強買] 出来高急増＋株価上昇({vol_surge:.1f}倍)")
                buy_count += 2
            else:
                signals.append(f"[注目] 出来高急増({vol_surge:.1f}倍)")
                buy_count += 1

        if macd:
            if macd.get("golden"):
                signals.append("[買] MACDゴールデンクロス")
                buy_count += 1
            elif macd.get("dead"):
                signals.append("[売] MACDデッドクロス")
                sell_count += 1
            elif macd.get("bullish"):
                signals.append("[↑] MACD上昇トレンド中")

        if boll:
            if boll.get("below_lower"):
                signals.append(f"[買] BB下限割れ(%-B:{boll['pct_b']:.0f}%)")
                buy_count += 1
            elif boll.get("above_upper"):
                signals.append(f"[売] BB上限突破(%-B:{boll['pct_b']:.0f}%)")
                sell_count += 1

        if cross:
            if cross.get("golden"):
                signals.append("[買] ゴールデンクロス(5MA>25MA)")
                buy_count += 1
            elif cross.get("dead"):
                signals.append("[売] デッドクロス(5MA<25MA)")
                sell_count += 1

        liq_judge = liquidity.get("judge", "unknown")

        if liq_judge == "low":
            summary = f"⚠️流動性不足（売買困難リスク）"
        elif buy_count >= 3:
            summary = f"★強買シグナル({buy_count}指標一致)"
        elif buy_count >= 2:
            summary = f"◎買いシグナル({buy_count}指標一致)"
        elif buy_count >= 1:
            summary = f"○買い候補({buy_count}指標)"
        elif sell_count >= 2:
            summary = f"▼売りシグナル({sell_count}指標一致)"
        elif sell_count >= 1:
            summary = f"△売り候補({sell_count}指標)"
        else:
            summary = "様子見"

        print(f"    テクニカル: RSI={rsi} MA25乖離={ma25_dev}% 出来高比={vol_surge}倍 "
              f"MACD={macd.get('hist')} BB%-B={boll.get('pct_b')} クロス={cross.get('above')} "
              f"流動性={liquidity.get('label')} → {summary}")
        return {
            "rsi":       rsi,
            "ma25_dev":  ma25_dev,
            "vol_surge": vol_surge,
            "macd":      macd,
            "bollinger": boll,
            "cross":     cross,
            "liquidity": liquidity,
            "signals":   signals,
            "summary":   summary,
            "buy_count": buy_count,
            "sell_count": sell_count,
        }
    except Exception as e:
        print(f"    [警告] {code} テクニカル取得失敗: {e}")
        return {
            "rsi": None, "ma25_dev": None, "vol_surge": None,
            "macd": {}, "bollinger": {}, "cross": {},
            "liquidity": {"avg_volume_yen": None, "judge": "unknown", "label": "取得失敗"},
            "signals": [], "summary": "取得失敗", "buy_count": 0, "sell_count": 0,
        }


# ============================================================
# 株価取得
# ============================================================
def get_stock_price(code: str) -> dict:
    try:
        closes = _fetch_yahoo(f"{code}.T")
        if not closes:
            print(f"    [警告] {code} の価格データが空です")
            return {"price": None}

        price      = closes[-1]
        prev_close = closes[-2] if len(closes) >= 2 else price
        change     = price - prev_close
        change_pct = (change / prev_close) * 100
        return {
            "price":      round(price, 0),
            "change":     round(change, 0),
            "change_pct": round(change_pct, 2),
        }
    except Exception as e:
        print(f"    [警告] {code} の株価取得失敗: {e}")
    return {"price": None}


# ============================================================
# WTI・コーポレートアクション
# ============================================================
def get_corporate_actions(stocks: list) -> dict:
    import os, requests
    api_key = os.environ.get("EDINETDB_API_KEY", "")
    if not api_key:
        return {}
    result = {}
    for stock in stocks:
        code = stock["code"]
        try:
            r = requests.get(
                f"https://edinetdb.jp/v1/search?q={code}",
                headers={"X-API-Key": api_key},
                timeout=10
            )
            if not r.ok:
                continue
            data = r.json().get("data", [])
            if not data:
                continue
            edinet_code = data[0].get("edinetCode") or data[0].get("edinet_code")
            if not edinet_code:
                continue
            r2 = requests.get(
                f"https://edinetdb.jp/v1/companies/{edinet_code}",
                headers={"X-API-Key": api_key},
                timeout=10
            )
            if not r2.ok:
                continue
            company = r2.json().get("data", {})
            actions = []
            latest = company.get("latestFinancials") or {}
            if latest.get("treasuryStockAcquisition"):
                actions.append("自社株買い実施中")
            health = company.get("healthScore")
            if health:
                actions.append(f"健全性スコア: {health}/100")
            result[code] = {
                "edinet_code": edinet_code,
                "actions": actions,
                "health_score": health,
            }
        except Exception:
            continue
    return result


def get_wti_price() -> dict:
    try:
        closes = _fetch_yahoo("CL%3DF")
        if not closes:
            return {"price": None, "error": "データが取得できませんでした"}

        price      = closes[-1]
        prev_close = closes[-2] if len(closes) >= 2 else price
        change     = price - prev_close
        change_pct = (change / prev_close) * 100
        return {
            "price":      round(price, 2),
            "change":     round(change, 2),
            "change_pct": round(change_pct, 2),
        }
    except Exception as e:
        print(f"    [警告] WTI 取得失敗: {e}")
    return {"price": None, "error": "取得できませんでした"}


# ============================================================
# 日経平均 + 東証33業種
# ============================================================
_TSE33_NAMES = {
    1: "水産・農林業",    2: "鉱業",            3: "建設業",
    4: "食料品",          5: "繊維業",          6: "パルプ・紙",
    7: "化学",            8: "医薬品",          9: "石油・石炭製品",
   10: "ゴム製品",       11: "ガラス・土石製品", 12: "鉄鋼",
   13: "非鉄金属",       14: "金属製品",        15: "機械",
   16: "電気機器",       17: "輸送用機器",      18: "精密機器",
   19: "その他製品",     20: "電力・ガス業",    21: "陸運業",
   22: "海運業",         23: "空運業",          24: "倉庫・運輸関連業",
   25: "情報・通信業",   26: "卸売業",          27: "小売業",
   28: "銀行業",         29: "証券業",          30: "保険業",
   31: "その他金融業",   32: "不動産業",        33: "サービス業",
}


def get_nikkei_data() -> dict:
    try:
        closes = _fetch_yahoo("%5EN225")
        if not closes:
            return {"price": None}
        price      = closes[-1]
        prev_close = closes[-2] if len(closes) >= 2 else price
        change     = price - prev_close
        change_pct = (change / prev_close) * 100
        return {
            "price":      round(price, 2),
            "change":     round(change, 2),
            "change_pct": round(change_pct, 2),
        }
    except Exception as e:
        print(f"    [警告] 日経平均取得失敗: {e}")
    return {"price": None}


def get_sector_trends() -> list:
    try:
        res = requests.get(f"https://kabutan.jp/?_={int(time.time())}", headers=HEADERS, timeout=15)
        res.raise_for_status()
        m = re.search(r'setIndustry\("datas=([^"]+)"', res.text)
        if not m:
            return []
        raw = m.group(1)
        pairs = [p for p in raw.split(",#") if "," in p]
        sectors = []
        for pair in pairs:
            sid_str, pct_str = pair.rstrip(",#").split(",", 1)
            try:
                sid = int(sid_str)
                pct = float(pct_str)
            except ValueError:
                continue
            name = _TSE33_NAMES.get(sid, f"業種{sid}")
            sectors.append({"id": sid, "name": name, "change_pct": pct})
        sectors.sort(key=lambda x: x["change_pct"], reverse=True)
        return sectors
    except Exception as e:
        print(f"    [警告] 東証33業種取得失敗: {e}")
    return []


# ============================================================
# 銘柄スクリーニング
# ============================================================
_SCREEN_URL = "https://kabutan.jp/warning/?mode=2_1&market=0&page={page}"
_COL_CODE   = 0
_COL_MARKET = 1
_COL_PRICE  = 4
_COL_VOLUME = 8
_COL_PER    = 9


def _get_op_profit(code: str) -> dict:
    url = f"https://kabutan.jp/stock/finance?code={code}"
    result = {
        "op_profit": None, "pbr": None, "mktcap_mn": None,
        "sales_growth": None, "net_cash_ratio": None, "net_cash_ratio_approx": False,
    }
    try:
        session = _get_kabutan_session()
        res = session.get(url, headers={"Referer": "https://kabutan.jp/"}, timeout=10)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        tables = soup.find_all("table")

        if len(tables) > 2:
            t2_rows = tables[2].find_all("tr")
            if len(t2_rows) >= 2:
                val_cells = [c.get_text(strip=True) for c in t2_rows[1].find_all("td")]
                result["pbr"] = _parse_ratio_float(val_cells[1]) if len(val_cells) > 1 else None
            if len(t2_rows) >= 3:
                mc_cells = [c.get_text(strip=True) for c in t2_rows[2].find_all(["th", "td"])]
                if len(mc_cells) >= 2:
                    result["mktcap_mn"] = _parse_mktcap_mn(mc_cells[1])

        _equity = None
        _debt_ratio = None
        _cash = None

        for table in tables:
            rows = table.find_all("tr")
            if not rows:
                continue
            header = [c.get_text(strip=True) for c in rows[0].find_all(["th", "td"])]
            header_text = " ".join(header)

            if result["op_profit"] is None and "営業益" in header:
                op_col = header.index("営業益")
                for row in reversed(rows[1:]):
                    cells = [c.get_text(strip=True) for c in row.find_all(["th", "td"])]
                    if not cells or not cells[0]:
                        continue
                    if cells[0].startswith("予") or "前期比" in cells[0]:
                        continue
                    if len(cells) > op_col:
                        val = cells[op_col].replace(",", "")
                        if val and val not in ("－", ""):
                            try:
                                result["op_profit"] = float(val)
                                break
                            except ValueError:
                                pass

            if _equity is None and "自己資本比率" in header_text and "有利子負債倍率" in header_text:
                _equity     = _extract_col_value(table, ["自己資本"], exact=True)
                _debt_ratio = _extract_col_value(table, ["有利子負債倍率"])

            if _cash is None and "営業CF" in header_text and "投資CF" in header_text:
                _cash = _extract_col_value(table, ["現金等残高"], exact=True)

            if result["sales_growth"] is None and "最終益" in header and "修正1株配" in header:
                sales_col = header.index("売上高") if "売上高" in header else None
                if sales_col is not None:
                    sales_actual: list[float] = []
                    for row in reversed(rows[1:]):
                        if len(sales_actual) >= 2:
                            break
                        cells = row.find_all(["th", "td"])
                        if not cells:
                            continue
                        label = cells[0].get_text(strip=True)
                        if label.startswith("予") or "前期比" in label:
                            continue
                        if len(cells) > sales_col:
                            s_raw = cells[sales_col].get_text(strip=True).replace(",", "").strip()
                            try:
                                sales_actual.append(float(s_raw))
                            except ValueError:
                                pass
                    if len(sales_actual) == 2 and sales_actual[1] != 0:
                        result["sales_growth"] = round(
                            (sales_actual[0] - sales_actual[1]) / sales_actual[1] * 100, 1
                        )

        mktcap = result["mktcap_mn"]
        _current_assets2    = _extract_bs_value(tables, "流動資産", "流動資産合計")
        _invest_sec2        = _extract_bs_value(tables, "投資有価証券")
        _total_liabilities2 = _extract_bs_value(tables, "負債合計")
        if _current_assets2 is not None and _total_liabilities2 is not None and mktcap is not None and mktcap > 0:
            if _invest_sec2 is not None:
                _net_cash2 = _current_assets2 + _invest_sec2 * 0.7 - _total_liabilities2
            else:
                _net_cash2 = _current_assets2 - _total_liabilities2
                result["net_cash_ratio_approx"] = True
            result["net_cash_ratio"] = round(_net_cash2 / mktcap, 2)
        elif _cash is not None and mktcap is not None and mktcap > 0:
            interest_debt_val = (_equity or 0) * (_debt_ratio or 0)
            result["net_cash_ratio"] = round((_cash - interest_debt_val) / mktcap, 2)
            result["net_cash_ratio_approx"] = True

    except Exception as e:
        print(f"    [警告] {code} 営業利益取得失敗: {e}")
    return result


def get_screened_stocks(max_pages: int = 8, max_results: int = 10) -> list:
    results = []

    for page in range(1, max_pages + 1):
        url = _SCREEN_URL.format(page=page)
        try:
            res = requests.get(url, headers=HEADERS, timeout=15)
            res.raise_for_status()
        except Exception as e:
            print(f"    [警告] スクリーニング page={page} 取得失敗: {e}")
            break

        soup = BeautifulSoup(res.text, "html.parser")
        page_has_data = False

        for row in soup.select("table tr"):
            tds = row.find_all("td")
            if len(tds) < 12:
                continue
            raw = [td.get_text(strip=True) for td in tds]

            if not re.match(r'^\d{4}[A-Z]?$', raw[_COL_CODE]):
                continue
            page_has_data = True

            th = row.find("th")
            name = th.get_text(strip=True) if th else raw[_COL_CODE]

            market = raw[_COL_MARKET]
            if "東Ｇ" not in market and "東Ｓ" not in market:
                continue

            try:
                price = float(raw[_COL_PRICE].replace(",", ""))
            except ValueError:
                continue
            if price > 1000:
                continue

            per_raw = raw[_COL_PER].replace(",", "")
            if per_raw in ("－", "", "N/A"):
                continue
            try:
                per = float(per_raw)
            except ValueError:
                continue
            if per <= 0 or per > 15:
                continue

            try:
                volume = int(raw[_COL_VOLUME].replace(",", ""))
            except ValueError:
                volume = 0

            code          = raw[_COL_CODE]
            fin_info      = _get_op_profit(code)
            op_profit       = fin_info["op_profit"]
            pbr_screen      = fin_info["pbr"]
            mktcap_mn       = fin_info["mktcap_mn"]
            sales_growth    = fin_info["sales_growth"]
            net_cash_ratio       = fin_info["net_cash_ratio"]
            net_cash_ratio_approx = fin_info.get("net_cash_ratio_approx", False)
            if op_profit is not None and op_profit <= 0:
                print(f"    [{code}] {name} 営業利益マイナス({op_profit:,.0f}百万円) → 除外")
                continue

            op_label = (
                f"{op_profit:+,.0f}百万円" if op_profit is not None else "確認不可"
            )
            graham_screen    = round(per * pbr_screen, 1) if pbr_screen is not None else None
            peg_screen       = (round(per / sales_growth, 2)
                                if sales_growth is not None and sales_growth > 0 else None)
            ev_ebitda_screen = (round(mktcap_mn / op_profit, 1)
                                if mktcap_mn is not None and op_profit is not None and op_profit > 0
                                else None)

            results.append({
                "code":           code,
                "name":           name,
                "market":         market,
                "price":          price,
                "per":            per,
                "pbr":            pbr_screen,
                "graham":         graham_screen,
                "peg":            peg_screen,
                "ev_ebitda":      ev_ebitda_screen,
                "net_cash_ratio":        net_cash_ratio,
                "net_cash_ratio_approx": net_cash_ratio_approx,
                "volume":         volume,
                "op_profit":      op_label,
                "stop_loss":      round(price * 0.92, 1),
                "take_profit":    round(price * 1.25, 1),
            })

            if len(results) >= max_results:
                return results

        if not page_has_data:
            break

    return results


# ============================================================
# スコア計算
# ============================================================
def calc_integrated_score(stock_item, wti, world_news, edinet_entry=None):
    score = 0
    breakdown = {}

    sig = stock_item.get("signals", {})
    buy_count  = sig.get("buy_count", 0)
    sell_count = sig.get("sell_count", 0)
    tech_score = max(0, min(30, buy_count * 6 - sell_count * 4))
    score += tech_score
    breakdown["テクニカル"] = (tech_score, 30, f"買{buy_count}指標 売{sell_count}指標")

    fin = stock_item.get("buffett", {})
    hs  = fin.get("health_score", 0)
    fin_score = round(hs / 100 * 25)
    score += fin_score
    breakdown["財務"] = (fin_score, 25, f"ヘルススコア{hs}/100  ROE:{fin.get('roe')}%  自己資本:{fin.get('equity_ratio')}%")

    mom_score = 0
    mom_notes = []
    if edinet_entry:
        fin_e = edinet_entry.get("financials") or {}
        q     = fin_e.get("quarter") or ""
        op    = fin_e.get("op_income")
        op_fc = fin_e.get("op_fc")
        if op and op_fc and op_fc > 0:
            prog = op / op_fc * 100
            pace = {"Q1": 25, "Q2": 50, "Q3": 75}.get(q, 100)
            if prog >= pace + 5:
                mom_score += 12
                mom_notes.append(f"進捗{prog:.0f}%✓")
            elif prog >= pace - 5:
                mom_score += 7
                mom_notes.append(f"進捗{prog:.0f}%")
            else:
                mom_notes.append(f"進捗{prog:.0f}%⚠")
    sg = fin.get("sales_growth")
    if sg is not None:
        if sg >= 10:
            mom_score += 8
            mom_notes.append(f"売上+{sg:.1f}%✓")
        elif sg >= 0:
            mom_score += 4
            mom_notes.append(f"売上+{sg:.1f}%")
        else:
            mom_notes.append(f"売上{sg:.1f}%⚠")
    mom_score = min(20, mom_score)
    score += mom_score
    breakdown["モメンタム"] = (mom_score, 20, " ".join(mom_notes) if mom_notes else "データなし")

    wti_price = wti.get("price") or 0
    if wti_price < 90:
        wti_score, wti_label = 15, "シナリオA（エントリー可）"
    elif wti_price <= 100:
        wti_score, wti_label = 8,  "シナリオB（凍結中）"
    else:
        wti_score, wti_label = 0,  "シナリオC（撤退検討）"
    score += wti_score
    breakdown["WTIシナリオ"] = (wti_score, 15, f"${wti_price:.1f}  {wti_label}")

    if world_news:
        avg_risk = sum(n.get("score", 0) for n in world_news) / len(world_news)
        if avg_risk <= 3:
            geo_score, geo_label = 10, "地政学リスク低"
        elif avg_risk <= 6:
            geo_score, geo_label = 6,  "地政学リスク中"
        else:
            geo_score, geo_label = 2,  "地政学リスク高"
    else:
        geo_score, geo_label = 5, "情報なし"
    score += geo_score
    breakdown["世界情勢"] = (geo_score, 10, geo_label)

    liquidity = sig.get("liquidity") or {}
    liq_judge = liquidity.get("judge", "unknown")
    liq_label = liquidity.get("label", "--")
    if liq_judge == "low":
        liq_penalty = -10
        liq_note = f"❌流動性不足 {liq_label}（-10点）"
    elif liq_judge == "warn":
        liq_penalty = -5
        liq_note = f"⚠️流動性やや低 {liq_label}（-5点）"
    else:
        liq_penalty = 0
        liq_note = f"流動性OK {liq_label}"
    score += liq_penalty
    score = max(0, score)
    breakdown["流動性"] = (liq_penalty, 0, liq_note)

    if liq_judge == "low" and score < 50:
        rating = "⚠️  流動性不足（売買困難リスク）"
    elif score >= 70:
        rating = "★★★ 強い買いシグナル"
    elif score >= 50:
        rating = "★★  買い候補"
    elif score >= 30:
        rating = "★   様子見"
    else:
        rating = "    見送り"

    return {"score": score, "rating": rating, "breakdown": breakdown}


def calc_selection_score(stock_item: dict) -> dict:
    fin = stock_item.get("buffett", {})
    sig = stock_item.get("signals", {})

    hs = fin.get("health_score", 0)
    fin_score = round(hs / 100 * 25)

    roe = fin.get("roe") or 0
    if roe >= 15:   roe_score = 10
    elif roe >= 10: roe_score = 6
    else:           roe_score = 2

    liq = sig.get("liquidity") or {}
    liq_judge = liq.get("judge", "unknown")
    if liq_judge == "ok":     liq_score = 10
    elif liq_judge == "warn": liq_score = 5
    else:                     liq_score = 0

    buf_score = 5 if stock_item.get("buffett_passed") else 0

    total = fin_score + roe_score + liq_score + buf_score

    if total >= 40:   grade = "S"
    elif total >= 30: grade = "A"
    elif total >= 20: grade = "B"
    else:             grade = "C"

    return {
        "score": total, "grade": grade,
        "fin_score": fin_score, "roe_score": roe_score,
        "liq_score": liq_score, "buf_score": buf_score,
    }


def calc_entry_signal(stock_item: dict, wti: dict) -> dict:
    sig = stock_item.get("signals", {})
    buy_count  = sig.get("buy_count", 0)
    sell_count = sig.get("sell_count", 0)
    tech_score = max(0, min(30, buy_count * 6 - sell_count * 4))

    wti_price = wti.get("price") or 0
    if wti_price < 90:     wti_score, wti_label = 20, "シナリオA✅"
    elif wti_price <= 100: wti_score, wti_label = 10, "シナリオB🟡"
    else:                  wti_score, wti_label = 0,  "シナリオC🔴"

    total = tech_score + wti_score
    summary = sig.get("summary", "様子見")

    if wti_score == 0:
        entry_judge = "🔴 エントリー停止"
    elif total >= 35:
        entry_judge = "🟢 エントリー推奨"
    elif total >= 20:
        entry_judge = "🟡 条件付きエントリー検討"
    else:
        entry_judge = "⚪ 様子見"

    return {
        "score": total, "tech_score": tech_score,
        "wti_score": wti_score, "wti_label": wti_label,
        "entry_judge": entry_judge, "summary": summary,
        "buy_count": buy_count, "sell_count": sell_count,
    }


# ============================================================
# データ査読
# ============================================================
def run_data_review(stock_data: list) -> list:
    alerts = []
    for item in stock_data:
        stock = item["stock"]
        price = item.get("price", {})
        fin   = item.get("buffett", {})
        sig   = item.get("signals", {})

        cur = price.get("price")
        if cur is None:
            alerts.append(f"{stock['name']}（{stock['code']}）: 株価取得失敗")
        if fin.get("roe") is None and fin.get("equity_ratio") is None:
            alerts.append(f"{stock['name']}（{stock['code']}）: 財務データ取得失敗")
        if sig.get("rsi") is None:
            alerts.append(f"{stock['name']}（{stock['code']}）: テクニカルデータ取得失敗")
    return alerts


# ============================================================
# メール本文の組み立て
# ============================================================
def build_email_body(
    stock_data: list,
    wti: dict,
    world_news: list,
    edinet_data: list,
    screened: list,
    nikkei: dict | None = None,
    sectors: list | None = None,
    reuters_news: list | None = None,
    reuters_source: str = "",
    nhk_risk_news: list | None = None,
    buffett_analysis: dict | None = None,
    corp_actions: dict | None = None,
) -> str:
    today = date.today().strftime("%Y年%m月%d日")

    wti_price = wti.get("price") or 0
    if wti_price < 90:
        wti_scenario = "🟢 シナリオA：エントリー可"
        wti_action   = "確認翌営業日から順次エントリー開始"
        wti_emoji    = "🟢"
    elif wti_price <= 100:
        wti_scenario = "🟡 シナリオB：凍結中"
        wti_action   = "新規エントリー凍結・Compression形成待ち"
        wti_emoji    = "🟡"
    else:
        wti_scenario = "🔴 シナリオC：エントリー停止"
        wti_action   = "WTI $100以下でB復帰 / $90以下でA移行"
        wti_emoji    = "🔴"

    wti_chg  = wti.get("change", 0) or 0
    wti_pct  = wti.get("change_pct", 0) or 0
    wti_sign = "+" if wti_chg >= 0 else ""

    lines = [
        f"■ IR通知（短期投資向け） ／ {today}",
        "=" * 52,
        "",
    ]

    # 【査読アラート】
    review_alerts = run_data_review(stock_data)
    if review_alerts:
        lines.append("【査読アラート】⚠️ データ異常を検知しました")
        lines.append("-" * 52)
        for alert in review_alerts:
            lines.append(f"  {alert}")
        lines.append("")
        lines.append("  ※ 上記銘柄のデータを手動で確認してください")
        lines.append("=" * 52)
        lines.append("")
    else:
        lines.append("【査読】✅ 全銘柄データ正常")
        lines.append("=" * 52)
        lines.append("")

    # ============================================================
    # 【0】保有ポジション・アラート
    # ============================================================
    POSITIONS = [
        {"code": "3150", "name": "グリムス", "shares": 100, "entry": 2428.0, "stop": 2234.0, "t1": 3035.0, "t1_shares": 100, "t1_profit": 60700, "t2": 3642.0, "t2_shares": 100, "t2_profit": 121400},
    ]  # 2026-06-17 グリムス100株@2428

    pos_lines = []
    pos_lines.append("━" * 52)
    pos_lines.append(f"📦 保有ポジション確認（{today}）")
    pos_lines.append("━" * 52)
    pos_lines.append("")

    for p in POSITIONS:
        price_data = get_stock_price(p["code"])
        cur = price_data.get("price") if price_data else None
        if cur and cur > 0:
            chg_pct  = (cur - p["entry"]) / p["entry"] * 100
            upnl     = round((cur - p["entry"]) * p["shares"])
            sign     = "+" if upnl >= 0 else "▲"
            chg_s    = f"{chg_pct:+.1f}%"
            upnl_s   = f"{sign}¥{abs(upnl):,}"

            dist_stop = (cur - p["stop"]) / p["stop"] * 100
            if dist_stop <= 3:
                stop_flag = "🔴"
            elif dist_stop <= 5:
                stop_flag = "🟡"
            else:
                stop_flag = "🟢"

            pos_lines.append(f"【{p['code']} {p['name']}】{p['shares']}株")
            pos_lines.append(f"  取得単価：¥{p['entry']:,.1f} → 現在：¥{cur:,.0f}（{chg_s}）")
            pos_lines.append(f"  含み損益：{upnl_s}")
            pos_lines.append(f"  損切まで：▲{dist_stop:.1f}%（¥{p['stop']:,.1f}）{stop_flag}")

            if p["t1"]:
                dist_t1 = (p["t1"] - cur) / cur * 100
                if dist_t1 <= 0:
                    pos_lines.append(f"  🎉 T1到達！¥{p['t1']:,.0f} → {p['t1_shares']}株売却 +¥{p['t1_profit']:,}確定")
                elif dist_t1 <= 3:
                    pos_lines.append(f"  🎯 T1接近！あと+{dist_t1:.1f}%（¥{p['t1']:,.0f}）→ {p['t1_shares']}株売却準備 +¥{p['t1_profit']:,}")
                else:
                    pos_lines.append(f"  T1まで：あと+{dist_t1:.1f}%（¥{p['t1']:,.0f}）→ {p['t1_shares']}株売却 +¥{p['t1_profit']:,}")

            dist_t2 = (p["t2"] - cur) / cur * 100
            if dist_t2 <= 0:
                pos_lines.append(f"  🎉 T2到達！¥{p['t2']:,.0f} → {p['t2_shares']}株売却 +¥{p['t2_profit']:,}確定")
            elif dist_t2 <= 3:
                pos_lines.append(f"  🎯 T2接近！あと+{dist_t2:.1f}%（¥{p['t2']:,.0f}）→ {p['t2_shares']}株売却準備 +¥{p['t2_profit']:,}")
            else:
                pos_lines.append(f"  T2まで：あと+{dist_t2:.1f}%（¥{p['t2']:,.0f}）→ {p['t2_shares']}株売却 +¥{p['t2_profit']:,}")

            alerts = []
            if dist_stop <= 3:
                alerts.append(f"  🚨 損切接近！¥{cur:,.0f} → 即時売却判断（損切ライン¥{p['stop']:,.1f}）")
            elif dist_stop <= 5:
                alerts.append(f"  ⚠️ 損切ライン注意（あと▲{dist_stop:.1f}%で損切）")
            if p["t1"] and 0 < (p["t1"] - cur) / cur * 100 <= 3:
                alerts.append(f"  🎯 T1利確ライン接近！¥{p['t1']:,.0f}まであと+{(p['t1']-cur)/cur*100:.1f}%")
            if 0 < dist_t2 <= 3:
                alerts.append(f"  🎯 T2利確ライン接近！¥{p['t2']:,.0f}まであと+{dist_t2:.1f}%")
            for al in alerts:
                pos_lines.append(al)
        else:
            pos_lines.append(f"【{p['code']} {p['name']}】{p['shares']}株")
            pos_lines.append(f"  取得単価：¥{p['entry']:,.1f} → 現在：取得失敗")

        pos_lines.append("")

    pos_lines.append("━" * 52)
    pos_lines.append("")
    lines.extend(pos_lines)

    # 【1】今日の結論
    lines.append("【1】今日の結論")
    lines.append("=" * 52)
    lines.append("")
    lines.append(f"  WTI: ${wti_price:.2f}  ({wti_sign}{wti_chg:.2f} / {wti_sign}{wti_pct:.2f}%)")
    lines.append(f"  {wti_scenario}")
    lines.append(f"  → {wti_action}")
    lines.append("")

    if nikkei and nikkei.get("price"):
        n_chg  = nikkei.get("change", 0) or 0
        n_pct  = nikkei.get("change_pct", 0) or 0
        n_sign = "+" if n_chg >= 0 else ""
        lines.append(f"  日経平均: {nikkei['price']:,.0f}円  ({n_sign}{n_chg:+.0f} / {n_sign}{n_pct:+.2f}%)")
        lines.append("")

    if sectors:
        up   = sorted([s for s in sectors if s.get("change_pct",0)>0], key=lambda x:-x["change_pct"])[:3]
        down = sorted([s for s in sectors if s.get("change_pct",0)<0], key=lambda x: x["change_pct"])[:3]
        if up:
            lines.append("  上昇業種: " + " / ".join(f"{s['name']}{s['change_pct']:+.1f}%" for s in up))
        if down:
            lines.append("  下落業種: " + " / ".join(f"{s['name']}{s['change_pct']:+.1f}%" for s in down))
        lines.append("")

    lines.append("  📊 注目銘柄トップ3")
    ranked = sorted(stock_data, key=lambda x: -(x.get("score") or 0))
    for i, d in enumerate(ranked[:3], 1):
        st    = d["stock"]
        pr    = d.get("price", {})
        bf    = d.get("buffett", {})
        tags  = d.get("signals", {}).get("signals", [])
        price_str = f"¥{pr['price']:,.0f}" if pr.get("price") else "--"
        peg_v = bf.get("peg")
        ni_v  = bf.get("ni_forecast_yoy")
        parts = []
        if peg_v: parts.append(f"PEG{peg_v:.2f}{'✓' if peg_v<=1 else ''}")
        if ni_v is not None: parts.append(f"来期{ni_v:+.1f}%")
        if tags: parts.append(tags[0])
        lines.append(f"  {i}位 {st['code']} {st['name']}  {price_str}")
        if parts: lines.append(f"       {' / '.join(parts)}")
    lines.append("")
    lines.append("=" * 52)
    lines.append("")

    # 【2】銘柄詳細カード
    lines.append("【2】銘柄詳細")
    lines.append("=" * 52)
    lines.append("")

    BUDGET = 1_000_000
    for item in stock_data:
        stock  = item["stock"]
        price  = item.get("price", {})
        sig    = item.get("signals", {})
        f      = item.get("buffett", {})
        passed = item.get("buffett_passed", False)
        news   = item.get("news", [])

        cur_price = price.get("price")
        if cur_price:
            chg  = price.get("change", 0) or 0
            pct  = price.get("change_pct", 0) or 0
            sign = "+" if chg >= 0 else ""
            price_str = f"¥{cur_price:,.0f}  ({sign}{chg:+.0f} / {sign}{pct:+.2f}%)"
        else:
            price_str = "取得できませんでした"

        hs   = (item.get("buffett") or {}).get("health_score", 0) or 0
        sel  = item.get("score", 0) or 0
        rank = "S" if sel>=45 else "A" if sel>=35 else "B" if sel>=25 else "C"

        if wti_price > 100:  entry_judge = "🔴 エントリー停止"
        elif wti_price > 90: entry_judge = "🟡 様子見"
        else:                entry_judge = "🟢 エントリー検討"

        rsi      = sig.get("rsi")
        ma25_dev = sig.get("ma25_dev")
        vol      = sig.get("vol_surge")
        macd_v   = (sig.get("macd") or {}).get("hist")
        pct_b    = (sig.get("bollinger") or {}).get("pct_b")
        cross    = sig.get("cross") or {}
        tags     = sig.get("signals", [])
        liq      = (sig.get("liquidity") or {}).get("label", "--")

        rsi_s  = f"RSI:{rsi:.1f}"         if rsi      is not None else "RSI:--"
        ma_s   = f"25MA:{ma25_dev:+.1f}%" if ma25_dev is not None else "25MA:--"
        vol_s  = f"出来高:{vol:.1f}倍"     if vol      is not None else "出来高:--"
        macd_s = f"MACD:{macd_v:+.3f}"    if macd_v   is not None else "MACD:--"
        bb_s   = f"BB:{pct_b:.0f}%"       if pct_b    is not None else "BB:--"
        cr_s   = ("5MA>25MA" if cross.get("above") else "5MA<25MA") if cross.get("ma5") is not None else "--"
        sig_str = " / ".join(tags) if tags else "シグナルなし"

        lines.append(f"▼ {stock['name']}（{stock['code']}）  [{rank}ランク]  {wti_emoji}")
        lines.append(f"  {price_str}")
        lines.append(f"  {entry_judge}")
        lines.append(f"  {rsi_s}  {ma_s}  {vol_s}")
        lines.append(f"  {macd_s}  {bb_s}  {cr_s}")
        lines.append(f"  流動性:{liq}")
        lines.append(f"  → {sig_str}")

        margin = item.get("margin")
        if margin and margin.get("ratio") is not None:
            ratio = margin["ratio"]
            if ratio >= 15:
                lines.append(f"  📊 信用倍率: {ratio}倍 ⚠️ 高水準（返済売り圧力に注意）")
                lines.append("     ※信用倍率10倍超は将来の売り圧力が強まるリスクあり")
            elif ratio >= 10:
                lines.append(f"  📊 信用倍率: {ratio}倍 ⚠️ 高水準（返済売り圧力に注意）")
                lines.append("     ※信用倍率10倍超は将来の売り圧力が強まるリスクあり")
            elif ratio >= 5:
                lines.append(f"  📊 信用倍率: {ratio}倍 △ やや高め")
            elif ratio <= 1:
                lines.append(f"  📊 信用倍率: {ratio}倍 🟢 低水準（踏み上げ期待）")
            else:
                lines.append(f"  📊 信用倍率: {ratio}倍")

        next_e = stock.get("next_earnings")
        e_note = stock.get("earnings_note", "")
        if next_e:
            from datetime import date as _date
            ed = _date.fromisoformat(next_e)
            days_left = (ed - _date.today()).days
            if days_left >= 0:
                lines.append(f"  📅 次回決算: {next_e}（あと{days_left}日）{(' ★'+e_note) if e_note else ''}")

        peg_v = f.get("peg")
        if passed or (peg_v and peg_v <= 1):
            roe_s  = f"{f['roe']}%"          if f.get("roe")          else "--"
            eq_s   = f"{f['equity_ratio']}%" if f.get("equity_ratio") else "--"
            roic_v = f.get("roic")
            roic_s = f"{roic_v}%" if roic_v else "--"
            om_v   = f.get("op_margin")
            om_s   = f"{om_v}%" if om_v else "--"
            peg_s  = f"{peg_v:.2f}{'✓' if peg_v<=1 else ''}" if peg_v else "--"
            ni_yoy = f.get("ni_forecast_yoy")
            yoy_s  = f"{ni_yoy:+.1f}%" if ni_yoy is not None else "--"
            lines.append(f"  ROE:{roe_s}  自己資本:{eq_s}  ROIC:{roic_s}  営業利益率:{om_s}")
            lines.append(f"  PEG:{peg_s}  来期純利益予想:{yoy_s}")

            per_v = f.get("per")
            if per_v and cur_price:
                eps_est  = cur_price / per_v
                ni_yoy2  = f.get("ni_forecast_yoy")
                eps_next = eps_est * (1 + ni_yoy2/100) if ni_yoy2 else eps_est
                t_low    = round(eps_next * 15)
                t_mid    = round(eps_next * per_v)
                t_high   = round(eps_next * min(per_v * 1.2, 25))
                stop     = round(cur_price * 0.92)
                lines.append(f"  📈 目標株価: PER15倍→¥{t_low:,} / 現在PER→¥{t_mid:,} / 拡張→¥{t_high:,}")
                lines.append(f"     損切ライン(-8%)→¥{stop:,}")

            if buffett_analysis and stock["code"] in buffett_analysis:
                ba = buffett_analysis[stock["code"]]
                lines.append(f"  🧓 {ba.get('verdict','--')}: {ba.get('comment','')}")

            if corp_actions and stock["code"] in corp_actions:
                for act in (corp_actions[stock["code"]].get("actions") or []):
                    lines.append(f"  🏢 {act}")

        if cur_price and cur_price > 0:
            shares1 = max(1, int(BUDGET*0.4/(cur_price*100)))*100
            cost1   = shares1 * cur_price
            stop1   = round(cur_price * 0.92)
            price2  = round(cur_price * 1.05)
            shares2 = max(1, int(BUDGET*0.4/(price2*100)))*100
            cost2   = shares2 * price2
            target  = round(cur_price * 1.25)
            lines.append(f"  【エントリー計画】")
            lines.append(f"  第1段階: {shares1}株 x ¥{cur_price:,.0f} = ¥{cost1:,.0f}（逆指値¥{stop1:,}）")
            lines.append(f"  第2段階: {shares2}株 x ¥{price2:,} = ¥{cost2:,.0f}（含み益+5%or決算後）")
            lines.append(f"  利確目標: ¥{target:,}（+25%）")

        if news:
            n  = news[0]
            dt = n.get("pub_date", n.get("date", "-"))[:10] if (n.get("pub_date") or n.get("date")) else "-"
            lines.append(f"  📰 {dt} {n['title']}")
            if n.get("url"):
                lines.append(f"     {n['url']}")

        lines.append("")

    lines.append("=" * 52)
    lines.append("")

    # 【3】マクロ・ニュース
    lines.append("【3】マクロ・ニュース")
    lines.append("=" * 52)
    lines.append("")

    lines.append("▼ テーマ環境指標")
    lines.append("")
    for ti in THEME_INDICATORS:
        try:
            import yfinance as yf
            code   = ti["code"]
            suffix = ".T" if not code.endswith(".T") else ""
            tk     = yf.Ticker(f"{code}{suffix}")
            hist   = tk.history(period="5d")
            if len(hist) >= 2:
                cur  = hist["Close"].iloc[-1]
                prev = hist["Close"].iloc[-2]
                chg  = cur - prev
                cp   = chg / prev * 100
                sign = "+" if chg >= 0 else ""
                arr  = "↑" if chg >= 0 else "↓"
                lines.append(f"  {arr} {ti['name']}（{ti['code']}）  ¥{cur:,.0f}  {sign}{cp:.1f}%  [{ti['theme']}]")
            else:
                lines.append(f"  {ti['name']}（{ti['code']}）  取得失敗")
        except Exception:
            lines.append(f"  {ti['name']}（{ti['code']}）  エラー")
    lines.append("")
    lines.append("=" * 52)
    lines.append("")

    section_label = f"▼ 世界情勢ニュース（{reuters_source}）" if reuters_source else "▼ 世界情勢ニュース"
    lines.append(section_label)
    if reuters_news:
        try:
            import anthropic as _ac
            _client = _ac.Anthropic()
            _titles = "\n".join([
                f"{i+1}. {n['title']} ({n['pub_date'][:10] if n['pub_date'] else '-'})"
                for i, n in enumerate(reuters_news)
            ])
            _prompt = (
                "以下のニュース見出しから、日本株の株価に影響しそうなものだけを選び、"
                "各記事を1〜2行の日本語で要約してください。\n"
                "関係ないものは除外してください。\n"
                "形式: [番号] 要約文\n\n" + _titles
            )
            _resp = _client.messages.create(
                model="claude-haiku-4-5",
                max_tokens=800,
                messages=[{"role": "user", "content": _prompt}]
            )
            _summary       = _resp.content[0].text.strip()
            _summary_lines = [l for l in _summary.split("\n") if l.strip()]
            import re as _re
            for _sl in _summary_lines:
                _m = _re.match(r"\[(\d+)\]", _sl)
                if _m:
                    _idx = int(_m.group(1)) - 1
                    if 0 <= _idx < len(reuters_news):
                        _n = reuters_news[_idx]
                        lines.append(f"  {_sl}")
                        if _n["url"]:
                            lines.append(f"  {_n['url']}")
                        lines.append("")
                else:
                    lines.append(f"  {_sl}")
        except Exception:
            for n in reuters_news[:3]:
                lines.append(f"  {n['pub_date'][:16] if n['pub_date'] else '-'}  {n['title']}")
                if n["url"]:
                    lines.append(f"  {n['url']}")
                lines.append("")
    else:
        lines.append("  ニュースを取得できませんでした")
    lines.append("")
    lines.append("=" * 52)
    lines.append("")

    aj_risk  = [n for n in (world_news or [])
                if any(kw in (n.get("title","")+" "+n.get("url","")).lower()
                       for kw in _TARIFF_KEYWORDS_EN)][:3]
    nhk_risk = (nhk_risk_news or [])[:3]
    if aj_risk or nhk_risk:
        lines.append("⚠️ 地政学リスクアラート")
        lines.append("=" * 52)
        for n in nhk_risk:
            dt = n["pub_date"][:16] if n.get("pub_date") else "-"
            lines.append(f"  🇯🇵 {dt}  {n['title']}")
            if n.get("url"): lines.append(f"  {n['url']}")
            lines.append("")
        for n in aj_risk:
            dt = n["pub_date"][:16] if n.get("pub_date") else "-"
            lines.append(f"  🌐 {dt}  {n['title']}")
            if n.get("url"): lines.append(f"  {n['url']}")
            lines.append("")
        lines.append("=" * 52)
        lines.append("")

    if world_news:
        has_impact = "impact" in world_news[0]
        high_news  = [n for n in world_news if n.get("impact")=="high"][:3] if has_impact else world_news[:3]
        if high_news:
            lines.append("▼ Al Jazeera x Claude分析")
            for n in high_news:
                dt = n["pub_date"][:16] if n.get("pub_date") else "-"
                lines.append(f"  {dt}  {n['title']}")
                if n.get("summary"): lines.append(f"  → {n['summary']}")
                if n.get("url"):     lines.append(f"  {n['url']}")
                lines.append("")
            lines.append("=" * 52)
            lines.append("")

    lines.append("▼ 急騰事後分析（翌朝5分チェック）")
    lines.append("  ※ 前日に急騰した銘柄があれば以下を確認")
    lines.append("")
    lines.append("  □ Q3進捗率は高かったか（70%以上）")
    lines.append("  □ 保守的決算パターンがあったか")
    lines.append("  □ ヘルスコアは70以上だったか")
    lines.append("  □ テーマ（半導体・防衛・DC等）に入っていたか")
    lines.append("  □ Compression形成中だったか")
    lines.append("")
    lines.append("  重複件数が多いほど次回の事前スコアに活用できます。")
    lines.append("")
    lines.append("─" * 52)
    lines.append("このメールは自動送信されています。")
    return "\n".join(lines)


def _build_html(body: str) -> str:
    def esc(s: str) -> str:
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    html_lines = []
    for line in body.split("\n"):
        raw = line.rstrip()
        e   = esc(raw)

        if set(raw.strip()) <= {"=", "─", "-"} and len(raw.strip()) >= 10:
            html_lines.append('<hr style="border:none;border-top:1px solid #444;margin:12px 0;">')
            continue

        if raw.startswith("▼"):
            html_lines.append(
                f'<div style="font-size:15px;font-weight:bold;color:#4fc3f7;'
                f'margin:16px 0 6px;">{e}</div>'
            )
            continue

        if "★★★" in raw:
            html_lines.append(
                f'<div style="font-size:14px;color:#ff5252;font-weight:bold;padding:2px 0;">{e}</div>'
            )
            continue
        if "★★" in raw and "★★★" not in raw:
            html_lines.append(
                f'<div style="font-size:14px;color:#ffab40;font-weight:bold;padding:2px 0;">{e}</div>'
            )
            continue
        if raw.strip().startswith("★") and "★★" not in raw:
            html_lines.append(
                f'<div style="font-size:13px;color:#aaa;padding:2px 0;">{e}</div>'
            )
            continue

        if "シナリオA" in raw and "🟢" in raw:
            html_lines.append(
                f'<div style="font-size:14px;color:#66bb6a;font-weight:bold;padding:4px 0;">{e}</div>'
            )
            continue
        if "シナリオB" in raw and "🟡" in raw:
            html_lines.append(
                f'<div style="font-size:14px;color:#ffd54f;font-weight:bold;padding:4px 0;">{e}</div>'
            )
            continue
        if "シナリオC" in raw and "🔴" in raw:
            html_lines.append(
                f'<div style="font-size:14px;color:#ef5350;font-weight:bold;padding:4px 0;">{e}</div>'
            )
            continue

        if ("✔" in raw or "✘" in raw) and ("（" in raw):
            color = "#66bb6a" if "✔" in raw else "#ef5350"
            html_lines.append(
                f'<div style="font-size:14px;font-weight:bold;color:{color};margin-top:10px;">{e}</div>'
            )
            continue

        if not raw.strip():
            html_lines.append('<div style="height:6px;"></div>')
            continue

        if raw.strip().startswith("http"):
            url = raw.strip()
            if html_lines and 'word-break:break-all' in html_lines[-1]:
                prev = html_lines.pop()
                prev = prev.replace('</div>',
                    f' <a href="{url}" style="display:inline;margin-left:6px;'
                    f'padding:2px 8px;background:#2a2a2a;border:1px solid #555;'
                    f'border-radius:4px;color:#4fc3f7;font-size:11px;'
                    f'text-decoration:none;">▶</a></div>')
                html_lines.append(prev)
            else:
                html_lines.append(
                    f'<a href="{url}" style="display:inline-block;margin:2px 0 6px;'
                    f'padding:4px 10px;background:#2a2a2a;border:1px solid #555;'
                    f'border-radius:6px;color:#4fc3f7;font-size:12px;'
                    f'text-decoration:none;">▶</a>'
                )
            continue

        html_lines.append(
            f'<div style="font-size:13px;color:#ddd;line-height:1.7;word-break:break-all;">{e}</div>'
        )

    return """<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
</head>
<body style="background:#1a1a1a;color:#ddd;font-family:-apple-system,sans-serif;
  padding:12px 16px;max-width:600px;margin:0 auto;">
""" + "\n".join(html_lines) + "\n</body></html>"


def send_email(subject: str, body: str) -> None:
    html_body = _build_html(body)
    resend.Emails.send({
        "from":    "onboarding@resend.dev",
        "to":      EMAIL_TO,
        "subject": subject,
        "text":    body,
        "html":    html_body,
    })
    print("メール送信完了")


# ============================================================
# メイン処理
# ============================================================
def main():
    print("=== IR通知スクリプト 開始 ===")

    stock_data = []
    for stock in STOCKS:
        print(f"  [{stock['code']}] {stock['name']} を取得中...")
        buffett    = get_financial_data(stock["code"])
        passed     = passes_buffett_screen(buffett)
        price      = get_stock_price(stock["code"])
        news       = get_stock_news(stock["code"]) if passed else []
        signals    = get_technical_signals(stock["code"])
        margin     = get_margin_ratio(stock["code"])
        print(f"    バフェット: {'✔ 通過' if passed else '✘ 不通過'} "
              f"ROE={buffett['roe']} 自己資本比率={buffett['equity_ratio']}")
        stock_data.append({
            "stock": stock, "buffett": buffett, "buffett_passed": passed,
            "price": price, "news": news, "signals": signals, "margin": margin,
        })

    print("  日経平均を取得中...")
    nikkei = get_nikkei_data()
    print("  東証33業種トレンドを取得中...")
    sectors = get_sector_trends()
    print(f"  東証33業種: {len(sectors)}業種取得")

    print("  WTI 原油価格を取得中...")
    wti = get_wti_price()

    print("  アルジャジーラRSSを取得中...")
    world_news = get_aljazeera_news()
    if ANTHROPIC_API_KEY:
        print("  Claude Haiku でニュース影響度を分析中...")
        world_news = analyze_aljazeera_news(world_news)
    else:
        print("  ANTHROPIC_API_KEY 未設定のためClaude分析をスキップ")
    print("  世界ビジネスニュースRSSを取得中...")
    reuters_news, reuters_source = get_world_business_news()
    print("  NHK 関税・地政学リスクニュースを取得中...")
    nhk_risk_news = get_nhk_risk_news()

    if EDINET_API_KEY:
        edinet_data = get_edinet_financials(STOCKS)
    else:
        print("  EDINET_API_KEY 未設定のため決算進捗をスキップ")
        edinet_data = []

    if ANTHROPIC_API_KEY:
        print("  Claude Haiku でBuffett視点分析中...")
        buffett_analysis = analyze_with_buffett_lens(stock_data)
    else:
        buffett_analysis = {}

    print("  銘柄スクリーニング中 (出来高急増ランキング 最大8ページ)...")
    screened = get_screened_stocks()
    print(f"  スクリーニング結果: {len(screened)}件")

    print("  コーポレートアクション情報を取得中...")
    try:
        corp_actions = get_corporate_actions(STOCKS)
    except Exception as e:
        print(f"  コーポレートアクション取得失敗: {e}")
        corp_actions = {}

    today   = date.today().strftime("%Y/%m/%d")
    subject = f"[IR通知] {today} 銘柄ニュース・WTI価格・世界情勢"
    body    = build_email_body(stock_data, wti, world_news, edinet_data, screened,
                               nikkei=nikkei, sectors=sectors, reuters_news=reuters_news,
                               reuters_source=reuters_source,
                               nhk_risk_news=nhk_risk_news,
                               buffett_analysis=buffett_analysis,
                               corp_actions=corp_actions)

    print("メールを送信中...")
    send_email(subject, body)
    print("=== 完了 ===")


if __name__ == "__main__":
    main()
