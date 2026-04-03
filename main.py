#!/usr/bin/env python3
"""
IR情報・WTI原油価格 自動通知スクリプト
毎朝 GitHub Actions で自動実行 → Resend でメール送信
"""

import os
import re
import io
import json
import time
import zipfile
import xml.etree.ElementTree as ET
import resend
import requests
from datetime import date, timedelta
from bs4 import BeautifulSoup

# ============================================================
# 設定（GitHub Secrets から自動的に読み込まれます）
# ============================================================
resend.api_key     = os.environ["RESEND_API_KEY"]          # Resend API キー
EMAIL_FROM         = os.environ["EMAIL_FROM"]               # 送信元アドレス
EMAIL_TO           = os.environ["EMAIL_TO"]                 # 受信先メールアドレス
EDINET_API_KEY     = os.environ.get("EDINET_API_KEY", "")  # EDINET API キー（任意）

# ============================================================
# 監視銘柄リスト
# ============================================================
STOCKS = [
    {"name": "ジャパンマテリアル", "code": "6055"},
    {"name": "坪田ラボ",           "code": "4890"},
    {"name": "エクシオグループ",   "code": "1951"},
    {"name": "ダイダン",           "code": "1980"},
    {"name": "キオクシア",         "code": "285A"},
    {"name": "アズビル",           "code": "6845"},
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
    """kabutan.jp 用セッションを返す。初回呼び出し時にホームページを訪問してCookieを取得する。"""
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
ROE_MIN          = 15.0   # ROE 15% 以上
EQUITY_RATIO_MIN = 40.0   # 自己資本比率 40% 以上


# ============================================================
# EDINET v2 API → 四半期報告書の決算進捗
# ============================================================
_EDINET_BASE = "https://disclosure.edinet-fsa.go.jp/api/v2"
_XBRL_NS     = "http://www.xbrl.org/2003/instance"
_XSI_NIL     = "{http://www.w3.org/2001/XMLSchema-instance}nil"

# XBRL 要素名セット（複数の命名規則に対応）
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
    """指定日に提出された書類一覧を返す"""
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
    """EDINETのsecCode（5桁・スペース含む等）を4桁コードにマッチングする。
    例: '60550' → '6055' / '6055 ' → '6055' / '6055' → '6055'
    """
    raw = (raw or "").strip()
    # 先頭4桁が一致するか確認
    if len(raw) >= 4 and raw[:4] in codes4:
        return raw[:4]
    # 末尾が '0' の5桁コード（例: '60550'）→ 先頭4桁
    if len(raw) == 5 and raw[4] == "0" and raw[:4] in codes4:
        return raw[:4]
    return None


def _find_quarterly_docs(codes4: list) -> dict:
    """各4桁証券コードの最新四半期/半期報告書を最大90日遡って検索。
    2024年4月から四半期報告書(120)が廃止され半期報告書(130)に統合されたため両方を対象とする。
    secCodeは5桁・スペース・末尾0など表記ゆれがあるため柔軟にマッチングする。
    """
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
    """EDINET書類ZIPをダウンロードし、最大のXBRLインスタンス文書を返す"""
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
    """XBRLから売上高・営業利益・純利益（当期累計実績・通期予想）を抽出"""
    try:
        root = ET.fromstring(xbrl)
    except ET.ParseError:
        return {}

    # コンテキスト分類
    actual_ctx, forecast_ctx = set(), set()
    for ctx in root.findall(f"{{{_XBRL_NS}}}context"):
        cid = ctx.get("id", "")
        if "Forecast" in cid:
            forecast_ctx.add(cid)
        elif re.search(r"CurrentAccumulated|CurrentYear", cid) and "Prior" not in cid:
            actual_ctx.add(cid)
    # フォールバック: "Current" を含み Prior/Forecast でないもの
    if not actual_ctx:
        for ctx in root.findall(f"{{{_XBRL_NS}}}context"):
            cid = ctx.get("id", "")
            if "Current" in cid and "Forecast" not in cid and "Prior" not in cid:
                actual_ctx.add(cid)

    # 四半期番号を推定
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
    """進捗率と四半期ペースから ○△× を判定"""
    pace = {"Q1": 25.0, "Q2": 50.0, "Q3": 75.0}.get(quarter, 100.0)
    diff = progress_pct - pace
    if diff >= -5.0:
        return "○"
    elif diff >= -15.0:
        return "△"
    else:
        return "×"


def get_edinet_financials(stocks: list) -> list:
    """監視銘柄の四半期報告書から決算進捗データを取得"""
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
# アルジャジーラ RSS → 世界情勢ニュース抽出
# ============================================================
_WORLD_KEYWORDS = [
    # 経済・金融
    "economy", "economic", "inflation", "recession", "gdp", "debt", "trade",
    "tariff", "sanction", "dollar", "currency", "market", "finance", "financial",
    "bank", "interest rate", "federal reserve", "central bank", "imf",
    "investment", "bond", "deficit", "surplus", "export", "import",
    # エネルギー
    "oil", "gas", "energy", "opec", "crude", "petroleum", "fuel", "nuclear",
    "pipeline", "lng", "electricity", "renewable",
    # 地政学リスク
    "war", "conflict", "tension", "crisis", "military", "attack", "ceasefire",
    "missile", "protest", "coup", "nato", "troops", "invasion", "occupation",
    "blockade", "embargo", "strait", "geopolit", "escalat", "airstrike",
    "sanction", "alliance", "treaty",
]

def get_aljazeera_news(max_items: int = 7) -> list:
    """アルジャジーラRSSから経済・エネルギー・地政学ニュースをスコアリングして返す"""
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


# ============================================================
# バフェット指標取得（kabutan.jp /stock/finance）
# ============================================================
def _extract_col_value(table, col_keywords: list, exact: bool = False) -> float | None:
    """テーブルのヘッダー行からキーワードが一致する列を探し、最新実績行の値を返す。
    kabutan.jp は「ヘッダー行 → データ行」の縦持ち構造。全角/半角どちらにも対応。
    exact=True の場合は部分一致でなく完全一致で列を探す。"""
    rows = table.find_all("tr")
    if not rows:
        return None

    # ヘッダー行から対象列インデックスを探す
    header_cells = [c.get_text(strip=True) for c in rows[0].find_all(["th", "td"])]
    col_idx = None
    for i, h in enumerate(header_cells):
        # 全角→半角に正規化して比較
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

    # データ行を逆順に走査し、最新の実績値（予想行・空行を除く）を返す
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
    """'18.9倍' や '－倍' などの比率文字列を float に変換する。－ は None を返す。"""
    raw = s.replace("倍", "").replace(",", "").replace("－", "").strip()
    try:
        return float(raw)
    except ValueError:
        return None


def _parse_mktcap_mn(s: str) -> float | None:
    """時価総額文字列（例: '1,744億円', '11兆8,954億円'）を百万円単位の float に変換する。"""
    s = s.replace(",", "").replace("円", "").strip()
    mn = 0.0
    if "兆" in s:
        parts = s.split("兆")
        mn += float(parts[0]) * 1_000_000  # 1兆円 = 1,000,000百万円
        s = parts[1]
    if "億" in s:
        mn += float(s.replace("億", "")) * 100  # 1億円 = 100百万円
        return mn
    return None


def _classify_cf_pattern(op_cf: float, inv_cf: float, fin_cf: float) -> str:
    """営業CF・投資CF・財務CFの符号からキャッシュフローパターンを分類する。"""
    op_pos  = op_cf  > 0
    inv_neg = inv_cf < 0
    fin_pos = fin_cf > 0
    if op_pos and inv_neg and fin_pos:
        return "成長型"       # 本業好調＋積極投資＋外部調達
    if op_pos and inv_neg and not fin_pos:
        return "安定型"       # 本業好調＋適度な投資＋借入返済・株主還元
    if op_pos and not inv_neg and not fin_pos:
        return "収穫型"       # 本業好調＋資産回収＋借入返済・株主還元
    if op_pos and not inv_neg and fin_pos:
        return "キャッシュ蓄積型"  # 本業好調＋資産回収＋外部調達でキャッシュ積み上げ
    if not op_pos and inv_cf > 0 and fin_pos:
        return "再建型"       # 本業不振＋資産売却＋外部調達で再建中
    if not op_pos and inv_cf > 0 and not fin_pos:
        return "リストラ型"   # 本業不振＋資産売却で借入返済
    if not op_pos and inv_neg:
        return "危険型"       # 本業不振＋借入で投資継続
    return "その他"


def _calc_health_score(f: dict) -> int:
    """ROE・ROIC・CFパターン・売上成長率・営業利益率から100点満点のヘルススコアを算出する。"""
    score = 0

    # ROE（25点）
    roe = f.get("roe")
    if roe is not None:
        if roe >= 20:   score += 25
        elif roe >= 15: score += 20
        elif roe >= 10: score += 15
        elif roe >= 5:  score += 8

    # ROIC（25点）
    roic = f.get("roic")
    if roic is not None:
        if roic >= 15:   score += 25
        elif roic >= 10: score += 20
        elif roic >= 5:  score += 10

    # CFパターン（20点）
    cf_scores = {
        "安定型": 20, "成長型": 15, "収穫型": 15,
        "キャッシュ蓄積型": 10, "再建型": 5, "リストラ型": 5, "危険型": 0,
    }
    score += cf_scores.get(f.get("cf_pattern") or "", 0)

    # 売上成長率（20点）
    sg = f.get("sales_growth")
    if sg is not None:
        if sg >= 10:  score += 20
        elif sg >= 5: score += 15
        elif sg >= 0: score += 8

    # 営業利益率（10点）
    om = f.get("op_margin")
    if om is not None:
        if om >= 20:   score += 10
        elif om >= 10: score += 8
        elif om >= 5:  score += 4

    return score


def get_financial_data(code: str) -> dict:
    """kabutan.jp の財務ページから ROE・自己資本比率・ROIC・CFパターン・来期純利益予想を取得する。"""
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

            # PER・PBR・時価総額テーブル（Table2: 行1=ラベル, 行2=値, 行3=時価総額）
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

            # 収益性テーブル（ROE・営業益・売上営業利益率）
            elif roe is None and ("ＲＯＥ" in header_text or "ROE" in header_text) and "総資産回転率" in header_text:
                roe       = _extract_col_value(table, ["ＲＯＥ", "ROE"])
                op_income = _extract_col_value(table, ["営業益"])
                op_margin = _extract_col_value(table, ["売上営業利益率"])

            # 財務テーブル（自己資本比率・自己資本・有利子負債倍率）
            elif equity_ratio is None and "自己資本比率" in header_text and "有利子負債倍率" in header_text:
                equity_ratio = _extract_col_value(table, ["自己資本比率"])
                equity       = _extract_col_value(table, ["自己資本"], exact=True)
                debt_ratio   = _extract_col_value(table, ["有利子負債倍率"])

            # CFテーブル（営業CF・投資CF・財務CF・現金等残高）
            elif op_cf is None and "営業CF" in header_text and "投資CF" in header_text:
                op_cf  = _extract_col_value(table, ["営業CF"],     exact=True)
                inv_cf = _extract_col_value(table, ["投資CF"],     exact=True)
                fin_cf = _extract_col_value(table, ["財務CF"],     exact=True)
                cash   = _extract_col_value(table, ["現金等残高"], exact=True)

            # 通期決算テーブル（実績最終益・来期予想最終益・売上成長率）— 半期テーブルを除外
            elif actual_ni is None and "最終益" in header_cells and "修正1株配" in header_cells:
                ni_col    = header_cells.index("最終益")
                sales_col = header_cells.index("売上高") if "売上高" in header_cells else None

                # reversed で最新順に走査し、実績最終益・来期予想・売上高2期分を取得
                sales_actual: list[float] = []
                for row in reversed(table.find_all("tr")[1:]):
                    cells = row.find_all(["th", "td"])
                    if not cells or ni_col >= len(cells):
                        continue
                    first = cells[0].get_text(strip=True)
                    if not first:
                        continue
                    # 年度形式（YYYY.MM）のみ対象。半期テーブル（YY.MM-MM）は除外
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
                        # 売上高を最大2期分収集（最新→前期の順）
                        if sales_col is not None and len(sales_actual) < 2:
                            s_raw = (cells[sales_col].get_text(strip=True)
                                     .replace(",", "").strip())
                            try:
                                sales_actual.append(float(s_raw))
                            except ValueError:
                                pass

                # 売上成長率 = (当期 - 前期) / 前期 × 100
                if len(sales_actual) >= 2 and sales_actual[1] != 0:
                    sales_growth = round(
                        (sales_actual[0] - sales_actual[1]) / sales_actual[1] * 100, 1
                    )

        # ROIC = NOPAT(営業利益×0.7) / 投下資本(自己資本×(1+有利子負債倍率))
        roic = None
        if op_income is not None and equity is not None and equity > 0:
            dr   = debt_ratio if debt_ratio is not None else 0.0
            roic = round(op_income * 0.7 / (equity * (1 + dr)) * 100, 1)

        # CFパターン判定
        cf_pattern = None
        if op_cf is not None and inv_cf is not None and fin_cf is not None:
            cf_pattern = _classify_cf_pattern(op_cf, inv_cf, fin_cf)

        # 来期純利益予想の前年比
        ni_forecast_yoy = None
        if actual_ni is not None and forecast_ni is not None and actual_ni != 0:
            ni_forecast_yoy = round((forecast_ni - actual_ni) / abs(actual_ni) * 100, 1)

        # PEG比率 = PER ÷ 売上成長率（成長率が正の場合のみ）
        peg = None
        if per is not None and sales_growth is not None and sales_growth > 0:
            peg = round(per / sales_growth, 2)

        # グレアムスコア = PER × PBR（22.5以下で割安）
        graham = None
        if per is not None and pbr is not None:
            graham = round(per * pbr, 1)

        # EV/EBITDA（EBITDAは営業利益で代替、kabutan.jpに減価償却費の個別開示なし）
        ev_ebitda = None
        if mktcap_mn is not None and op_income is not None and op_income > 0:
            interest_debt = (equity or 0) * (debt_ratio or 0)
            ev = mktcap_mn + interest_debt - (cash or 0)
            ev_ebitda = round(ev / op_income, 1)

        # ヘルススコア（100点満点）
        financials_for_score = {
            "roe": roe, "roic": roic, "cf_pattern": cf_pattern,
            "sales_growth": sales_growth, "op_margin": op_margin,
        }
        health_score = _calc_health_score(financials_for_score)

        print(
            f"    財務データ: ROE={roe} 自己資本比率={equity_ratio} ROIC={roic}% "
            f"営業利益率={op_margin}% 売上成長率={sales_growth}% CF={cf_pattern} "
            f"来期益={ni_forecast_yoy}% PEG={peg} グレアム={graham} EV/EBITDA={ev_ebitda} スコア={health_score}"
        )
        return {
            "roe": roe, "equity_ratio": equity_ratio,
            "roic": roic, "cf_pattern": cf_pattern,
            "ni_forecast_yoy": ni_forecast_yoy,
            "op_margin": op_margin, "sales_growth": sales_growth,
            "health_score": health_score,
            "per": per, "pbr": pbr,
            "peg": peg, "graham": graham, "ev_ebitda": ev_ebitda,
        }

    except Exception as e:
        print(f"    [警告] {code} の財務データ取得失敗: {e}")
        return {
            "roe": None, "equity_ratio": None,
            "roic": None, "cf_pattern": None, "ni_forecast_yoy": None,
            "op_margin": None, "sales_growth": None, "health_score": 0,
            "per": None, "pbr": None,
            "peg": None, "graham": None, "ev_ebitda": None,
        }


def passes_buffett_screen(financials: dict) -> bool:
    """ROE >= 15% かつ 自己資本比率 >= 40% で通過"""
    roe = financials.get("roe")
    eq  = financials.get("equity_ratio")
    if roe is None or eq is None:
        return False
    return roe >= ROE_MIN and eq >= EQUITY_RATIO_MIN


# ============================================================
# IR・ニュース取得（kabutan.jp）
# ============================================================
def get_stock_news(code: str, max_items: int = 5) -> list:
    """kabutan.jp の IR・ニュースページから最新情報を取得する"""
    results = []

    # ニュース取得（kabutan.jp /stock/news のみ）
    for path in [f"/stock/news?code={code}"]:
        url = f"https://kabutan.jp{path}"
        try:
            session = _get_kabutan_session()
            res = session.get(url, headers={"Referer": "https://kabutan.jp/"}, timeout=15)
            res.raise_for_status()
            soup = BeautifulSoup(res.text, "html.parser")

            # テーブル行を探す（複数のセレクタに対応）
            SELECTORS = [
                "table.s-news-list tr",
                "table.news_list tr",
                "#newslist table tr",
                "div#news_list table tr",
                "div.news_box table tr",
                "table tr",           # 最後の手段：ページ内の全テーブル行
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
                # ヘッダー行や空行はスキップ
                if not date_text or date_text in ("日付", "日時", ""):
                    continue
                # 日付らしくない行はスキップ（数字が含まれない）
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
                    "date":  date_text,
                    "title": title,
                    "url":   href,
                })

                if len(results) >= max_items:
                    break

            if results:
                return results  # 取得できたら終了

        except requests.RequestException as e:
            print(f"    [警告] {code} のニュース取得失敗 ({url}): {e}")
        except Exception as e:
            print(f"    [警告] {code} のHTML解析失敗: {e}")

    return [{"date": "-", "title": "情報を取得できませんでした", "url": ""}]


# ============================================================
# Yahoo Finance API から価格取得（共通）
# ============================================================
_YF_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}

def _fetch_yahoo(symbol: str) -> list:
    """Yahoo Finance API から直近5日の終値リストを返す"""
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=5d"
    res = requests.get(url, headers=_YF_HEADERS, timeout=15)
    res.raise_for_status()
    data = res.json()
    closes = data["chart"]["result"][0]["indicators"]["quote"][0]["close"]
    return [c for c in closes if c is not None]


def _fetch_yahoo_full(symbol: str, range_: str = "60d") -> tuple[list, list]:
    """Yahoo Finance から終値・出来高リストを返す (None除外済みペア)。"""
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
# テクニカル指標計算
# ============================================================
def _calc_rsi(closes: list, period: int = 14) -> float | None:
    """Wilder平滑化法による RSI(14) 計算"""
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
    """25日移動平均線からの乖離率（%）"""
    if len(closes) < 25:
        return None
    ma25 = sum(closes[-25:]) / 25
    return round((closes[-1] - ma25) / ma25 * 100, 1)


def _calc_vol_surge(volumes: list) -> float | None:
    """直近出来高の5日平均比（倍率）"""
    if len(volumes) < 6:
        return None
    avg5 = sum(volumes[-6:-1]) / 5
    if avg5 == 0:
        return None
    return round(volumes[-1] / avg5, 2)


def _calc_ema(closes: list, period: int) -> list:
    """指数移動平均（EMA）リストを返す"""
    if len(closes) < period:
        return []
    k = 2 / (period + 1)
    ema = [sum(closes[:period]) / period]
    for price in closes[period:]:
        ema.append(price * k + ema[-1] * (1 - k))
    return ema


def _calc_macd(closes: list) -> dict:
    """MACD(12,26,9) を計算して方向性とシグナルとのクロスを返す"""
    if len(closes) < 35:
        return {}
    ema12 = _calc_ema(closes, 12)
    ema26 = _calc_ema(closes, 26)
    # ema12とema26を同じ長さに揃える
    diff = len(ema12) - len(ema26)
    ema12 = ema12[diff:]
    macd_line = [e12 - e26 for e12, e26 in zip(ema12, ema26)]
    if len(macd_line) < 9:
        return {}
    signal_line = _calc_ema(macd_line, 9)
    # macd_lineとsignal_lineの末尾2点でクロス判定
    diff2 = len(macd_line) - len(signal_line)
    macd_tail = macd_line[diff2:]
    if len(macd_tail) < 2 or len(signal_line) < 2:
        return {}
    prev_diff = macd_tail[-2] - signal_line[-2]
    curr_diff = macd_tail[-1] - signal_line[-1]
    golden = prev_diff < 0 and curr_diff >= 0   # ゴールデンクロス（買い）
    dead   = prev_diff > 0 and curr_diff <= 0   # デッドクロス（売り）
    return {
        "macd":   round(macd_tail[-1], 4),
        "signal": round(signal_line[-1], 4),
        "hist":   round(curr_diff, 4),
        "golden": golden,
        "dead":   dead,
        "bullish": curr_diff > 0,  # MACDがシグナル線より上 = 上昇トレンド
    }


def _calc_bollinger(closes: list, period: int = 25, sigma: float = 2.0) -> dict:
    """ボリンジャーバンド（±2σ）を計算して位置を返す"""
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
        "above_upper": price >= upper,   # 上限突破 → 過熱・売り候補
        "below_lower": price <= lower,   # 下限突破 → 売られすぎ・買い候補
        "pct_b": round((price - lower) / (upper - lower) * 100, 1) if upper != lower else 50.0,
    }


def _calc_golden_dead_cross(closes: list) -> dict:
    """5日・25日MAのゴールデン/デッドクロスを判定"""
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
        "above":  ma5_curr > ma25_curr,  # 5MA > 25MA = 上昇トレンド中
    }


def _calc_liquidity(closes: list, volumes: list) -> dict:
    """直近5日の日次売買代金（株価×出来高）から流動性リスクを判定する。
    基準: 1億円以上=OK / 3000万〜1億円=⚠️ / 3000万円未満=❌流動性不足
    """
    if len(closes) < 5 or len(volumes) < 5:
        return {"avg_volume_yen": None, "judge": "unknown", "label": "データ不足"}
    # 直近5日の売買代金（円）= 終値 × 出来高
    daily_values = [c * v for c, v in zip(closes[-5:], volumes[-5:]) if c and v]
    if not daily_values:
        return {"avg_volume_yen": None, "judge": "unknown", "label": "データ不足"}
    avg = sum(daily_values) / len(daily_values)
    if avg >= 1_0000_0000:        # 1億円以上
        judge, label = "ok", f"{avg/1e8:.1f}億円/日"
    elif avg >= 3000_0000:        # 3000万円以上
        judge, label = "warn", f"⚠️{avg/1e4:.0f}万円/日（流動性やや低）"
    else:                          # 3000万円未満
        judge, label = "low", f"❌{avg/1e4:.0f}万円/日（流動性不足）"
    return {"avg_volume_yen": round(avg), "judge": judge, "label": label}


def get_technical_signals(code: str) -> dict:
    """Yahoo Finance から7指標+流動性を計算してシグナルを返す。
    指標: RSI / 25MA乖離 / 出来高急増 / MACD / ボリンジャーバンド / ゴールデン・デッドクロス / 出来高+株価上昇 / 流動性
    """
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

        # ① RSI
        if rsi is not None:
            if rsi <= 30:
                signals.append(f"[買] RSI売られ過ぎ({rsi:.1f})")
                buy_count += 1
            elif rsi >= 70:
                signals.append(f"[売] RSI買われ過ぎ({rsi:.1f})")
                sell_count += 1

        # ② 25MA乖離
        if ma25_dev is not None:
            if ma25_dev <= -5.0:
                signals.append(f"[買検討] 25MA下方乖離({ma25_dev:+.1f}%)")
                buy_count += 1
            elif ma25_dev >= 10.0:
                signals.append(f"[利確検討] 25MA上方乖離({ma25_dev:+.1f}%)")
                sell_count += 1

        # ③ 出来高急増
        if vol_surge is not None and vol_surge >= 2.0:
            # ④ 出来高急増 + 株価上昇の組み合わせ（本物の上昇確認）
            if len(closes) >= 2 and closes[-1] > closes[-2]:
                signals.append(f"[強買] 出来高急増＋株価上昇({vol_surge:.1f}倍)")
                buy_count += 2
            else:
                signals.append(f"[注目] 出来高急増({vol_surge:.1f}倍)")
                buy_count += 1

        # ⑤ MACD
        if macd:
            if macd.get("golden"):
                signals.append("[買] MACDゴールデンクロス")
                buy_count += 1
            elif macd.get("dead"):
                signals.append("[売] MACDデッドクロス")
                sell_count += 1
            elif macd.get("bullish"):
                signals.append("[↑] MACD上昇トレンド中")

        # ⑥ ボリンジャーバンド
        if boll:
            if boll.get("below_lower"):
                signals.append(f"[買] BB下限割れ(%-B:{boll['pct_b']:.0f}%)")
                buy_count += 1
            elif boll.get("above_upper"):
                signals.append(f"[売] BB上限突破(%-B:{boll['pct_b']:.0f}%)")
                sell_count += 1

        # ⑦ ゴールデン/デッドクロス（5日・25日MA）
        if cross:
            if cross.get("golden"):
                signals.append("[買] ゴールデンクロス(5MA>25MA)")
                buy_count += 1
            elif cross.get("dead"):
                signals.append("[売] デッドクロス(5MA<25MA)")
                sell_count += 1

        # ⑧ 流動性チェック（売買代金）※メール表示側で別途表示するためsignalsには追加しない
        liq_judge = liquidity.get("judge", "unknown")

        # 総合判定（流動性不足は強制的に警告付き）
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
# 株価取得（Yahoo Finance API）
# ============================================================
def get_stock_price(code: str) -> dict:
    """Yahoo Finance API で現在株価・前日比を取得する（東証: コード.T）"""
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
# WTI 原油先物価格取得（Yahoo Finance API）
# ============================================================
def get_wti_price() -> dict:
    """WTI 原油先物（CL=F）の直近価格を取得する"""
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
# 日経平均 + 東証33業種トレンド
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
    """日経平均の現在値・前日比・騰落率を Yahoo Finance API から取得"""
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
    """kabutan.jp トップページの setIndustry() データから東証33業種の騰落率リストを返す

    戻り値: [{"id": int, "name": str, "change_pct": float}, ...]  (騰落率降順)
    """
    try:
        res = requests.get(f"https://kabutan.jp/?_={int(time.time())}", headers=HEADERS, timeout=15)
        res.raise_for_status()
        # setIndustry("datas=ID,PCT,#ID,PCT,#...", ...) を抽出
        m = re.search(r'setIndustry\("datas=([^"]+)"', res.text)
        if not m:
            return []
        raw = m.group(1)
        # 末尾の "#" を除いて "ID,PCT" ペアに分割
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
        # すでに降順ソート済みだが念のため
        sectors.sort(key=lambda x: x["change_pct"], reverse=True)
        return sectors
    except Exception as e:
        print(f"    [警告] 東証33業種取得失敗: {e}")
    return []


# ============================================================
# 銘柄スクリーニング（kabutan.jp 出来高急増ランキング）
# ============================================================
_SCREEN_URL = "https://kabutan.jp/warning/?mode=2_1&market=0&page={page}"

# カラム定義（td要素のみ 12列）
# [コード, 市場, ガイヨウ, チャート, 株価, S印, 前日比, 前日比%, 出来高, PER, PBR, 配当]
# ※ 銘柄名は <th> タグから別途取得
_COL_CODE   = 0
_COL_MARKET = 1
_COL_PRICE  = 4
_COL_VOLUME = 8
_COL_PER    = 9


def _get_op_profit(code: str) -> dict:
    """kabutan.jp 財務ページから営業利益・PBR・時価総額を取得する。"""
    url = f"https://kabutan.jp/stock/finance?code={code}"
    result = {"op_profit": None, "pbr": None, "mktcap_mn": None}
    try:
        session = _get_kabutan_session()
        res = session.get(url, headers={"Referer": "https://kabutan.jp/"}, timeout=10)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        tables = soup.find_all("table")

        # Table 2: PBR・時価総額
        if len(tables) > 2:
            t2_rows = tables[2].find_all("tr")
            if len(t2_rows) >= 2:
                val_cells = [c.get_text(strip=True) for c in t2_rows[1].find_all("td")]
                result["pbr"] = _parse_ratio_float(val_cells[1]) if len(val_cells) > 1 else None
            if len(t2_rows) >= 3:
                mc_cells = [c.get_text(strip=True) for c in t2_rows[2].find_all(["th", "td"])]
                if len(mc_cells) >= 2:
                    result["mktcap_mn"] = _parse_mktcap_mn(mc_cells[1])

        # 営業益（直近実績）
        for table in tables:
            rows = table.find_all("tr")
            if not rows:
                continue
            header = [c.get_text(strip=True) for c in rows[0].find_all(["th", "td"])]
            if "営業益" not in header:
                continue
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
            if result["op_profit"] is not None:
                break
    except Exception as e:
        print(f"    [警告] {code} 営業利益取得失敗: {e}")
    return result


def get_screened_stocks(max_pages: int = 8, max_results: int = 10) -> list:
    """出来高急増ランキングから条件合致銘柄を抽出

    フィルター条件:
      - 東証グロース / スタンダード
      - 株価 1000円以下
      - PER: 0 < PER ≤ 15（"－"=赤字・PER>60=実質赤字水準も除外）
      - 営業利益（直近通期実績）がプラスであること
    """
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

            # PERフィルター:
            #   "－" → 赤字（当期純損失のためPER算出不可）→ 除外
            #   PER ≤ 0 → 赤字 → 除外
            #   PER > 60 → 実質赤字水準（極小利益）→ 除外
            #   PER > 15 → スクリーニング条件外 → 除外
            per_raw = raw[_COL_PER].replace(",", "")
            if per_raw in ("－", "", "N/A"):  # 赤字でPER算出不可
                continue
            try:
                per = float(per_raw)
            except ValueError:
                continue
            if per <= 0 or per > 15:  # 赤字 or 条件外（60倍超も含む）
                continue

            try:
                volume = int(raw[_COL_VOLUME].replace(",", ""))
            except ValueError:
                volume = 0

            # 営業利益チェック（赤字企業の除外）+ PBR・時価総額取得
            code     = raw[_COL_CODE]
            fin_info = _get_op_profit(code)
            op_profit  = fin_info["op_profit"]
            pbr_screen = fin_info["pbr"]
            if op_profit is not None and op_profit <= 0:
                print(f"    [{code}] {name} 営業利益マイナス({op_profit:,.0f}百万円) → 除外")
                continue

            op_label = (
                f"{op_profit:+,.0f}百万円" if op_profit is not None else "確認不可"
            )
            graham_screen = round(per * pbr_screen, 1) if pbr_screen is not None else None

            results.append({
                "code":        code,
                "name":        name,
                "market":      market,
                "price":       price,
                "per":         per,
                "pbr":         pbr_screen,
                "graham":      graham_screen,
                "volume":      volume,
                "op_profit":   op_label,
                "stop_loss":   round(price * 0.92, 1),
                "take_profit": round(price * 1.25, 1),
            })

            if len(results) >= max_results:
                return results

        if not page_has_data:
            break

    return results


# ============================================================
# StockRadar 統合スコア計算
# ============================================================
def calc_integrated_score(
    stock_item: dict,
    wti: dict,
    world_news: list,
    edinet_entry: dict | None = None,
) -> dict:
    """各銘柄の統合スコアを100点満点で算出する。

    配点:
      テクニカル   30点  (7指標の買いシグナル数)
      財務         25点  (ROE・自己資本比率・ROIC・ヘルススコア)
      モメンタム   20点  (決算進捗・OI成長=営業利益成長)
      WTIシナリオ  15点  (A=15 / B=8 / C=0)
      世界情勢     10点  (地政学リスク低=高得点)
    """
    score = 0
    breakdown = {}

    # ── テクニカル（30点）──
    sig = stock_item.get("signals", {})
    buy_count  = sig.get("buy_count", 0)
    sell_count = sig.get("sell_count", 0)
    tech_score = max(0, min(30, buy_count * 6 - sell_count * 4))
    score += tech_score
    breakdown["テクニカル"] = (tech_score, 30,
        f"買{buy_count}指標 売{sell_count}指標")

    # ── 財務（25点）──
    fin = stock_item.get("buffett", {})
    hs  = fin.get("health_score", 0)          # 0〜100
    fin_score = round(hs / 100 * 25)
    score += fin_score
    roe = fin.get("roe")
    eq  = fin.get("equity_ratio")
    breakdown["財務"] = (fin_score, 25,
        f"ヘルススコア{hs}/100  ROE:{roe}%  自己資本:{eq}%")

    # ── モメンタム（20点）──
    mom_score = 0
    mom_notes = []
    # 決算進捗（EDINETデータから）
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
    # 売上成長（財務データから）
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
    breakdown["モメンタム"] = (mom_score, 20,
        " ".join(mom_notes) if mom_notes else "データなし")

    # ── WTIシナリオ（15点）──
    wti_price = wti.get("price") or 0
    if wti_price < 90:
        wti_score, wti_label = 15, "シナリオA（エントリー可）"
    elif wti_price <= 100:
        wti_score, wti_label = 8,  "シナリオB（凍結中）"
    else:
        wti_score, wti_label = 0,  "シナリオC（撤退検討）"
    score += wti_score
    breakdown["WTIシナリオ"] = (wti_score, 15,
        f"${wti_price:.1f}  {wti_label}")

    # ── 世界情勢（10点）──
    # アルジャジーラスコアの平均が低い=地政学リスク低=高得点
    if world_news:
        avg_risk = sum(n.get("score", 0) for n in world_news) / len(world_news)
        # スコア1〜3=低リスク→10点、4〜6=中→6点、7+=高リスク→2点
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

    # ── 総合判定 ──
    if score >= 70:
        rating = "★★★ 強い買いシグナル"
    elif score >= 50:
        rating = "★★  買い候補"
    elif score >= 30:
        rating = "★   様子見"
    else:
        rating = "    見送り"

    return {
        "score":     score,
        "rating":    rating,
        "breakdown": breakdown,
    }


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
) -> str:
    today = date.today().strftime("%Y年%m月%d日")
    lines = [
        f"■ IR・株価・WTI通知 ／ {today}",
        "=" * 52,
        "",
    ]

    # --- StockRadar 統合スコア サマリー ---
    lines.append("▼ StockRadar 統合スコア（監視銘柄）")
    lines.append("  ★★★≥70点=強買 / ★★≥50=買候補 / ★≥30=様子見 / <30=見送り")
    lines.append("  配点: テクニカル30 + 財務25 + モメンタム20 + WTI15 + 世界情勢10")
    lines.append("")
    for item in stock_data:
        # EDINETデータから対応エントリーを探す
        code4 = item["stock"]["code"][:4]
        edinet_entry = next(
            (e for e in edinet_data if e["stock"]["code"][:4] == code4), None
        ) if edinet_data else None
        integrated = calc_integrated_score(item, wti, world_news, edinet_entry)
        score = integrated["score"]
        rating = integrated["rating"]
        bd = integrated["breakdown"]
        price = item.get("price", {})
        price_str = f"¥{price['price']:,.0f}" if price.get("price") else "--"
        lines.append(f"  {item['stock']['name']}（{item['stock']['code']}）")
        lines.append(f"    {rating}  [{score}/100点]  現在値:{price_str}")
        for cat, (s, mx, note) in bd.items():
            bar = "■" * s + "□" * (mx - s) if mx <= 30 else ""
            lines.append(f"    {cat:<10} {s:>2}/{mx}点  {note}")
        lines.append("")
    lines.append("=" * 52)
    lines.append("")

    # --- 日経平均 ---
    lines.append("▼ 日経平均")
    if nikkei and nikkei.get("price"):
        sign = "+" if nikkei["change"] >= 0 else ""
        lines.append(
            f"  {nikkei['price']:,.2f}円  "
            f"（前日比 {sign}{nikkei['change']:+,.2f} ／ {sign}{nikkei['change_pct']:+.2f}%）"
        )
    else:
        lines.append("  取得できませんでした")
    lines.append("")

    # --- 東証33業種トレンド ---
    lines.append("▼ 東証33業種トレンド")
    if sectors:
        top3    = sectors[:3]
        bottom3 = sectors[-3:][::-1]   # 下落上位3（変化率が小さい順→昇順）
        lines.append("  【上昇上位3業種】")
        for s in top3:
            lines.append(f"    {s['name']:<12}  +{s['change_pct']:.2f}%")
        lines.append("  【下落上位3業種】")
        for s in bottom3:
            sign = "+" if s["change_pct"] >= 0 else ""
            lines.append(f"    {s['name']:<12}  {sign}{s['change_pct']:.2f}%")
    else:
        lines.append("  取得できませんでした")
    lines.append("")
    lines.append("=" * 52)
    lines.append("")

    # --- 世界情勢（Al Jazeera） ---
    lines.append("▼ 世界情勢（Al Jazeera）")
    if world_news:
        for n in world_news:
            lines.append(f"  {n['pub_date'][:16] if n['pub_date'] else '-'}")
            lines.append(f"  {n['title']}")
            if n["url"]:
                lines.append(f"  {n['url']}")
            lines.append("")
    else:
        lines.append("  ニュースを取得できませんでした")
        lines.append("")
    lines.append("=" * 52)
    lines.append("")

    # --- WTI 原油価格 + シナリオ判定 ---
    lines.append("▼ WTI 原油先物価格（ドル/バレル）")
    if wti.get("price"):
        sign = "+" if wti["change"] >= 0 else ""
        wti_price = wti["price"]
        lines.append(
            f"  ${wti_price:,.2f}  "
            f"（前日比 {sign}{wti['change']:+.2f} ／ {sign}{wti['change_pct']:+.2f}%）"
        )
        lines.append("")

        # シナリオ判定
        if wti_price < 90:
            scenario = "A"
            scenario_label = "🟢 シナリオA：停戦・封鎖解除（最良）"
            scenario_desc = [
                "  【状況】WTI $90以下 → 地政学リスク後退",
                "  【アクション】確認翌営業日から順次エントリー開始",
                "  【優先順】",
                "    1位 日本電技（1723）目標 9,000円",
                "    2位 新日本空調（1952）目標 3,400円",
                "    3位 ローツェ（6323）目標 2,600円",
            ]
        elif wti_price <= 100:
            scenario = "B"
            scenario_label = "🟡 シナリオB：膠着継続（現状維持）"
            scenario_desc = [
                "  【状況】WTI $90〜$100 → 情勢変わらず",
                "  【アクション】新規エントリー凍結継続",
                "  【例外的エントリー候補】",
                "    ・楽待（6037）910円逆指値維持のまま保有継続",
                f"    ・JM（6055）WTI $95以下({'✅ 条件クリア' if wti_price <= 95 else '❌ 未達'})＋株価1,750円以上で買い増し検討",
            ]
        else:
            scenario = "C"
            scenario_label = "🔴 シナリオC：完全ロックダウン（最悪）"
            scenario_desc = [
                "  【状況】WTI $120超 → イラン停戦拒否継続",
                "  【アクション】全ポジション見直し・新規エントリー全面停止",
                "  【必須対応】",
                "    → JM・楽待ともに損切りライン厳守",
                "  【逆張り注目銘柄（保有不要・観察のみ）】",
                "    ・新日本空調 → 原発緊急稼働で急騰可能性",
                "    ・東京計器 → 防衛需要爆発",
            ]

        lines.append(f"  ■ 現在のシナリオ判定：{scenario_label}")
        lines.append(f"  （A: <$90 エントリー可 ／ B: $90〜$100 凍結 ／ C: >$100 撤退）")
        lines.append("")
        lines.extend(scenario_desc)
    else:
        lines.append(f"  {wti.get('error', '取得失敗')}")
    lines.append("")
    lines.append("=" * 52)
    lines.append("")

    # --- バフェット指標スクリーニング サマリー ---
    lines.append(f"▼ バフェット指標スクリーニング（監視{len(stock_data)}銘柄）")
    lines.append(f"  条件: ROE {ROE_MIN:.0f}%以上 かつ 自己資本比率 {EQUITY_RATIO_MIN:.0f}%以上")
    lines.append(f"  参考: ROIC≥10%=✓ / 営業利益率≥10%=✓ / CFパターン")
    lines.append(f"        売上成長率 マイナス=⚠ / 来期益 -30%以下=⚠ / ヘルススコア /100")
    lines.append(f"        PEG≤1=割安✓ / グレアム(PER×PBR)≤22.5=割安✓ / EV/EBITDA≤10=割安✓")
    lines.append("")

    def _fmt_buffett_row(d: dict) -> list[str]:
        """1銘柄分のバフェット指標行を返す"""
        f    = d["buffett"]
        name = d["stock"]["name"]
        code = d["stock"]["code"]

        roe_s  = f"{f['roe']}%"           if f["roe"]          is not None else "--"
        eq_s   = f"{f['equity_ratio']}%"  if f["equity_ratio"] is not None else "--"

        roic_v = f.get("roic")
        roic_s = (f"{roic_v}%{'✓' if roic_v >= 10 else '✗'}"
                  if roic_v is not None else "--")

        om_v   = f.get("op_margin")
        om_s   = (f"{om_v}%{'✓' if om_v >= 10 else '✗'}"
                  if om_v is not None else "--")

        sg_v   = f.get("sales_growth")
        if sg_v is None:
            sg_s = "--"
        elif sg_v < 0:
            sg_s = f"{sg_v:+.1f}% ⚠"
        else:
            sg_s = f"{sg_v:+.1f}%✓"

        cf_s   = f.get("cf_pattern") or "--"

        yoy_v  = f.get("ni_forecast_yoy")
        if yoy_v is None:
            yoy_s = "--"
        elif yoy_v <= -30:
            yoy_s = f"{yoy_v:+.1f}% ⚠大幅減益"
        else:
            yoy_s = f"{yoy_v:+.1f}%"

        hs     = f.get("health_score", 0)
        hs_s   = f"{hs}/100"

        peg_v  = f.get("peg")
        peg_s  = (f"{peg_v:.2f}{'✓' if peg_v <= 1 else ''}"
                  if peg_v is not None else "--")

        gv     = f.get("graham")
        gs_s   = (f"{gv:.1f}{'✓' if gv <= 22.5 else ''}"
                  if gv is not None else "--")

        ev_v   = f.get("ev_ebitda")
        ev_s   = (f"{ev_v:.1f}倍{'✓' if ev_v <= 10 else ''}"
                  if ev_v is not None else "--")

        return [
            f"    {name}（{code}）  【スコア: {hs_s}】",
            f"      ROE:{roe_s}  自己資本比率:{eq_s}  ROIC:{roic_s}  営業利益率:{om_s}",
            f"      売上成長率:{sg_s}  CFパターン:{cf_s}  来期純利益予想:{yoy_s}",
            f"      PEG:{peg_s}  グレアム:{gs_s}  EV/EBITDA:{ev_s}",
        ]

    passed_items = [d for d in stock_data if d.get("buffett_passed")]
    failed_items = [d for d in stock_data if not d.get("buffett_passed")]
    if passed_items:
        lines.append("  ✔ 通過:")
        for d in passed_items:
            lines.extend(_fmt_buffett_row(d))
            lines.append("")
    if failed_items:
        lines.append("  ✘ 不通過:")
        for d in failed_items:
            lines.extend(_fmt_buffett_row(d))
            lines.append("")
    lines.append("")
    lines.append("=" * 52)
    lines.append("")

    # --- テクニカルシグナル ---
    lines.append("▼ テクニカルシグナル（監視銘柄）【7指標＋流動性】")
    lines.append("  ★強買=3指標以上一致 ◎買い=2指標 ○買い候補=1指標")
    lines.append("  ▼売り=2指標以上 △売り候補=1指標")
    lines.append("  流動性: ≥1億円/日=OK / 3000万〜1億=⚠️ / <3000万=❌")
    lines.append("")
    for item in stock_data:
        sig       = item.get("signals", {})
        rsi       = sig.get("rsi")
        ma25_dev  = sig.get("ma25_dev")
        vol_surge = sig.get("vol_surge")
        macd      = sig.get("macd") or {}
        boll      = sig.get("bollinger") or {}
        cross     = sig.get("cross") or {}
        liquidity = sig.get("liquidity") or {}
        summary   = sig.get("summary", "様子見")
        tags      = sig.get("signals", [])

        rsi_s  = f"RSI:{rsi:.1f}"             if rsi       is not None else "RSI:--"
        ma_s   = f"25MA:{ma25_dev:+.1f}%"     if ma25_dev  is not None else "25MA:--"
        vol_s  = f"出来高:{vol_surge:.1f}倍"   if vol_surge is not None else "出来高:--"
        hist   = macd.get("hist")
        macd_s = f"MACD:{hist:+.3f}"          if hist      is not None else "MACD:--"
        pct_b  = boll.get("pct_b")
        bb_s   = f"BB%-B:{pct_b:.0f}%"        if pct_b     is not None else "BB:--"
        ma5    = cross.get("ma5")
        cr_s   = (f"5MA{'>' if cross.get('above') else '<'}25MA") if ma5 is not None else "クロス:--"
        liq_s  = liquidity.get("label", "--")

        sig_str = "\n    ".join(tags) if tags else "シグナルなし"
        lines.append(f"  {item['stock']['name']}（{item['stock']['code']}）  → {summary}")
        lines.append(f"    {rsi_s}  {ma_s}  {vol_s}")
        lines.append(f"    {macd_s}  {bb_s}  {cr_s}")
        lines.append(f"    流動性: {liq_s}")
        lines.append(f"    {sig_str}")
        lines.append("")
    lines.append("=" * 52)
    lines.append("")

    # --- 各銘柄（通過銘柄のみ詳細表示） ---
    for item in stock_data:
        stock  = item["stock"]
        price  = item["price"]
        news   = item["news"]
        passed = item.get("buffett_passed", False)

        if not passed:
            continue

        # 株価
        if price.get("price"):
            sign = "+" if price["change"] >= 0 else ""
            price_str = (
                f"  現在値: ¥{price['price']:,.0f}  "
                f"（{sign}{price['change']:+.0f} ／ {sign}{price['change_pct']:+.2f}%）"
            )
        else:
            price_str = "  株価: 取得できませんでした"

        lines.append(f"▼ {stock['name']}（{stock['code']}） ✔")
        lines.append(price_str)

        sig      = item.get("signals", {})
        tags     = sig.get("signals", [])
        if tags:
            lines.append("  【シグナル】 " + " / ".join(tags))
        lines.append("")

        # ニュース・IR
        lines.append("  【最新IR・ニュース】")
        for n in news:
            lines.append(f"  {n['date']}  {n['title']}")
            if n["url"]:
                lines.append(f"           {n['url']}")
        lines.append("")

    # --- 決算進捗（EDINET 四半期報告書）---
    if edinet_data:
        # 実際にデータが取れたエントリーがあるか確認
        has_data = any(not e.get("error") or "見つかりませんでした" not in (e.get("error") or "") for e in edinet_data)
        all_not_found = all("見つかりませんでした" in (e.get("error") or "") for e in edinet_data)

        lines.append("▼ 決算進捗（EDINET 半期報告書）")
        if all_not_found:
            lines.append("  ※ 現在提出済みの報告書はありません")
            lines.append("  ※ 3月決算銘柄の次回提出予定：11月頃（第2四半期）")
            lines.append("")
            lines.append("=" * 52)
            lines.append("")
        else:
            for entry in edinet_data:
                stock = entry["stock"]
                doc   = entry.get("doc") or {}
                fin   = entry.get("financials") or {}
                q     = fin.get("quarter") or "?"
                period = (doc.get("periodEnd") or "")[:7]
                lines.append(f"  {stock['name']}（{stock['code']}）  {q} 期末:{period}")
                if entry.get("error"):
                    if "見つかりませんでした" in entry["error"]:
                        lines.append(f"    ※ 報告書未提出（次回：11月頃）")
                    else:
                        lines.append(f"    ※ {entry['error']}")
            else:
                for label, ak, fk in [
                    ("売上高",   "sales",      "sales_fc"),
                    ("営業利益", "op_income",  "op_fc"),
                    ("純利益",   "net_income", "net_fc"),
                ]:
                    actual = fin.get(ak)
                    fc     = fin.get(fk)
                    if actual is None:
                        lines.append(f"    {label}: データなし")
                        continue
                    a_oku = actual / 1e8
                    if fc:
                        prog  = actual / fc * 100
                        judge = _judge(prog, q)
                        lines.append(
                            f"    {label}: {a_oku:>8,.1f}億円 /"
                            f" 予想{fc/1e8:,.1f}億円  ({prog:.1f}%) {judge}"
                        )
                    else:
                        lines.append(f"    {label}: {a_oku:>8,.1f}億円  (通期予想なし)")
            lines.append("")
        lines.append("=" * 52)
        lines.append("")

    # --- 銘柄スクリーニング ---
    lines.append("▼ 銘柄スクリーニング（出来高急増 × PER≤15 × 株価≤1000 × 東Ｇ/東Ｓ）")
    lines.append("  ※ StockRadar簡易判定付き / 損切-8% / 利確+25%")
    lines.append("  流動性: ≥1億円/日=OK / <1億=⚠️")
    lines.append("")
    if screened:
        for s in screened:
            mkt = "グロース" if "東Ｇ" in s["market"] else "スタンダード"
            gv       = s.get("graham")
            pbr_s    = f"{s['pbr']:.2f}倍" if s.get("pbr") is not None else "--"
            graham_s = (f"{gv:.1f}{'✓' if gv <= 22.5 else ''}"
                        if gv is not None else "--")

            # --- StockRadar 簡易判定 ---
            radar_pass = []
            radar_fail = []

            # PER判定
            per = s.get("per")
            if per is not None:
                if per <= 10:
                    radar_pass.append(f"PER{per:.1f}倍✓")
                elif per <= 15:
                    radar_pass.append(f"PER{per:.1f}倍")
                else:
                    radar_fail.append(f"PER{per:.1f}倍✗")

            # グレアムスコア判定
            if gv is not None:
                if gv <= 22.5:
                    radar_pass.append(f"グレアム{gv:.1f}✓")
                else:
                    radar_fail.append(f"グレアム{gv:.1f}✗")

            # 営業利益判定（プラス確認済みだがラベル表示）
            op = s.get("op_profit", "")
            if "+" in str(op):
                radar_pass.append("営業利益✓")

            # 流動性判定（出来高×株価）
            vol = s.get("volume", 0)
            price = s.get("price", 0)
            daily_yen = vol * price
            if daily_yen >= 1_0000_0000:
                liq_s = f"{daily_yen/1e8:.1f}億円/日✓"
            elif daily_yen >= 3000_0000:
                liq_s = f"⚠️{daily_yen/1e4:.0f}万円/日"
                radar_fail.append("流動性⚠️")
            else:
                liq_s = f"❌{daily_yen/1e4:.0f}万円/日"
                radar_fail.append("流動性❌")

            # 総合判定
            fail_n = len(radar_fail)
            pass_n = len(radar_pass)
            if fail_n == 0 and pass_n >= 3:
                radar_verdict = "◎有望"
            elif fail_n == 0:
                radar_verdict = "○候補"
            elif "流動性❌" in radar_fail:
                radar_verdict = "❌流動性不足"
            elif fail_n >= 2:
                radar_verdict = "✗除外"
            else:
                radar_verdict = "△要検討"

            lines.append("  ----")
            lines.append(f"  【{s['code']}】{s['name']}（{mkt}）  {radar_verdict}")
            lines.append(
                f"  株価：{s['price']:,.0f}円  PER：{s['per']:.1f}倍  PBR：{pbr_s}"
                f"  グレアム：{graham_s}"
            )
            lines.append(f"  流動性：{liq_s}  出来高：{s['volume']:,}")
            lines.append(f"  営業利益（直近通期）：{s['op_profit']}")
            if radar_pass:
                lines.append(f"  ✓ {' / '.join(radar_pass)}")
            if radar_fail:
                lines.append(f"  ✗ {' / '.join(radar_fail)}")
            lines.append(f"  損切：▼{s['stop_loss']:,.1f}円 / 利確：▲{s['take_profit']:,.1f}円")
        lines.append("  ----")
    else:
        lines.append("  本日の条件合致銘柄はありませんでした")
    lines.append("")
    lines.append("=" * 52)
    lines.append("")

    lines.append("─" * 52)
    lines.append("このメールは自動送信されています。")
    return "\n".join(lines)


# ============================================================
# Resend 送信
# ============================================================
def _build_html(body: str) -> str:
    """テキスト本文をスマホ対応HTMLに変換する。"""
    def esc(s: str) -> str:
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    html_lines = []
    for line in body.split("\n"):
        raw = line.rstrip()
        e   = esc(raw)

        # 区切り線
        if set(raw.strip()) <= {"=", "─", "-"} and len(raw.strip()) >= 10:
            html_lines.append('<hr style="border:none;border-top:1px solid #444;margin:12px 0;">')
            continue

        # セクションヘッダー（▼ で始まる行）
        if raw.startswith("▼"):
            html_lines.append(
                f'<div style="font-size:15px;font-weight:bold;color:#4fc3f7;'
                f'margin:16px 0 6px;">{e}</div>'
            )
            continue

        # 総合判定行（★ で始まる）
        if "★★★" in raw:
            html_lines.append(
                f'<div style="font-size:14px;color:#ff5252;font-weight:bold;'
                f'padding:2px 0;">{e}</div>'
            )
            continue
        if "★★" in raw and "★★★" not in raw:
            html_lines.append(
                f'<div style="font-size:14px;color:#ffab40;font-weight:bold;'
                f'padding:2px 0;">{e}</div>'
            )
            continue
        if raw.strip().startswith("★") and "★★" not in raw:
            html_lines.append(
                f'<div style="font-size:13px;color:#aaa;padding:2px 0;">{e}</div>'
            )
            continue

        # シナリオ判定
        if "シナリオA" in raw and "🟢" in raw:
            html_lines.append(
                f'<div style="font-size:14px;color:#66bb6a;font-weight:bold;'
                f'padding:4px 0;">{e}</div>'
            )
            continue
        if "シナリオB" in raw and "🟡" in raw:
            html_lines.append(
                f'<div style="font-size:14px;color:#ffd54f;font-weight:bold;'
                f'padding:4px 0;">{e}</div>'
            )
            continue
        if "シナリオC" in raw and "🔴" in raw:
            html_lines.append(
                f'<div style="font-size:14px;color:#ef5350;font-weight:bold;'
                f'padding:4px 0;">{e}</div>'
            )
            continue

        # 銘柄名行（✔ or ✘ or 【 を含む見出し的な行）
        if ("✔" in raw or "✘" in raw) and ("（" in raw):
            color = "#66bb6a" if "✔" in raw else "#ef5350"
            html_lines.append(
                f'<div style="font-size:14px;font-weight:bold;color:{color};'
                f'margin-top:10px;">{e}</div>'
            )
            continue

        # 空行
        if not raw.strip():
            html_lines.append('<div style="height:6px;"></div>')
            continue

        # 通常行
        html_lines.append(
            f'<div style="font-size:13px;color:#ddd;line-height:1.7;'
            f'word-break:break-all;">{e}</div>'
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

    # 各銘柄のデータ収集
    stock_data = []
    for stock in STOCKS:
        print(f"  [{stock['code']}] {stock['name']} を取得中...")
        buffett    = get_financial_data(stock["code"])
        passed     = passes_buffett_screen(buffett)
        price      = get_stock_price(stock["code"])
        news       = get_stock_news(stock["code"]) if passed else []
        signals    = get_technical_signals(stock["code"])
        print(f"    バフェット: {'✔ 通過' if passed else '✘ 不通過'} "
              f"ROE={buffett['roe']} 自己資本比率={buffett['equity_ratio']}")
        stock_data.append({
            "stock": stock, "buffett": buffett, "buffett_passed": passed,
            "price": price, "news": news, "signals": signals,
        })

    # 日経平均・東証33業種
    print("  日経平均を取得中...")
    nikkei = get_nikkei_data()
    print("  東証33業種トレンドを取得中...")
    sectors = get_sector_trends()
    print(f"  東証33業種: {len(sectors)}業種取得")

    # WTI 価格取得
    print("  WTI 原油価格を取得中...")
    wti = get_wti_price()

    # 世界情勢ニュース取得
    print("  アルジャジーラRSSを取得中...")
    world_news = get_aljazeera_news()

    # 決算進捗取得（EDINET APIキーが設定されている場合のみ）
    if EDINET_API_KEY:
        edinet_data = get_edinet_financials(STOCKS)
    else:
        print("  EDINET_API_KEY 未設定のため決算進捗をスキップ")
        edinet_data = []

    # 銘柄スクリーニング
    print("  銘柄スクリーニング中 (出来高急増ランキング 最大8ページ)...")
    screened = get_screened_stocks()
    print(f"  スクリーニング結果: {len(screened)}件")

    # メール組み立て・送信
    today   = date.today().strftime("%Y/%m/%d")
    subject = f"[IR通知] {today} 銘柄ニュース・WTI価格・世界情勢"
    body    = build_email_body(stock_data, wti, world_news, edinet_data, screened,
                               nikkei=nikkei, sectors=sectors)

    print("メールを送信中...")
    send_email(subject, body)
    print("=== 完了 ===")


if __name__ == "__main__":
    main()
