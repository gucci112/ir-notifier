#!/usr/bin/env python3
"""
IR情報・WTI原油価格 自動通知スクリプト
毎朝 GitHub Actions で自動実行 → Resend でメール送信
"""

import os
import re
import io
import json
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
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://kabutan.jp/",
    "Accept-Language": "ja,en-US;q=0.9",
}

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


def _find_quarterly_docs(codes4: list) -> dict:
    """各4桁証券コードの最新四半期報告書（docTypeCode=120）を最大90日遡って検索"""
    found, today = {}, date.today()
    for delta in range(90):
        if len(found) == len(codes4):
            break
        d = (today - timedelta(days=delta)).strftime("%Y-%m-%d")
        for doc in _edinet_doc_list(d):
            sec = (doc.get("secCode") or "")[:4]
            if sec in codes4 and sec not in found and doc.get("docTypeCode") == "120":
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
    url = "https://www.aljazeera.com/xml/rss/all.xml"
    try:
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
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
def _extract_col_value(table, col_keywords: list) -> float | None:
    """テーブルのヘッダー行からキーワードが一致する列を探し、最新実績行の値を返す。
    kabutan.jp は「ヘッダー行 → データ行」の縦持ち構造。全角/半角どちらにも対応。"""
    rows = table.find_all("tr")
    if not rows:
        return None

    # ヘッダー行から対象列インデックスを探す
    header_cells = [c.get_text(strip=True) for c in rows[0].find_all(["th", "td"])]
    col_idx = None
    for i, h in enumerate(header_cells):
        # 全角→半角に正規化して比較
        h_norm = h.replace("Ｒ", "R").replace("Ｏ", "O").replace("Ｅ", "E")
        if any(kw in h or kw in h_norm for kw in col_keywords):
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


def get_financial_data(code: str) -> dict:
    """kabutan.jp の財務ページから ROE・自己資本比率を取得する。
    ROEは収益性テーブル（ヘッダー: ＲＯＥ）、自己資本比率は財務テーブルから取得。"""
    url = f"https://kabutan.jp/stock/finance?code={code}"
    try:
        res = requests.get(url, headers=HEADERS, timeout=15)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")

        roe          = None
        equity_ratio = None

        for table in soup.find_all("table"):
            header_text = " ".join(
                c.get_text(strip=True) for c in (table.find("tr") or []).find_all(["th", "td"])
            ) if table.find("tr") else ""

            # ROEテーブル: ヘッダーに「ＲＯＥ」または「ROE」がある
            if roe is None and ("ＲＯＥ" in header_text or "ROE" in header_text):
                roe = _extract_col_value(table, ["ＲＯＥ", "ROE"])

            # 自己資本比率テーブル: ヘッダーに「自己資本比率」がある
            if equity_ratio is None and "自己資本比率" in header_text:
                equity_ratio = _extract_col_value(table, ["自己資本比率"])

        print(f"    財務データ: ROE={roe} 自己資本比率={equity_ratio}")
        return {"roe": roe, "equity_ratio": equity_ratio}

    except Exception as e:
        print(f"    [警告] {code} の財務データ取得失敗: {e}")
        return {"roe": None, "equity_ratio": None}


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
            res = requests.get(url, headers=HEADERS, timeout=15)
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


def _get_op_profit(code: str) -> float | None:
    """kabutan.jp 業績ページから最新期（実績）の営業利益を取得（百万円単位）"""
    url = f"https://kabutan.jp/stock/finance?code={code}"
    try:
        res = requests.get(url, headers=HEADERS, timeout=10)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            if not rows:
                continue
            header = [c.get_text(strip=True) for c in rows[0].find_all(["th", "td"])]
            if "営業益" not in header:
                continue
            op_col = header.index("営業益")
            # 実績行を逆順に走査して最新の値を取得（予・前期比行を除外）
            for row in reversed(rows[1:]):
                cells = [c.get_text(strip=True) for c in row.find_all(["th", "td"])]
                if not cells or not cells[0]:
                    continue
                period = cells[0]
                if period.startswith("予") or "前期比" in period:
                    continue
                if len(cells) > op_col:
                    val = cells[op_col].replace(",", "")
                    if val and val not in ("－", ""):
                        try:
                            return float(val)
                        except ValueError:
                            pass
    except Exception as e:
        print(f"    [警告] {code} 営業利益取得失敗: {e}")
    return None


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

            # 営業利益チェック（赤字企業の除外）
            code = raw[_COL_CODE]
            op_profit = _get_op_profit(code)
            if op_profit is not None and op_profit <= 0:
                print(f"    [{code}] {name} 営業利益マイナス({op_profit:,.0f}百万円) → 除外")
                continue

            op_label = (
                f"{op_profit:+,.0f}百万円" if op_profit is not None else "確認不可"
            )

            results.append({
                "code":        code,
                "name":        name,
                "market":      market,
                "price":       price,
                "per":         per,
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
# メール本文の組み立て
# ============================================================
def build_email_body(
    stock_data: list,
    wti: dict,
    world_news: list,
    edinet_data: list,
    screened: list,
) -> str:
    today = date.today().strftime("%Y年%m月%d日")
    lines = [
        f"■ IR・株価・WTI通知 ／ {today}",
        "=" * 52,
        "",
    ]

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

    # --- WTI 原油価格 ---
    lines.append("▼ WTI 原油先物価格（ドル/バレル）")
    if wti.get("price"):
        sign = "+" if wti["change"] >= 0 else ""
        lines.append(
            f"  ${wti['price']:,.2f}  "
            f"（前日比 {sign}{wti['change']:+.2f} ／ {sign}{wti['change_pct']:+.2f}%）"
        )
    else:
        lines.append(f"  {wti.get('error', '取得失敗')}")
    lines.append("")
    lines.append("=" * 52)
    lines.append("")

    # --- バフェット指標スクリーニング サマリー ---
    lines.append(f"▼ バフェット指標スクリーニング（監視6銘柄）")
    lines.append(f"  条件: ROE {ROE_MIN:.0f}%以上 かつ 自己資本比率 {EQUITY_RATIO_MIN:.0f}%以上")
    lines.append("")
    passed_items = [d for d in stock_data if d.get("buffett_passed")]
    failed_items = [d for d in stock_data if not d.get("buffett_passed")]
    if passed_items:
        lines.append("  ✔ 通過:")
        for d in passed_items:
            f = d["buffett"]
            roe_s = f"{f['roe']}%" if f["roe"] is not None else "取得不可"
            eq_s  = f"{f['equity_ratio']}%" if f["equity_ratio"] is not None else "取得不可"
            lines.append(f"    {d['stock']['name']}（{d['stock']['code']}）  ROE:{roe_s} / 自己資本比率:{eq_s}")
    if failed_items:
        lines.append("  ✘ 不通過:")
        for d in failed_items:
            f = d["buffett"]
            roe_s = f"{f['roe']}%" if f["roe"] is not None else "取得不可"
            eq_s  = f"{f['equity_ratio']}%" if f["equity_ratio"] is not None else "取得不可"
            lines.append(f"    {d['stock']['name']}（{d['stock']['code']}）  ROE:{roe_s} / 自己資本比率:{eq_s}")
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
        lines.append("▼ 決算進捗（EDINET 四半期報告書）")
        for entry in edinet_data:
            stock = entry["stock"]
            doc   = entry.get("doc") or {}
            fin   = entry.get("financials") or {}
            q     = fin.get("quarter") or "?"
            period = (doc.get("periodEnd") or "")[:7]
            lines.append(f"  {stock['name']}（{stock['code']}）  {q} 期末:{period}")
            if entry.get("error"):
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
    lines.append("  ※ 損切ライン = 取得想定価格 -8%  /  利確ライン = +25%")
    lines.append("")
    if screened:
        for s in screened:
            mkt = "グロース" if "東Ｇ" in s["market"] else "スタンダード"
            lines.append("  ----")
            lines.append(f"  【{s['code']}】{s['name']}（{mkt}）")
            lines.append(f"  株価：{s['price']:,.0f}円  PER：{s['per']:.1f}倍  出来高：{s['volume']:,}")
            lines.append(f"  営業利益（直近通期）：{s['op_profit']}")
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
def send_email(subject: str, body: str) -> None:
    html_body = "<html><head><meta charset='UTF-8'></head><body><pre>" \
                + body.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;") \
                + "</pre></body></html>"
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
        print(f"    バフェット: {'✔ 通過' if passed else '✘ 不通過'} "
              f"ROE={buffett['roe']} 自己資本比率={buffett['equity_ratio']}")
        stock_data.append({
            "stock": stock, "buffett": buffett, "buffett_passed": passed,
            "price": price, "news": news,
        })

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
    body    = build_email_body(stock_data, wti, world_news, edinet_data, screened)

    print("メールを送信中...")
    send_email(subject, body)
    print("=== 完了 ===")


if __name__ == "__main__":
    main()
