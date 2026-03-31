#!/usr/bin/env python3
"""
IR情報・WTI原油価格 自動通知スクリプト
毎朝 GitHub Actions で自動実行 → Resend でメール送信
"""

import os
import json
import resend
import requests
from datetime import date
from bs4 import BeautifulSoup

# ============================================================
# 設定（GitHub Secrets から自動的に読み込まれます）
# ============================================================
resend.api_key     = os.environ["RESEND_API_KEY"]  # Resend API キー
EMAIL_FROM         = os.environ["EMAIL_FROM"]       # 送信元アドレス
EMAIL_TO           = os.environ["EMAIL_TO"]         # 受信先メールアドレス

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
# メール本文の組み立て
# ============================================================
def build_email_body(
    stock_data: list,
    wti: dict,
) -> str:
    today = date.today().strftime("%Y年%m月%d日")
    lines = [
        f"■ IR・株価・WTI通知 ／ {today}",
        "=" * 52,
        "",
    ]

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

    # --- 各銘柄 ---
    for item in stock_data:
        stock  = item["stock"]
        price  = item["price"]
        news   = item["news"]

        # 株価
        price_str = ""
        if price.get("price"):
            sign = "+" if price["change"] >= 0 else ""
            price_str = (
                f"  現在値: ¥{price['price']:,.0f}  "
                f"（{sign}{price['change']:+.0f} ／ {sign}{price['change_pct']:+.2f}%）"
            )
        else:
            price_str = "  株価: 取得できませんでした"

        lines.append(f"▼ {stock['name']}（{stock['code']}）")
        lines.append(price_str)
        lines.append("")

        # ニュース・IR
        lines.append("  【最新IR・ニュース】")
        for n in news:
            lines.append(f"  {n['date']}  {n['title']}")
            if n["url"]:
                lines.append(f"           {n['url']}")
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
        price = get_stock_price(stock["code"])
        news  = get_stock_news(stock["code"])
        stock_data.append({"stock": stock, "price": price, "news": news})

    # WTI 価格取得
    print("  WTI 原油価格を取得中...")
    wti = get_wti_price()

    # メール組み立て・送信
    today   = date.today().strftime("%Y/%m/%d")
    subject = f"[IR通知] {today} 銘柄ニュース・WTI価格"
    body    = build_email_body(stock_data, wti)

    print("メールを送信中...")
    send_email(subject, body)
    print("=== 完了 ===")


if __name__ == "__main__":
    main()
