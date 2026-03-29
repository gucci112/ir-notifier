#!/usr/bin/env python3
"""
IR情報・WTI原油価格 自動通知スクリプト
毎朝 GitHub Actions で自動実行 → Gmail でメール送信
"""

import os
import smtplib
import requests
import yfinance as yf
from datetime import date
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from bs4 import BeautifulSoup

# ============================================================
# 設定（GitHub Secrets から自動的に読み込まれます）
# ============================================================
EMAIL_FROM     = os.environ["EMAIL_FROM"]       # 送信元 Gmail アドレス
EMAIL_TO       = os.environ["EMAIL_TO"]         # 受信先メールアドレス
EMAIL_PASSWORD = os.environ["EMAIL_PASSWORD"]   # Gmail アプリパスワード

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
    )
}


# ============================================================
# IR・ニュース取得（kabutan.jp）
# ============================================================
def get_stock_news(code: str, max_items: int = 5) -> list:
    """kabutan.jp の IR・ニュースページから最新情報を取得する"""
    results = []

    # IRニュース → 一般ニュースの順に試みる
    for path in [f"/stock/irnews?code={code}", f"/stock/news?code={code}"]:
        url = f"https://kabutan.jp{path}"
        try:
            res = requests.get(url, headers=HEADERS, timeout=15)
            res.raise_for_status()
            soup = BeautifulSoup(res.text, "html.parser")

            # テーブル行を探す（複数のセレクタに対応）
            rows = soup.select(
                "table.s-news-list tr, "
                "table.news_list tr, "
                "#newslist table tr"
            )

            for row in rows:
                tds = row.find_all("td")
                if len(tds) < 2:
                    continue

                date_text = tds[0].get_text(strip=True)
                # ヘッダー行や空行はスキップ
                if not date_text or date_text in ("日付", "日時", ""):
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
# 株価取得（yfinance）
# ============================================================
def get_stock_price(code: str) -> dict:
    """yfinance で現在株価・前日比を取得する（東証: コード.T）"""
    # キオクシア（285A）のような英数混在コードにも対応
    ticker_symbol = f"{code}.T"
    try:
        ticker = yf.Ticker(ticker_symbol)
        info = ticker.fast_info
        price      = info.last_price
        prev_close = info.previous_close
        if price and prev_close:
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
# WTI 原油先物価格取得（yfinance）
# ============================================================
def get_wti_price() -> dict:
    """WTI 原油先物（CL=F）の直近価格を取得する"""
    try:
        ticker = yf.Ticker("CL=F")
        info   = ticker.fast_info
        price      = info.last_price
        prev_close = info.previous_close
        if price and prev_close:
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
# Gmail 送信
# ============================================================
def send_email(subject: str, body: str) -> None:
    msg = MIMEMultipart()
    msg["From"]    = EMAIL_FROM
    msg["To"]      = EMAIL_TO
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain", "utf-8"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(EMAIL_FROM, EMAIL_PASSWORD)
        server.sendmail(EMAIL_FROM, EMAIL_TO, msg.as_string())
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
