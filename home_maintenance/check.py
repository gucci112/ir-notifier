#!/usr/bin/env python3
"""
home_maintenance/check.py
ホームメンテナンス通知スクリプト
- 期限7日前 / 当日 / 超過(warn_days, alert_days)に該当する品目があればメール送信
- メールには全品目の状況一覧を表示
"""

import json
import os
import sys
from datetime import date, timedelta
from pathlib import Path
import urllib.request
import urllib.error

# ── パス設定 ──────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
ITEMS_FILE = BASE_DIR / "items.json"
HISTORY_FILE = BASE_DIR / "history.json"

# ── 環境変数 ──────────────────────────────────────────────
RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
NOTIFY_EMAIL   = os.environ.get("NOTIFY_EMAIL", "")
FROM_EMAIL     = os.environ.get("FROM_EMAIL", "home@resend.dev")
GITHUB_OWNER   = os.environ.get("GITHUB_OWNER", "")
GITHUB_REPO    = os.environ.get("GITHUB_REPO", "ir-notifier")
PAGES_URL      = os.environ.get("PAGES_URL", "")  # GitHub PagesのURL

# ── ステータス定義 ─────────────────────────────────────────
STATUS_OK      = "ok"        # 余裕あり
STATUS_SOON    = "soon"      # 7日以内
STATUS_TODAY   = "today"     # 当日
STATUS_WARN    = "warn"      # warn_days超過（黄色）
STATUS_ALERT   = "alert"     # alert_days超過（赤）


def load_json(path: Path) -> dict | list:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def get_status(days_to_next: int, days_overdue: int, item: dict) -> str:
    """品目のステータスを返す"""
    if days_overdue >= item["alert_days"]:
        return STATUS_ALERT
    if days_overdue >= item["warn_days"]:
        return STATUS_WARN
    if days_overdue > 0:
        return STATUS_WARN  # 超過してるがwarn未満
    if days_to_next == 0:
        return STATUS_TODAY
    if days_to_next <= item["notify_before_days"]:
        return STATUS_SOON
    return STATUS_OK


def build_item_rows(items: list, history: dict, today: date) -> list[dict]:
    """全品目の状況リストを生成"""
    rows = []
    for item in items:
        item_id = item["id"]
        hist = history.get(item_id, {})
        last_replaced_str = hist.get("last_replaced", "")
        memo = hist.get("memo", "")

        if last_replaced_str:
            last_replaced = date.fromisoformat(last_replaced_str)
            next_date = last_replaced + timedelta(days=item["interval_days"])
            days_to_next = (next_date - today).days
            days_overdue = max(0, -days_to_next)
        else:
            # 未設定の場合は即アラート
            last_replaced = None
            next_date = None
            days_to_next = -999
            days_overdue = 999

        status = get_status(days_to_next, days_overdue, item)

        rows.append({
            "id": item_id,
            "name": item["name"],
            "note": item.get("note", ""),
            "last_replaced": last_replaced_str or "未設定",
            "next_date": next_date.isoformat() if next_date else "未設定",
            "days_to_next": days_to_next,
            "days_overdue": days_overdue,
            "status": status,
            "memo": memo,
            "interval_days": item["interval_days"],
        })

    return rows


def should_notify(rows: list[dict]) -> bool:
    """通知が必要かどうか（要対応品目が1つ以上あればTrue）"""
    notify_statuses = {STATUS_TODAY, STATUS_SOON, STATUS_WARN, STATUS_ALERT}
    return any(r["status"] in notify_statuses for r in rows)


def status_label(status: str, days_to_next: int, days_overdue: int) -> str:
    if status == STATUS_ALERT:
        return f"🔴 {days_overdue}日超過"
    if status == STATUS_WARN:
        return f"🟡 {days_overdue}日超過"
    if status == STATUS_TODAY:
        return "📅 本日交換日"
    if status == STATUS_SOON:
        return f"⏰ あと{days_to_next}日"
    return f"✅ あと{days_to_next}日"


def status_bg_color(status: str) -> str:
    return {
        STATUS_ALERT: "#fff0f0",
        STATUS_WARN:  "#fffbe6",
        STATUS_TODAY: "#e8f4fd",
        STATUS_SOON:  "#f0f7ff",
        STATUS_OK:    "#f9f9f9",
    }.get(status, "#f9f9f9")


def status_badge_color(status: str) -> tuple[str, str]:
    """(背景色, 文字色)"""
    return {
        STATUS_ALERT: ("#e53e3e", "#ffffff"),
        STATUS_WARN:  ("#d69e2e", "#ffffff"),
        STATUS_TODAY: ("#3182ce", "#ffffff"),
        STATUS_SOON:  ("#805ad5", "#ffffff"),
        STATUS_OK:    ("#38a169", "#ffffff"),
    }.get(status, ("#718096", "#ffffff"))


def build_html_email(rows: list[dict], today: date) -> str:
    """HTMLメール本文を生成"""

    # 品目行のHTML
    def item_row_html(r: dict) -> str:
        bg = status_bg_color(r["status"])
        badge_bg, badge_fg = status_badge_color(r["status"])
        label = status_label(r["status"], r["days_to_next"], r["days_overdue"])
        update_url = f"{PAGES_URL}?id={r['id']}" if PAGES_URL else "#"

        memo_html = f'<div style="color:#888;font-size:12px;margin-top:4px;">{r["memo"]}</div>' if r["memo"] else ""

        return f"""
        <tr>
          <td style="padding:14px 16px;background:{bg};border-bottom:1px solid #e8e8e8;">
            <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;">
              <div>
                <div style="font-weight:600;font-size:15px;color:#1a1a1a;">{r["name"]}</div>
                <div style="font-size:12px;color:#888;margin-top:2px;">{r["note"]} ／ 最終交換：{r["last_replaced"]} ／ 次回：{r["next_date"]}</div>
                {memo_html}
              </div>
              <div style="display:flex;align-items:center;gap:10px;flex-shrink:0;">
                <span style="background:{badge_bg};color:{badge_fg};padding:4px 10px;border-radius:12px;font-size:13px;font-weight:600;white-space:nowrap;">{label}</span>
                <a href="{update_url}" style="background:#2d3748;color:#fff;padding:6px 12px;border-radius:6px;font-size:12px;text-decoration:none;white-space:nowrap;">交換を記録</a>
              </div>
            </div>
          </td>
        </tr>"""

    rows_html = "\n".join(item_row_html(r) for r in rows)

    # 要対応品目のサマリー
    alert_count = sum(1 for r in rows if r["status"] == STATUS_ALERT)
    warn_count  = sum(1 for r in rows if r["status"] == STATUS_WARN)
    today_count = sum(1 for r in rows if r["status"] == STATUS_TODAY)
    soon_count  = sum(1 for r in rows if r["status"] == STATUS_SOON)

    summary_parts = []
    if alert_count: summary_parts.append(f'<span style="color:#e53e3e;font-weight:700;">🔴 要交換 {alert_count}件</span>')
    if warn_count:  summary_parts.append(f'<span style="color:#d69e2e;font-weight:700;">🟡 超過中 {warn_count}件</span>')
    if today_count: summary_parts.append(f'<span style="color:#3182ce;font-weight:700;">📅 本日交換日 {today_count}件</span>')
    if soon_count:  summary_parts.append(f'<span style="color:#805ad5;font-weight:700;">⏰ 7日以内 {soon_count}件</span>')
    summary_html = "　".join(summary_parts) if summary_parts else '<span style="color:#38a169;">✅ 全品目正常</span>'

    update_all_url = PAGES_URL if PAGES_URL else "#"

    return f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ホームメンテナンス通知</title>
</head>
<body style="margin:0;padding:0;background:#f0f2f5;font-family:-apple-system,BlinkMacSystemFont,'Hiragino Sans',sans-serif;">
  <div style="max-width:600px;margin:20px auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.08);">

    <!-- ヘッダー -->
    <div style="background:linear-gradient(135deg,#2d3748,#4a5568);padding:24px 20px;text-align:center;">
      <div style="font-size:28px;margin-bottom:6px;">🏠</div>
      <div style="color:#fff;font-size:18px;font-weight:700;">ホームメンテナンス</div>
      <div style="color:#a0aec0;font-size:13px;margin-top:4px;">{today.strftime("%Y年%m月%d日")} 時点</div>
    </div>

    <!-- サマリー -->
    <div style="padding:16px 20px;background:#f7fafc;border-bottom:1px solid #e8e8e8;text-align:center;font-size:14px;">
      {summary_html}
    </div>

    <!-- 品目一覧 -->
    <table style="width:100%;border-collapse:collapse;">
      {rows_html}
    </table>

    <!-- フッター -->
    <div style="padding:16px 20px;text-align:center;border-top:1px solid #e8e8e8;">
      <a href="{update_all_url}" style="display:inline-block;background:#4a5568;color:#fff;padding:10px 24px;border-radius:8px;font-size:14px;text-decoration:none;font-weight:600;">📋 交換記録ページを開く</a>
      <div style="color:#aaa;font-size:11px;margin-top:12px;">ir-notifier / home-maintenance</div>
    </div>

  </div>
</body>
</html>"""


def send_email(subject: str, html_body: str) -> bool:
    """Resend APIでメール送信"""
    if not RESEND_API_KEY or not NOTIFY_EMAIL:
        print("ERROR: RESEND_API_KEY または NOTIFY_EMAIL が未設定")
        return False

    payload = json.dumps({
        "from": FROM_EMAIL,
        "to": [NOTIFY_EMAIL],
        "subject": subject,
        "html": html_body,
    }).encode("utf-8")

    req = urllib.request.Request(
        "https://api.resend.com/emails",
        data=payload,
        headers={
            "Authorization": f"Bearer {RESEND_API_KEY}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req) as res:
            result = json.loads(res.read())
            print(f"メール送信成功: {result.get('id')}")
            return True
    except urllib.error.HTTPError as e:
        print(f"メール送信失敗: {e.code} {e.read().decode()}")
        return False


def main():
    today = date.today()
    print(f"チェック日: {today}")

    items   = load_json(ITEMS_FILE)
    history = load_json(HISTORY_FILE)

    rows = build_item_rows(items, history, today)

    # ステータス表示（ログ）
    for r in rows:
        print(f"  {r['name']}: {r['status']} (次回:{r['next_date']})")

    if not should_notify(rows):
        print("通知不要 - 全品目余裕あり")
        sys.exit(0)

    # 要対応品目数をカウント
    urgent = [r for r in rows if r["status"] in {STATUS_TODAY, STATUS_SOON, STATUS_WARN, STATUS_ALERT}]
    subject = f"🏠 ホームメンテナンス通知 - {len(urgent)}件の対応が必要 ({today.strftime('%m/%d')})"

    html_body = build_html_email(rows, today)
    success = send_email(subject, html_body)

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
