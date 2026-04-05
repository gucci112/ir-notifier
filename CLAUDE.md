# ir-notifier CLAUDE.md

## プロジェクト概要
株式分析・メール通知の自動化システム
GitHub: gucci112/ir-notifier

## 監視銘柄
- 6055 Japan Material
- 4890 Tsubota Lab
- 1951 Exeo Group
- 1980 Daidan
- 285A Kioxia
- 6845 Azbil

## 次のタスク（優先順）
1. ROIC閾値を≥15%（税前ベース）に修正
2. PEG/EV/EBITDAをスクリーニング出力に追加
3. netCashRatio（清原式）のスクリーニング追加
4. edinetdb.jp方式との整合性確認
5. 関税・地政学リスク自動アラート機能

## 設計方針
- メールはモバイルフレンドリーなフォーマット
- 株価取得はYahoo Finance API（yfinanceは使わない）
- メール送信はResend（Gmail SMTPは使わない）
- GitHub Actionsのcronは UTC 22:00（JST 7:00）

## 証券口座
- 大和コネクト証券（月次無料クーポン制）