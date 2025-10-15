# Fair Queue SQS 実験プロジェクト

Amazon SQS (FIFO) を LocalStack 上で用い、マルチテナント環境におけるスループット偏りと (簡易) フェアネス制御の挙動を観察するための最小構成検証コードです。大量メッセージを送信するテナントと低レートのテナントが同居した際のメッセージ滞留時間 (dwell time) 分布を収集・可視化します。

## 構成概要

- `producer.py` : SQS にメッセージを送信するプロセス。
- `consumer.py` : ポーリングし取得したメッセージを送信するプロセス。
- `main.py` : Producer / Consumer の起動・停止制御、統計集計
- `analyze.py` : 収集済み `all_stats.json` を読み込み、テナント別 dwell time 分布をヒストグラムで描画。

## 前提

- Python 3.13 以上 (pyproject 参照)

## セットアップ手順

1. 依存パッケージインストール (例: uv 使用)
	```bash
	uv sync
	```
2. 環境変数を設定:
	```bash
	export QUEUE_URL=http://localhost:4566/000000000000/test-queue
	export SQS_ENDPOINT_URL=http://localhost:4566
	```

## 実行

生産 (送信) 時間を秒で指定して起動します (例: 60 秒間):
```bash
python main.py 60
```

完了後、`all_stats.json` が生成され、標準出力にテナント別統計 (件数 / レート / dwell time 指標) が表示されます。

## 分析 (可視化)

`all_stats.json` をヒストグラムで可視化:
```bash
python analyze.py all_stats.json
```
テナント A (高レート) と テナント B+C (低レート群) の dwell time 分布形状を比較します。

## パラメータ調整ポイント

- `main.py` 内 `ProducerManager` への設定リストで各テナントのレート (`rate`) とバッチサイズ (`batch_size`) を変更。
- フェアネス挙動の仮実験: `main.py` 冒頭の `USE_FAIR_QUEUE` を `True` に切り替え、`producer.py` が `MessageGroupId` を付与する形で FIFO 内グルーピングを利用。
- Consumer プロセス数 (`ConsumerManager` 設定) を増減し取り込み能力を変更。

## 生成される統計

- Producer 側: 送信件数、ターゲットレート、実測レート
- Consumer 側: メッセージごとの
  - `tenant_id`
  - `sent_timestamp` / `received_timestamp`
  - `dwell_time_ms` (SQS 送信→最初の受信まで)
- 集計: テナント別 count / mean / std / min / max / p50 / p95 / p99


