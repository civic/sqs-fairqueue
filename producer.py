import os
import time
import json
import logging
from multiprocessing import Event, Queue
import boto3

AWS_REGION = 'ap-northeast-1'
QUEUE_URL = os.environ.get("QUEUE_URL")

def producer_process(
    use_fair_queue: bool,
    exec_id: str,
    tenant_id: str,
    messages_per_sec: int,
    batch_size: int,
    process_id: int | str,
    stop_event: Event,
    stats_queue: Queue
):
    """
    SQSにメッセージを送信するプロデューサープロセス

    Args:
        exec_id: 実行毎の識別子
        tenant_id: テナントID (MessageGroupIdとして使用)
        messages_per_sec: 1秒あたりの送信メッセージ数(合計)
        batch_size: 1ループで送信するメッセージ数 (レート計算 = batch_size / messages_per_sec)
        process_id: プロセスID (ログ用)
        stop_event: 外部から停止指示されるEvent
        stats_queue: 統計情報送信用Queue
    """
    logger = logging.getLogger(f"{tenant_id}-{process_id}")

    if not QUEUE_URL:
        logger.error("QUEUE_URL が未設定です。環境変数を設定してください。")
        return

    # SQSクライアント作成 (LocalStack対応)
    if 'SQS_ENDPOINT_URL' in os.environ:
        sqs = boto3.client('sqs', region_name=AWS_REGION, endpoint_url=os.getenv('SQS_ENDPOINT_URL'))
    else:
        sqs = boto3.client('sqs', region_name=AWS_REGION)

    # 送信間隔 (秒)
    interval = batch_size / messages_per_sec

    sent_count = 0
    start_time = time.time()
    output_time = start_time

    logger.info(
        f"開始: tenant={tenant_id}, target_rate={messages_per_sec}msg/sec, batch_size={batch_size}, interval={interval:.2f}s (pid={os.getpid()})"
    )

    try:
        while not stop_event.is_set():
            loop_start = time.time()
            batch_messages = []

            for _ in range(batch_size):
                body = {
                    'exec_id': exec_id,
                    'tenant_id': tenant_id,
                    'process_id': process_id,
                    'message_number': sent_count + 1,
                }
                msg = {
                    'Id': f"msg-{exec_id}-{process_id}-{sent_count + 1}",
                    'MessageBody': json.dumps(body),
                }
                if use_fair_queue:
                    msg['MessageGroupId'] = tenant_id
                else:
                    # fair_queueを使用しない場合はMessageGroupIdは不要
                    pass

                batch_messages.append(msg)
                sent_count += 1

            try:
                sqs.send_message_batch(QueueUrl=QUEUE_URL, Entries=batch_messages)
            except Exception as e:
                logger.exception(f"send_message_batch エラー: {e}")
                time.sleep(1)
                continue

            if time.time() - output_time > 10:
                logger.info(f"送信済み: {sent_count}件 (pid={os.getpid()})")
                output_time = time.time()

            elapsed = time.time() - loop_start
            sleep_time = max(0, interval - elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt 受信")
    finally:
        total_time = time.time() - start_time
        actual_rate = sent_count / total_time if total_time > 0 else 0
        stats_queue.put({
            'tenant_id': tenant_id,
            'process_id': process_id,
            'sent_count': sent_count,
            'duration': total_time,
            'actual_rate': actual_rate,
            'target_rate': messages_per_sec
        })
        logger.info(
            f"完了: sent={sent_count}, actual_rate={actual_rate:.1f}msg/sec, target={messages_per_sec}msg/sec (pid={os.getpid()})"
        )
