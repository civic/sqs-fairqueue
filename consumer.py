import os
import time
import json
import logging
from multiprocessing import Queue, Event
from datetime import datetime
import boto3

AWS_REGION = 'ap-northeast-1'
QUEUE_URL = os.environ.get("QUEUE_URL")


def consumer_process(
    exec_id: str,
    process_id: int | str,
    stop_event: Event,
    stats_queue: Queue
):
    """SQSメッセージを取得して削除するコンシューマープロセス。

    終了条件:
      - 連続 empty poll 3回
      - KeyboardInterrupt
    """
    logger = logging.getLogger(f"Consumer-{exec_id}-{process_id}")

    if not QUEUE_URL:
        logger.error("QUEUE_URL が未設定です。環境変数を設定してください。")
        return

    if 'SQS_ENDPOINT_URL' in os.environ:
        sqs = boto3.client('sqs', region_name=AWS_REGION, endpoint_url=os.getenv('SQS_ENDPOINT_URL'))
    else:
        sqs = boto3.client('sqs', region_name=AWS_REGION)

    start_time = time.time()
    output_time = start_time
    received_count = 0
    empty_message_times = 0
    last_received_timestamp = 0

    logger.info(f"開始: pid={os.getpid()}")

    try:
        while not stop_event.is_set():
            response = sqs.receive_message(
                QueueUrl=QUEUE_URL,
                MaxNumberOfMessages=10,
                AttributeNames=["All"],
                MessageAttributeNames=['All'],
                WaitTimeSeconds=3
            )
            messages = response.get('Messages', [])
            if not messages:
                empty_message_times += 1
                if empty_message_times >= 3:
                    break
                continue

            empty_message_times = 0  # reset after success
            for msg in messages:
                
                body = json.loads(msg.get('Body', '{}'))
                attributes = msg.get('Attributes', {})
                received_timestamp = float(attributes.get('ApproximateFirstReceiveTimestamp'))
                last_received_timestamp = received_timestamp
                sent_timestamp = float(attributes.get('SentTimestamp'))

                stats_queue.put({
                    'tenant_id': body.get('tenant_id'),
                    'process_id': process_id,
                    'message_id': msg.get('MessageId'),
                    'sent_timestamp': int(sent_timestamp),
                    'received_timestamp': int(received_timestamp),
                    'dwell_time_ms': (received_timestamp - sent_timestamp) * 1000
                })

                # ここで body を使った処理を追加可能
                try:
                    sqs.delete_message(
                        QueueUrl=QUEUE_URL,
                        ReceiptHandle=msg['ReceiptHandle']
                    )
                except Exception as e:
                    logger.exception(f"delete_message エラー: {e}")
                received_count += 1

            if time.time() - output_time > 10:
                output_time = time.time()
                logger.info(f"受信: {received_count}件")

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt 受信")
    finally:
        total_time = max(0.0001, last_received_timestamp - start_time)
        actual_rate = received_count / total_time
        logger.info(f"完了: 受信={received_count}, rate={actual_rate:.1f}msg/sec")
    