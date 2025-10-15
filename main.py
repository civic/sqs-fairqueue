import time
from multiprocessing import Process, Queue, Event
import logging
import uuid
import sys
import json
from threading import Thread
import pandas as pd

from producer import producer_process
from consumer import consumer_process

USE_FAIR_QUEUE = False  # Fair Queueingを使う場合はTrue、通常のFIFOキューならFalse

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(processName)s - %(levelname)s - %(message)s'
)

class ProducerManager:
    """
    複数のプロデューサープロセスを管理するクラス
    """
    def __init__(self, exec_id, producer_configs: list, producing_duration: int):
        self.exec_id = exec_id
        self.configs = producer_configs
        self.duration = producing_duration
        self.stop_event = Event()
        self.stats_queue = Queue()
        self.stats = []
        self.start_time = None
        self.processes = []

    def start(self):
        logging.info("=== SQS Multi-tenant Producer 開始 ===")
        logging.info(f"実行時間: {self.duration}秒")
        logging.info(f"プロセス数: {len(self.configs)}")

        # 停止イベントと統計キュー
        self.start_time = time.time()
    
        # Producerプロセス起動
        for config in self.configs:
            p = Process(
                target=producer_process,
                args=(
                    USE_FAIR_QUEUE,
                    self.exec_id,
                    config['tenant_id'],
                    config['rate'],
                    config['batch_size'],
                    config['process_id'],
                    self.stop_event,
                    self.stats_queue
                ),
                name=f"Producer-{config['tenant_id']}-{config['process_id']}",
            )
            p.start()
            self.processes.append(p)
            logging.info(f"Producerプロセス起動: {p.name}")
        
        # Producer統計情報収集スレッド起動
        stats_collector_thread = Thread(
            target=self._stats_collector,
            args=(self.stats_queue, self.stats),
            daemon=True
        )
        stats_collector_thread.start()
    
    def wait_until_done(self):
        try:
            while True:
                if time.time() - self.start_time > self.duration:
                    self.stop_event.set()
                    break
                time.sleep(1)
        except KeyboardInterrupt:
            logging.info("Ctrl+C 受信。停止処理中...")
            self.stop_event.set()
            return

        for p in self.processes:
            p.join(timeout=5)
            if p.is_alive():
                logging.warning(f"Producerプロセス {p.name} がまだ生存中 - 強制終了")
                p.terminate()
    
    def _stats_collector(self, stats_queue, stats: list):
        while any(p.is_alive() for p in self.processes):
            time.sleep(1)
            while not stats_queue.empty():
                stats.append(stats_queue.get())
            
        logging.info("Producer統計情報収集完了")

class ConsumerManager:
    """
    複数のコンシューマープロセスを管理するクラス
    """
    def __init__(self, exec_id, consumer_configs: list):
        self.exec_id = exec_id
        self.configs = consumer_configs
        self.stop_event = Event()
        self.stats_queue = Queue()
        self.stats = []
        self.processes = []

    def start(self):
        # Consumerプロセス起動
        for config in self.configs:
            p = Process(
                target=consumer_process,
                args=(
                    self.exec_id,
                    config['process_id'],
                    self.stop_event,
                    self.stats_queue
                ),
                name=f"Consumer-{config['process_id']}",
            )
            p.start()
            self.processes.append(p)
            logging.info(f"Consumerプロセス起動: {p.name}")
        
        # Consumer統計情報収集スレッド起動
        stats_collector_thread = Thread(
            target=self._stats_collector,
            args=(self.stats_queue, self.stats),
            daemon=True
        )
        stats_collector_thread.start()
    
    def stop(self):
        self.stop_event.set()
    
    def wait_until_done(self):
        while any(p.is_alive() for p in self.processes):
            for p in self.processes:
                logging.info(f"Consumerプロセス {p.name} の完了を待機中... {len(self.stats)}")
                p.join(timeout=5)

    def _stats_collector(self, stats_queue, stats: list):
        empty_count = 0
        while empty_count < 3 and any(p.is_alive() for p in self.processes):
            time.sleep(1)
            while not stats_queue.empty():
                stats.append(stats_queue.get())
                empty_count = 0
            else:
                empty_count += 1
        
        logging.info("Consumer統計情報収集完了")

def summarize_stats(all_stats, start_time, producing_duration):
    logging.info("\n=== 実行結果 ===")
    tenant_summary = {}
    for stat in all_stats['producer']:
        tenant = stat['tenant_id']
        if tenant not in tenant_summary:
            tenant_summary[tenant] = {'sent_count': 0, 'target_rate': 0}

        tenant_summary[tenant]['sent_count'] += stat['sent_count']
        tenant_summary[tenant]['target_rate'] += stat['target_rate']

    for tenant, summary in sorted(tenant_summary.items()):
        actual_rate = summary['sent_count'] / producing_duration
        logging.info(
            f"{tenant}: 送信={summary['sent_count']}件, 目標レート={summary['target_rate']}msg/sec, 実測レート={actual_rate:.1f}msg/sec"
        )

    total_sent = sum(s['sent_count'] for s in all_stats['producer'])
    total_rate = total_sent / producing_duration

    logging.info(f"合計送信={total_sent}件, 全体実測レート={total_rate:.1f}msg/sec FairQueue={USE_FAIR_QUEUE}")

    total_receive = len(all_stats['consumer'])
    logging.info(f"合計受信={total_receive}件")

    df = pd.DataFrame(all_stats['consumer'])
    df['timestamp'] = pd.to_datetime(df['sent_timestamp'], unit='ms')

    tenant_stats = df.groupby('tenant_id')['dwell_time_ms'].agg([
        'count', 'mean', 'std', 'min', 'max',
        ('p50', lambda x: x.quantile(0.5)),
        ('p95', lambda x: x.quantile(0.95)),
        ('p99', lambda x: x.quantile(0.99)),
    ])
    print(tenant_stats)

    with open("all_stats.json", "w") as f:
        json.dump(all_stats, f, indent=2)

def wait_prodcuers_stopped(processes):
    for p in processes:
        p.join(timeout=5)
        if p.is_alive():
            logging.warning(f"Producerプロセス {p.name} がまだ生存中 - 強制終了")
            p.terminate()

def wait_consumers_stopped(processes):
    while any(p.is_alive() for p in processes):
        for p in processes:
            logging.info(f"Consumerプロセス {p.name} の完了を待機中...")
            p.join(timeout=5)

def main():
    """
    メインプロセス
    複数のプロデューサープロセスを起動・管理
    """
    # 使い方: python main.py [duration秒]
    if len(sys.argv) >= 2:
        try:
            producing_duration = int(sys.argv[1])
        except ValueError:
            logging.error("[ERROR] duration は整数で指定してください")
            sys.exit(1)
    else:
        logging.error("[ERROR] duration を指定してください")
        sys.exit(1)
    
    # プロセス設定
    
    exec_id = uuid.uuid4().hex[0:8]
    logging.info(f"実行ID={exec_id} Use FairQueue={USE_FAIR_QUEUE}")
    logging.info("=== SQS Multi-tenant Producer 開始 ===")
    logging.info(f"実行時間: {producing_duration}秒")

    # producerプロセス起動
    producer_manager = ProducerManager(exec_id, [
        # テナントA: 5プロセス × 20msg/sec = 100msg/sec
        {'tenant_id': 'tenant-A', 'rate': 20, 'process_id': "a1", "batch_size": 10},
        {'tenant_id': 'tenant-A', 'rate': 20, 'process_id': "a2", "batch_size": 10},
        {'tenant_id': 'tenant-A', 'rate': 20, 'process_id': "a3", "batch_size": 10},
        {'tenant_id': 'tenant-A', 'rate': 20, 'process_id': "a4", "batch_size": 10},
        {'tenant_id': 'tenant-A', 'rate': 20, 'process_id': "a5", "batch_size": 10},
        # テナントB: 1プロセス × 1msg/sec
        {'tenant_id': 'tenant-B', 'rate': 1, 'process_id': "b1", "batch_size": 1},
        # テナントC: 1プロセス × 1msg/sec
        {'tenant_id': 'tenant-C', 'rate': 1, 'process_id': "c1", "batch_size": 1},
    ], producing_duration)
    start_time = time.time()
    producer_manager.start()

    # Consumerプロセス起動
    consumer_manager = ConsumerManager(exec_id, [
        {'process_id': "consumer-1"},
        {'process_id': "consumer-2"},
        {'process_id': "consumer-3"},
    ])
    consumer_manager.start()

    # プロデューサーの完了待機
    producer_manager.wait_until_done()
    # プロデューサー停止後、コンシューマーに停止指示
    consumer_manager.wait_until_done()

    # 統計情報の集計
    all_stats = {  
        'producer': producer_manager.stats,
        'consumer': consumer_manager.stats
    }
    summarize_stats(all_stats, start_time, producing_duration)

if __name__ == '__main__':
    main()
