import json
import sys
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

def analyze_stats(stats_file: str):
    with open(stats_file, "r") as f:
        all_stats = json.load(f)
    
    df = pd.DataFrame(all_stats['consumer'])
    df['timestamp'] = pd.to_datetime(df['sent_timestamp'], unit='ms')

    tenant_a_df = df[df['tenant_id'] == 'tenant-A']
    tenant_a_p99_threshold = tenant_a_df['dwell_time_ms'].quantile(0.50)
    tenant_a_data = tenant_a_df[tenant_a_df['dwell_time_ms'] <= tenant_a_p99_threshold]['dwell_time_ms']
    #tenant_a_data = tenant_a_df['dwell_time_ms']

    tenant_bc_df = df[df['tenant_id'].isin(['tenant-B', 'tenant-C'])]
    tenant_bc_p99_threshold = tenant_bc_df['dwell_time_ms'].quantile(0.50) if not tenant_bc_df.empty else None
    tenant_bc_data = tenant_bc_df[tenant_bc_df['dwell_time_ms'] <= tenant_bc_p99_threshold]['dwell_time_ms']
    #tenant_bc_data = tenant_bc_df['dwell_time_ms']

    plt.figure(figsize=(8,5))
    plt.hist(tenant_a_data, bins=50, alpha=0.4, label='Tenant A', color='blue', edgecolor='black', density=True)
    plt.hist(tenant_bc_data, bins=50, alpha=0.4, label='Tenant B+C', color='red', edgecolor='black', density=True)
    plt.xlabel('Dwell Time (ms)')
    plt.ylabel('Density')
    plt.title('Normalized Dwell Time Distribution (Shape Comparison)')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python analyze.py all_stats.log")
        sys.exit(1)
    analyze_stats(sys.argv[1])
