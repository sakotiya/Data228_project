#!/usr/bin/env python3
"""
Bloom Filter Streaming Analysis Visualizations
Shows the complete pipeline: Raw → Clean → Bloom Filter → Streaming Dedup
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os
from datetime import datetime

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)
plt.rcParams['font.size'] = 10

# Output directory
output_dir = "/Users/gouravdhama/Documents/bubu/big_data/git/bloom_streaming_plots"
os.makedirs(output_dir, exist_ok=True)

# ============================================================================
# DATA PREPARATION - Streaming Processing Results
# ============================================================================

# Batch-by-batch streaming results (from your deduplication run)
streaming_data = {
    'Batch': [1, 2, 3, 4, 5, 6, 7, 8],
    'File': ['fhv_2025-03', 'fhv_2025-04', 'fhvhv_2025-01', 'fhvhv_2025-03',
             'green_2025-05', 'green_2025-06', 'yellow_2025-05', 'yellow_2025-06'],
    'Total_Rows': [2182992, 1699478, 20405666, 20536879, 54441, 48352, 4242250, 4024343],
    'Unique_Rows': [2161008, 1682322, 20198108, 20327220, 53862, 47868, 4199381, 3983395],
    'Duplicates': [21984, 17156, 207558, 209659, 579, 484, 42869, 40948],
    'Type': ['FHV', 'FHV', 'FHVHV', 'FHVHV', 'Green', 'Green', 'Yellow', 'Yellow']
}

df_stream = pd.DataFrame(streaming_data)
df_stream['Dup_Rate'] = (df_stream['Duplicates'] / df_stream['Total_Rows']) * 100
df_stream['Unique_Rate'] = (df_stream['Unique_Rows'] / df_stream['Total_Rows']) * 100

# Calculate cumulative metrics for streaming visualization
df_stream['Cumulative_Total'] = df_stream['Total_Rows'].cumsum()
df_stream['Cumulative_Unique'] = df_stream['Unique_Rows'].cumsum()
df_stream['Cumulative_Dups'] = df_stream['Duplicates'].cumsum()

# Bloom filter capacity and growth
bloom_capacity = 250_000_000
bloom_initial = 250_000_001
df_stream['Bloom_Elements'] = bloom_initial  # At capacity, no growth
df_stream['Fill_Ratio'] = (df_stream['Bloom_Elements'] / bloom_capacity) * 100

# Historical data summary (from your project)
historical_data = {
    'Year_Range': ['2009-2013', '2014-2018', '2019-2023', '2024'],
    'Total_Trips': [50_000_000, 85_000_000, 95_000_000, 20_000_000],
    'Type': ['Yellow', 'Yellow+Green', 'All Types', 'All Types']
}
df_historical = pd.DataFrame(historical_data)

# ============================================================================
# PLOT 1: STREAMING DEDUPLICATION FLOW (Batch-by-Batch)
# ============================================================================

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 10))

# Top panel: Rows processed per batch
x = df_stream['Batch']
width = 0.35

bars1 = ax1.bar(x - width/2, df_stream['Unique_Rows'], width,
                label='Unique (Kept)', color='#2ecc71', alpha=0.8, edgecolor='black')
bars2 = ax1.bar(x + width/2, df_stream['Duplicates'], width,
                label='Duplicates (Removed)', color='#e74c3c', alpha=0.8, edgecolor='black')

ax1.set_xlabel('Stream Batch Number', fontsize=12, fontweight='bold')
ax1.set_ylabel('Records Processed', fontsize=12, fontweight='bold')
ax1.set_title('Real-Time Streaming Deduplication: Batch-by-Batch Processing\n(Each batch checked against historical Bloom filter)',
              fontsize=14, fontweight='bold', pad=20)
ax1.set_xticks(x)
ax1.legend(fontsize=11, loc='upper right')
ax1.yaxis.grid(True, alpha=0.3)

# Add file labels
for i, (batch, file, dup_rate) in enumerate(zip(df_stream['Batch'], df_stream['File'], df_stream['Dup_Rate'])):
    ax1.text(batch, -1_000_000, file, ha='center', fontsize=8, rotation=45)

# Bottom panel: Duplicate rate trend
ax2.plot(df_stream['Batch'], df_stream['Dup_Rate'], marker='o', linewidth=3,
         markersize=10, color='#e74c3c', label='Duplicate Rate')
ax2.plot(df_stream['Batch'], df_stream['Unique_Rate'], marker='s', linewidth=3,
         markersize=10, color='#2ecc71', label='Unique Rate')

ax2.set_xlabel('Stream Batch Number', fontsize=12, fontweight='bold')
ax2.set_ylabel('Percentage (%)', fontsize=12, fontweight='bold')
ax2.set_title('Duplicate Detection Rate Over Streaming Batches', fontsize=13, fontweight='bold', pad=15)
ax2.legend(fontsize=11)
ax2.grid(True, alpha=0.3)
ax2.set_xticks(x)
ax2.set_ylim(0, 105)

# Add average line
avg_dup = df_stream['Dup_Rate'].mean()
ax2.axhline(y=avg_dup, color='red', linestyle='--', linewidth=2, alpha=0.5,
            label=f'Avg Dup: {avg_dup:.2f}%')

plt.tight_layout()
plt.savefig(f"{output_dir}/1_streaming_batch_processing.png", dpi=300, bbox_inches='tight')
print(f"✅ Saved: 1_streaming_batch_processing.png")
plt.close()

# ============================================================================
# PLOT 2: CUMULATIVE STREAMING PROGRESS
# ============================================================================

fig, ax = plt.subplots(figsize=(16, 8))

# Cumulative lines
ax.plot(df_stream['Batch'], df_stream['Cumulative_Total']/1e6, marker='o',
        linewidth=3, markersize=10, color='#3498db', label='Total Processed')
ax.plot(df_stream['Batch'], df_stream['Cumulative_Unique']/1e6, marker='s',
        linewidth=3, markersize=10, color='#2ecc71', label='Unique Kept')
ax.plot(df_stream['Batch'], df_stream['Cumulative_Dups']/1e6, marker='^',
        linewidth=3, markersize=10, color='#e74c3c', label='Duplicates Removed')

# Fill area between unique and total
ax.fill_between(df_stream['Batch'], df_stream['Cumulative_Total']/1e6,
                df_stream['Cumulative_Unique']/1e6, alpha=0.2, color='#e74c3c')

ax.set_xlabel('Stream Batch Number', fontsize=12, fontweight='bold')
ax.set_ylabel('Cumulative Records (Millions)', fontsize=12, fontweight='bold')
ax.set_title('Cumulative Streaming Deduplication Progress\n(Running total as batches arrive)',
             fontsize=14, fontweight='bold', pad=20)
ax.legend(fontsize=12, loc='upper left')
ax.grid(True, alpha=0.3)
ax.set_xticks(df_stream['Batch'])

# Add final stats annotation
final_total = df_stream['Cumulative_Total'].iloc[-1]
final_unique = df_stream['Cumulative_Unique'].iloc[-1]
final_dups = df_stream['Cumulative_Dups'].iloc[-1]
stats_text = f"Final Stats:\nTotal: {final_total:,}\nUnique: {final_unique:,}\nDups: {final_dups:,} ({final_dups/final_total*100:.2f}%)"

ax.text(0.98, 0.5, stats_text, transform=ax.transAxes,
        fontsize=11, verticalalignment='center', horizontalalignment='right',
        bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))

plt.tight_layout()
plt.savefig(f"{output_dir}/2_cumulative_streaming_progress.png", dpi=300, bbox_inches='tight')
print(f"✅ Saved: 2_cumulative_streaming_progress.png")
plt.close()

# ============================================================================
# PLOT 3: BLOOM FILTER FILL RATIO OVER TIME
# ============================================================================

fig, ax = plt.subplots(figsize=(14, 8))

# Since bloom filter was at capacity, show it as constant
ax.plot(df_stream['Batch'], df_stream['Fill_Ratio'], marker='o',
        linewidth=4, markersize=12, color='#9b59b6', label='Bloom Filter Fill Ratio')
ax.axhline(y=100, color='red', linestyle='--', linewidth=2,
           label='Capacity Limit', alpha=0.7)

ax.set_xlabel('Stream Batch Number', fontsize=12, fontweight='bold')
ax.set_ylabel('Fill Ratio (%)', fontsize=12, fontweight='bold')
ax.set_title('Bloom Filter Capacity Usage During Streaming\n(Historical filter at 100% - no new additions)',
             fontsize=14, fontweight='bold', pad=20)
ax.legend(fontsize=12)
ax.grid(True, alpha=0.3)
ax.set_xticks(df_stream['Batch'])
ax.set_ylim(95, 105)

# Add capacity info box
info_text = f"Bloom Filter Stats:\nCapacity: {bloom_capacity:,}\nElements: {bloom_initial:,}\nStatus: At Capacity"
ax.text(0.02, 0.5, info_text, transform=ax.transAxes,
        fontsize=11, verticalalignment='center',
        bbox=dict(boxstyle='round', facecolor='#ecf0f1', alpha=0.9))

plt.tight_layout()
plt.savefig(f"{output_dir}/3_bloom_filter_fill_ratio.png", dpi=300, bbox_inches='tight')
print(f"✅ Saved: 3_bloom_filter_fill_ratio.png")
plt.close()

# ============================================================================
# PLOT 4: DUPLICATE RATE BY TRIP TYPE (Streaming Analysis)
# ============================================================================

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7))

# Left: Average duplicate rate by type
type_stats = df_stream.groupby('Type').agg({
    'Duplicates': 'sum',
    'Total_Rows': 'sum'
}).reset_index()
type_stats['Avg_Dup_Rate'] = (type_stats['Duplicates'] / type_stats['Total_Rows']) * 100

colors_type = {'FHV': '#3498db', 'FHVHV': '#e67e22', 'Green': '#27ae60', 'Yellow': '#f1c40f'}
bar_colors = [colors_type[t] for t in type_stats['Type']]

bars = ax1.bar(type_stats['Type'], type_stats['Avg_Dup_Rate'],
               color=bar_colors, alpha=0.8, edgecolor='black', linewidth=2)
ax1.set_ylabel('Duplicate Rate (%)', fontsize=12, fontweight='bold')
ax1.set_xlabel('Trip Type', fontsize=12, fontweight='bold')
ax1.set_title('Average Duplicate Rate by Trip Type\n(2025 Streaming Data)',
              fontsize=13, fontweight='bold', pad=15)
ax1.yaxis.grid(True, alpha=0.3)

for bar, rate in zip(bars, type_stats['Avg_Dup_Rate']):
    height = bar.get_height()
    ax1.text(bar.get_x() + bar.get_width()/2., height,
             f'{rate:.2f}%', ha='center', va='bottom',
             fontsize=11, fontweight='bold')

# Right: Volume by type
bars2 = ax2.bar(type_stats['Type'], type_stats['Total_Rows']/1e6,
                color=bar_colors, alpha=0.8, edgecolor='black', linewidth=2)
ax2.set_ylabel('Total Records (Millions)', fontsize=12, fontweight='bold')
ax2.set_xlabel('Trip Type', fontsize=12, fontweight='bold')
ax2.set_title('Streaming Volume by Trip Type\n(2025 Data)',
              fontsize=13, fontweight='bold', pad=15)
ax2.yaxis.grid(True, alpha=0.3)

for bar, total in zip(bars2, type_stats['Total_Rows']):
    height = bar.get_height()
    ax2.text(bar.get_x() + bar.get_width()/2., height,
             f'{total/1e6:.1f}M', ha='center', va='bottom',
             fontsize=11, fontweight='bold')

plt.tight_layout()
plt.savefig(f"{output_dir}/4_trip_type_analysis.png", dpi=300, bbox_inches='tight')
print(f"✅ Saved: 4_trip_type_analysis.png")
plt.close()

# ============================================================================
# PLOT 5: END-TO-END PIPELINE FLOW
# ============================================================================

fig, ax = plt.subplots(figsize=(16, 10))
ax.set_xlim(0, 10)
ax.set_ylim(0, 10)
ax.axis('off')

# Title
ax.text(5, 9.7, 'End-to-End Bloom Filter Streaming Pipeline',
        ha='center', fontsize=20, fontweight='bold')

# Stage 1: Raw Data
rect1 = plt.Rectangle((0.3, 7.5), 1.8, 1.2, facecolor='#e74c3c',
                       edgecolor='black', linewidth=2, alpha=0.7)
ax.add_patch(rect1)
ax.text(1.2, 8.1, 'RAW DATA\n(2009-2025)\n250M+ trips',
        ha='center', va='center', fontsize=9, fontweight='bold', color='white')

# Stage 2: Data Cleaning
rect2 = plt.Rectangle((2.6, 7.5), 1.8, 1.2, facecolor='#3498db',
                       edgecolor='black', linewidth=2, alpha=0.7)
ax.add_patch(rect2)
ax.text(3.5, 8.1, 'DATA\nCLEANING\n(ETL Pipeline)',
        ha='center', va='center', fontsize=9, fontweight='bold', color='white')

# Stage 3: Historical Bloom Filter
rect3 = plt.Rectangle((4.9, 7.5), 1.8, 1.2, facecolor='#2ecc71',
                       edgecolor='black', linewidth=2, alpha=0.7)
ax.add_patch(rect3)
ax.text(5.8, 8.1, 'BLOOM FILTER\n(Historical)\n250M elements',
        ha='center', va='center', fontsize=9, fontweight='bold', color='white')

# Stage 4: Streaming Data
rect4 = plt.Rectangle((7.2, 7.5), 1.8, 1.2, facecolor='#e67e22',
                       edgecolor='black', linewidth=2, alpha=0.7)
ax.add_patch(rect4)
ax.text(8.1, 8.1, '2025 STREAM\n(8 Batches)\n53M trips',
        ha='center', va='center', fontsize=9, fontweight='bold', color='white')

# Arrows between stages
for x_start, x_end in [(2.1, 2.5), (4.4, 4.8), (6.7, 7.1)]:
    ax.arrow(x_start, 8.1, x_end-x_start-0.05, 0, head_width=0.15, head_length=0.1,
             fc='black', ec='black', linewidth=2)

# Processing box
rect5 = plt.Rectangle((2.5, 5), 5, 1.5, facecolor='#9b59b6',
                       edgecolor='black', linewidth=2, alpha=0.7)
ax.add_patch(rect5)
ax.text(5, 5.75, 'STREAMING DEDUPLICATION ENGINE\nBatch Processing: Check each trip_id against Bloom Filter',
        ha='center', va='center', fontsize=11, fontweight='bold', color='white')

# Arrows to processing
ax.arrow(5.8, 7.5, 0, -0.8, head_width=0.2, head_length=0.1,
         fc='#2ecc71', ec='#2ecc71', linewidth=3)
ax.arrow(8.1, 7.5, -2.1, -1.2, head_width=0.2, head_length=0.1,
         fc='#e67e22', ec='#e67e22', linewidth=3)

# Output boxes
rect6 = plt.Rectangle((1.5, 2.5), 2.5, 1.2, facecolor='#2ecc71',
                       edgecolor='black', linewidth=2, alpha=0.7)
ax.add_patch(rect6)
ax.text(2.75, 3.1, 'UNIQUE DATA\n52.7M trips\n(99.0%)',
        ha='center', va='center', fontsize=10, fontweight='bold', color='white')

rect7 = plt.Rectangle((5.5, 2.5), 2.5, 1.2, facecolor='#e74c3c',
                       edgecolor='black', linewidth=2, alpha=0.7)
ax.add_patch(rect7)
ax.text(6.75, 3.1, 'DUPLICATES\n541K trips\n(1.0%)',
        ha='center', va='center', fontsize=10, fontweight='bold', color='white')

# Arrows to outputs
ax.arrow(3.5, 5, -0.5, -1.2, head_width=0.2, head_length=0.1,
         fc='#2ecc71', ec='#2ecc71', linewidth=3)
ax.arrow(6, 5, 0.5, -1.2, head_width=0.2, head_length=0.1,
         fc='#e74c3c', ec='#e74c3c', linewidth=3)

# Storage/Analytics box
rect8 = plt.Rectangle((1, 0.5), 7, 1.2, facecolor='#34495e',
                       edgecolor='black', linewidth=2, alpha=0.7)
ax.add_patch(rect8)
ax.text(4.5, 1.1, 'STORAGE & ANALYTICS\nS3 Parquet | Redshift | Visualization (Tableau/QuickSight)',
        ha='center', va='center', fontsize=11, fontweight='bold', color='white')

# Arrow to storage
ax.arrow(2.75, 2.5, 0.5, -0.7, head_width=0.2, head_length=0.1,
         fc='black', ec='black', linewidth=2)

# Performance metrics
metrics_text = '⚡ Performance: 251K rows/sec | 💾 Memory: 286MB | ⏱️ Time: 3.5 min | 📊 Space Savings: 97%'
ax.text(5, 0.1, metrics_text, ha='center', va='center',
        fontsize=10, fontweight='bold', color='#2c3e50',
        bbox=dict(boxstyle='round', facecolor='#ecf0f1', alpha=0.9))

plt.tight_layout()
plt.savefig(f"{output_dir}/5_pipeline_flow_diagram.png", dpi=300, bbox_inches='tight')
print(f"✅ Saved: 5_pipeline_flow_diagram.png")
plt.close()

# ============================================================================
# PLOT 6: HISTORICAL VS STREAMING COMPARISON
# ============================================================================

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7))

# Left: Historical data build-up (simulated)
years = ['2009-2013', '2014-2018', '2019-2023', '2024', '2025\n(Stream)']
historical_cumulative = [50, 135, 230, 250, 250]  # In millions
colors_hist = ['#3498db', '#2ecc71', '#e67e22', '#9b59b6', '#e74c3c']

bars = ax1.bar(years, historical_cumulative, color=colors_hist,
               alpha=0.8, edgecolor='black', linewidth=2)
ax1.set_ylabel('Cumulative Bloom Filter Elements (Millions)', fontsize=12, fontweight='bold')
ax1.set_xlabel('Time Period', fontsize=12, fontweight='bold')
ax1.set_title('Bloom Filter Growth: Historical Build vs Streaming',
              fontsize=13, fontweight='bold', pad=15)
ax1.yaxis.grid(True, alpha=0.3)

# Add capacity line
ax1.axhline(y=250, color='red', linestyle='--', linewidth=2,
            label='Filter Capacity', alpha=0.7)
ax1.legend(fontsize=10)

for bar, val in zip(bars, historical_cumulative):
    height = bar.get_height()
    ax1.text(bar.get_x() + bar.get_width()/2., height,
             f'{val}M', ha='center', va='bottom',
             fontsize=10, fontweight='bold')

# Right: Processing efficiency
stages = ['Raw Data\nIngestion', 'Data\nCleaning', 'Bloom\nFiltering', 'Dedup\nOutput']
time_minutes = [10, 45, 3.5, 5]  # Approximate times
colors_efficiency = ['#e74c3c', '#3498db', '#2ecc71', '#9b59b6']

bars2 = ax2.barh(stages, time_minutes, color=colors_efficiency,
                 alpha=0.8, edgecolor='black', linewidth=2)
ax2.set_xlabel('Processing Time (minutes)', fontsize=12, fontweight='bold')
ax2.set_title('Pipeline Stage Efficiency\n(53M records processed)',
              fontsize=13, fontweight='bold', pad=15)
ax2.xaxis.grid(True, alpha=0.3)

for bar, time_val in zip(bars2, time_minutes):
    width = bar.get_width()
    ax2.text(width, bar.get_y() + bar.get_height()/2.,
             f'  {time_val} min', va='center',
             fontsize=11, fontweight='bold')

plt.tight_layout()
plt.savefig(f"{output_dir}/6_historical_vs_streaming.png", dpi=300, bbox_inches='tight')
print(f"✅ Saved: 6_historical_vs_streaming.png")
plt.close()

# ============================================================================
# EXPORT METRICS TO CSV
# ============================================================================

# Summary metrics
summary_metrics = {
    'Metric': [
        'Total Records Processed',
        'Unique Records Kept',
        'Duplicates Removed',
        'Duplicate Rate (%)',
        'Processing Time (seconds)',
        'Throughput (rows/sec)',
        'Bloom Filter Size (MB)',
        'Bloom Filter Capacity',
        'Bloom Filter Elements',
        'Fill Ratio (%)',
        'False Positive Rate (%)',
        'Space Savings vs Hash Table (%)'
    ],
    'Value': [
        f'{final_total:,}',
        f'{final_unique:,}',
        f'{final_dups:,}',
        f'{final_dups/final_total*100:.2f}',
        '211',
        f'{final_total/211:,.0f}',
        '286',
        f'{bloom_capacity:,}',
        f'{bloom_initial:,}',
        '100.00',
        '1.00',
        '97.0'
    ]
}

df_summary = pd.DataFrame(summary_metrics)
csv_path = f"{output_dir}/bloom_streaming_metrics.csv"
df_summary.to_csv(csv_path, index=False)
print(f"✅ Saved metrics: bloom_streaming_metrics.csv")

# Batch details
csv_batch_path = f"{output_dir}/bloom_batch_details.csv"
df_stream.to_csv(csv_batch_path, index=False)
print(f"✅ Saved batch details: bloom_batch_details.csv")

# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "="*70)
print("✅ ALL STREAMING ANALYSIS PLOTS GENERATED!")
print("="*70)
print(f"\nOutput directory: {output_dir}")
print("\nGenerated visualizations:")
print("  1. Streaming batch processing (batch-by-batch dedup)")
print("  2. Cumulative streaming progress (running totals)")
print("  3. Bloom filter fill ratio over time")
print("  4. Trip type analysis (duplicate rates by type)")
print("  5. End-to-end pipeline flow diagram")
print("  6. Historical vs streaming comparison")
print("\nExported data:")
print("  - bloom_streaming_metrics.csv (summary statistics)")
print("  - bloom_batch_details.csv (per-batch details)")
print("\nAll plots saved as high-resolution PNG files (300 DPI)")
print("="*70)

