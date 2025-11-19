#!/usr/bin/env python3
"""
Bloom Filter Analysis Visualizations
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 8)
plt.rcParams['font.size'] = 10

# Output directory
output_dir = "/Users/gouravdhama/Documents/bubu/big_data/git/bloom_analysis_plots"
os.makedirs(output_dir, exist_ok=True)

# ============================================================================
# DATA PREPARATION
# ============================================================================

# Results from deduplication
results = {
    'File': ['FHV Mar', 'FHV Apr', 'FHVHV Jan', 'FHVHV Mar', 
             'Green May', 'Green Jun', 'Yellow May', 'Yellow Jun'],
    'Total_Rows': [2182992, 1699478, 20405666, 20536879, 
                   54441, 48352, 4242250, 4024343],
    'Unique_Rows': [2161008, 1682322, 20198108, 20327220, 
                    53862, 47868, 4199381, 3983395],
    'Duplicates': [21984, 17156, 207558, 209659, 
                   579, 484, 42869, 40948],
    'Type': ['FHV', 'FHV', 'FHVHV', 'FHVHV', 
             'Green', 'Green', 'Yellow', 'Yellow']
}

df = pd.DataFrame(results)
df['Dup_Rate'] = (df['Duplicates'] / df['Total_Rows']) * 100
df['Size_MB'] = [26, 21, 507, 515, 1.4, 1.3, 82, 78]  # Approximate file sizes

# ============================================================================
# PLOT 1: DUPLICATE DETECTION BY FILE
# ============================================================================

fig, ax = plt.subplots(figsize=(14, 6))

x = np.arange(len(df))
width = 0.35

bars1 = ax.bar(x - width/2, df['Unique_Rows'], width, 
               label='Unique (Kept)', color='#2ecc71', alpha=0.8)
bars2 = ax.bar(x + width/2, df['Duplicates'], width, 
               label='Duplicates (Removed)', color='#e74c3c', alpha=0.8)

ax.set_xlabel('2025 Data Files', fontsize=12, fontweight='bold')
ax.set_ylabel('Number of Rows', fontsize=12, fontweight='bold')
ax.set_title('Bloom Filter Duplicate Detection Results by File\n(53.2M rows processed → 52.7M unique, 541K duplicates)', 
             fontsize=14, fontweight='bold', pad=20)
ax.set_xticks(x)
ax.set_xticklabels(df['File'], rotation=45, ha='right')
ax.legend(fontsize=11)
ax.yaxis.grid(True, alpha=0.3)

# Add percentage labels on duplicate bars
for i, (bar, dup_rate) in enumerate(zip(bars2, df['Dup_Rate'])):
    height = bar.get_height()
    ax.text(bar.get_x() + bar.get_width()/2., height,
            f'{dup_rate:.2f}%',
            ha='center', va='bottom', fontsize=9, fontweight='bold')

plt.tight_layout()
plt.savefig(f"{output_dir}/1_duplicate_detection_by_file.png", dpi=300, bbox_inches='tight')
print(f"✅ Saved: 1_duplicate_detection_by_file.png")
plt.close()

# ============================================================================
# PLOT 2: DUPLICATE RATE COMPARISON
# ============================================================================

fig, ax = plt.subplots(figsize=(12, 7))

colors = {'FHV': '#3498db', 'FHVHV': '#e67e22', 'Green': '#27ae60', 'Yellow': '#f1c40f'}
bar_colors = [colors[t] for t in df['Type']]

bars = ax.bar(df['File'], df['Dup_Rate'], color=bar_colors, alpha=0.8, edgecolor='black', linewidth=1.5)

ax.set_xlabel('File', fontsize=12, fontweight='bold')
ax.set_ylabel('Duplicate Rate (%)', fontsize=12, fontweight='bold')
ax.set_title('Duplicate Rate Across Different Taxi Types\n(Consistent ~1.02% duplication pattern)', 
             fontsize=14, fontweight='bold', pad=20)
ax.set_ylim(0, 1.2)
plt.xticks(rotation=45, ha='right')
ax.yaxis.grid(True, alpha=0.3)

# Add average line
avg_dup_rate = df['Dup_Rate'].mean()
ax.axhline(y=avg_dup_rate, color='red', linestyle='--', linewidth=2, 
           label=f'Average: {avg_dup_rate:.2f}%')
ax.legend(fontsize=11)

# Add value labels
for bar in bars:
    height = bar.get_height()
    ax.text(bar.get_x() + bar.get_width()/2., height,
            f'{height:.2f}%',
            ha='center', va='bottom', fontsize=9, fontweight='bold')

plt.tight_layout()
plt.savefig(f"{output_dir}/2_duplicate_rate_comparison.png", dpi=300, bbox_inches='tight')
print(f"✅ Saved: 2_duplicate_rate_comparison.png")
plt.close()

# ============================================================================
# PLOT 3: SPACE EFFICIENCY COMPARISON
# ============================================================================

fig, ax = plt.subplots(figsize=(10, 8))

methods = ['Hash Table\n(All IDs)', 'Database\nIndex', 'Bloom Filter\n(Actual)']
memory_mb = [8000, 2000, 286]
colors_space = ['#e74c3c', '#f39c12', '#2ecc71']

bars = ax.barh(methods, memory_mb, color=colors_space, alpha=0.8, edgecolor='black', linewidth=2)

ax.set_xlabel('Memory Usage (MB)', fontsize=12, fontweight='bold')
ax.set_title('Space Efficiency: Bloom Filter vs Traditional Methods\n(250M elements stored)', 
             fontsize=14, fontweight='bold', pad=20)
ax.xaxis.grid(True, alpha=0.3)

# Add value labels and savings
for i, (bar, mem) in enumerate(zip(bars, memory_mb)):
    width = bar.get_width()
    savings = ((memory_mb[0] - mem) / memory_mb[0]) * 100 if i > 0 else 0
    label = f'{mem:,} MB'
    if savings > 0:
        label += f'\n({savings:.1f}% savings)'
    ax.text(width, bar.get_y() + bar.get_height()/2.,
            label, ha='left', va='center', fontsize=11, 
            fontweight='bold', bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

plt.tight_layout()
plt.savefig(f"{output_dir}/3_space_efficiency.png", dpi=300, bbox_inches='tight')
print(f"✅ Saved: 3_space_efficiency.png")
plt.close()

# ============================================================================
# PLOT 4: PROCESSING VOLUME BY TYPE
# ============================================================================

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

# Pie chart - Total rows by type
type_totals = df.groupby('Type')['Total_Rows'].sum()
colors_pie = ['#3498db', '#e67e22', '#27ae60', '#f1c40f']
explode = (0.05, 0.05, 0.05, 0.05)

wedges, texts, autotexts = ax1.pie(type_totals, labels=type_totals.index, 
                                     autopct='%1.1f%%', colors=colors_pie,
                                     explode=explode, startangle=90,
                                     textprops={'fontsize': 11, 'fontweight': 'bold'})
ax1.set_title('Volume Distribution by Taxi Type\n(Total: 53.2M rows)', 
              fontsize=13, fontweight='bold', pad=15)

# Bar chart - Duplicates by type
type_dups = df.groupby('Type')['Duplicates'].sum()
bars = ax2.bar(type_dups.index, type_dups.values, color=colors_pie, 
               alpha=0.8, edgecolor='black', linewidth=2)
ax2.set_ylabel('Total Duplicates', fontsize=12, fontweight='bold')
ax2.set_xlabel('Taxi Type', fontsize=12, fontweight='bold')
ax2.set_title('Total Duplicates Detected by Type\n(Total: 541K duplicates)', 
              fontsize=13, fontweight='bold', pad=15)
ax2.yaxis.grid(True, alpha=0.3)

# Add value labels
for bar in bars:
    height = bar.get_height()
    ax2.text(bar.get_x() + bar.get_width()/2., height,
            f'{int(height):,}',
            ha='center', va='bottom', fontsize=10, fontweight='bold')

plt.tight_layout()
plt.savefig(f"{output_dir}/4_volume_by_type.png", dpi=300, bbox_inches='tight')
print(f"✅ Saved: 4_volume_by_type.png")
plt.close()

# ============================================================================
# PLOT 5: BLOOM FILTER PERFORMANCE METRICS
# ============================================================================

fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))

# Metric 1: Processing speed
processing_time = 211  # seconds
total_rows = 53194401
rows_per_sec = total_rows / processing_time

metrics = ['Rows/Second', 'Total Rows\n(Millions)', 'Processing\nTime (min)', 'Memory\n(MB)']
values = [rows_per_sec, total_rows/1e6, processing_time/60, 286]
colors_met = ['#3498db', '#e67e22', '#2ecc71', '#9b59b6']

bars = ax1.bar(metrics, values, color=colors_met, alpha=0.8, edgecolor='black', linewidth=2)
ax1.set_title('Bloom Filter Performance Metrics', fontsize=13, fontweight='bold', pad=15)
ax1.set_ylabel('Value', fontsize=11, fontweight='bold')
ax1.yaxis.grid(True, alpha=0.3)

for bar, val in zip(bars, values):
    height = bar.get_height()
    ax1.text(bar.get_x() + bar.get_width()/2., height,
            f'{val:,.0f}',
            ha='center', va='bottom', fontsize=10, fontweight='bold')

# Metric 2: False positive rate visualization
fp_rates = [0.001, 0.005, 0.01, 0.05, 0.1]
memory_savings = [95, 96, 97, 98, 98.5]

ax2.plot(fp_rates, memory_savings, marker='o', linewidth=3, markersize=10, 
         color='#2ecc71', label='Memory Savings')
ax2.scatter([0.01], [97], color='red', s=200, zorder=5, 
            label='Our Configuration', edgecolors='black', linewidths=2)
ax2.set_xlabel('False Positive Rate', fontsize=11, fontweight='bold')
ax2.set_ylabel('Memory Savings vs Hash Table (%)', fontsize=11, fontweight='bold')
ax2.set_title('Trade-off: False Positive Rate vs Space Savings', 
              fontsize=13, fontweight='bold', pad=15)
ax2.grid(True, alpha=0.3)
ax2.legend(fontsize=10)
ax2.set_xscale('log')

# Metric 3: File size comparison
ax3.barh(df['File'], df['Size_MB'], color=bar_colors, alpha=0.8, 
         edgecolor='black', linewidth=1.5)
ax3.set_xlabel('File Size (MB)', fontsize=11, fontweight='bold')
ax3.set_title('Output File Sizes (Deduplicated Data)', fontsize=13, fontweight='bold', pad=15)
ax3.xaxis.grid(True, alpha=0.3)

for i, (file, size) in enumerate(zip(df['File'], df['Size_MB'])):
    ax3.text(size, i, f'  {size}MB', va='center', fontsize=9, fontweight='bold')

# Metric 4: Cumulative processing
cumulative_rows = df['Total_Rows'].cumsum()
cumulative_dups = df['Duplicates'].cumsum()

ax4.plot(range(1, 9), cumulative_rows/1e6, marker='o', linewidth=3, 
         markersize=8, label='Total Processed', color='#3498db')
ax4.plot(range(1, 9), (cumulative_rows - cumulative_dups)/1e6, marker='s', 
         linewidth=3, markersize=8, label='Unique Kept', color='#2ecc71')
ax4.fill_between(range(1, 9), cumulative_rows/1e6, 
                  (cumulative_rows - cumulative_dups)/1e6, 
                  alpha=0.3, color='#e74c3c', label='Duplicates')
ax4.set_xlabel('Batch Number', fontsize=11, fontweight='bold')
ax4.set_ylabel('Cumulative Rows (Millions)', fontsize=11, fontweight='bold')
ax4.set_title('Cumulative Processing Progress', fontsize=13, fontweight='bold', pad=15)
ax4.legend(fontsize=10)
ax4.grid(True, alpha=0.3)
ax4.set_xticks(range(1, 9))

plt.tight_layout()
plt.savefig(f"{output_dir}/5_performance_metrics.png", dpi=300, bbox_inches='tight')
print(f"✅ Saved: 5_performance_metrics.png")
plt.close()

# ============================================================================
# PLOT 6: BLOOM FILTER CONCEPTUAL DIAGRAM
# ============================================================================

fig, ax = plt.subplots(figsize=(14, 8))
ax.set_xlim(0, 10)
ax.set_ylim(0, 10)
ax.axis('off')

# Title
ax.text(5, 9.5, 'Bloom Filter Architecture & Flow', 
        ha='center', fontsize=18, fontweight='bold')

# Historical data box
rect1 = plt.Rectangle((0.5, 7), 2, 1.5, facecolor='#3498db', 
                       edgecolor='black', linewidth=2, alpha=0.7)
ax.add_patch(rect1)
ax.text(1.5, 7.75, 'Historical Data\n250M Trips\n(2009-2024)', 
        ha='center', va='center', fontsize=10, fontweight='bold', color='white')

# Bloom filter box
rect2 = plt.Rectangle((3.5, 7), 2, 1.5, facecolor='#2ecc71', 
                       edgecolor='black', linewidth=2, alpha=0.7)
ax.add_patch(rect2)
ax.text(4.5, 7.75, 'Bloom Filter\n286 MB\n7 hash functions', 
        ha='center', va='center', fontsize=10, fontweight='bold', color='white')

# Streaming data box
rect3 = plt.Rectangle((6.5, 7), 2, 1.5, facecolor='#e67e22', 
                       edgecolor='black', linewidth=2, alpha=0.7)
ax.add_patch(rect3)
ax.text(7.5, 7.75, '2025 Streaming\n53.2M Trips\n(8 batches)', 
        ha='center', va='center', fontsize=10, fontweight='bold', color='white')

# Arrows
ax.arrow(2.5, 7.75, 0.8, 0, head_width=0.2, head_length=0.15, fc='black', ec='black', linewidth=2)
ax.arrow(5.5, 7.75, 0.8, 0, head_width=0.2, head_length=0.15, fc='black', ec='black', linewidth=2)

# Processing box
rect4 = plt.Rectangle((2, 4.5), 5, 1.5, facecolor='#9b59b6', 
                       edgecolor='black', linewidth=2, alpha=0.7)
ax.add_patch(rect4)
ax.text(4.5, 5.25, 'Duplicate Detection\nCheck: trip_id IN bloom_filter?', 
        ha='center', va='center', fontsize=11, fontweight='bold', color='white')

ax.arrow(4.5, 7, 0, -1.3, head_width=0.2, head_length=0.15, fc='black', ec='black', linewidth=2)

# Output boxes
rect5 = plt.Rectangle((1, 2), 2.5, 1.2, facecolor='#2ecc71', 
                       edgecolor='black', linewidth=2, alpha=0.7)
ax.add_patch(rect5)
ax.text(2.25, 2.6, 'UNIQUE\n52.7M rows\n(99.0%)', 
        ha='center', va='center', fontsize=10, fontweight='bold', color='white')

rect6 = plt.Rectangle((5.5, 2), 2.5, 1.2, facecolor='#e74c3c', 
                       edgecolor='black', linewidth=2, alpha=0.7)
ax.add_patch(rect6)
ax.text(6.75, 2.6, 'DUPLICATES\n541K rows\n(1.0%)', 
        ha='center', va='center', fontsize=10, fontweight='bold', color='white')

ax.arrow(3, 4.5, -0.5, -1.1, head_width=0.2, head_length=0.15, fc='#2ecc71', ec='#2ecc71', linewidth=3)
ax.arrow(6, 4.5, 0.5, -1.1, head_width=0.2, head_length=0.15, fc='#e74c3c', ec='#e74c3c', linewidth=3)

# Labels
ax.text(1.5, 4, 'NOT IN\nFILTER', ha='center', fontsize=9, fontweight='bold', color='#2ecc71')
ax.text(7.5, 4, 'IN\nFILTER', ha='center', fontsize=9, fontweight='bold', color='#e74c3c')

# Stats box
rect7 = plt.Rectangle((0.5, 0.2), 8, 1, facecolor='#ecf0f1', 
                       edgecolor='black', linewidth=2, alpha=0.9)
ax.add_patch(rect7)
stats_text = 'Performance: 251K rows/sec | Memory: 286MB | Time: 3.5 min | Space Savings: 97%'
ax.text(4.5, 0.7, stats_text, ha='center', va='center', 
        fontsize=11, fontweight='bold', color='#2c3e50')

plt.tight_layout()
plt.savefig(f"{output_dir}/6_bloom_filter_architecture.png", dpi=300, bbox_inches='tight')
print(f"✅ Saved: 6_bloom_filter_architecture.png")
plt.close()

# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "="*70)
print("✅ ALL PLOTS GENERATED SUCCESSFULLY!")
print("="*70)
print(f"\nOutput directory: {output_dir}")
print("\nGenerated plots:")
print("  1. Duplicate detection by file (bar chart)")
print("  2. Duplicate rate comparison (bar chart)")
print("  3. Space efficiency comparison (horizontal bar)")
print("  4. Volume by type (pie + bar)")
print("  5. Performance metrics (4-panel dashboard)")
print("  6. Bloom filter architecture (flow diagram)")
print("\nAll plots saved as high-resolution PNG files (300 DPI)")
print("="*70)

