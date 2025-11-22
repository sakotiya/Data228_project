#!/usr/bin/env python3
"""
Bloom Filter Streaming Analysis - Timeline View
Shows: Historical (2009-2024) builds filter → 2025 streams through it
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import matplotlib.patches as mpatches


def _load_env_file(path: str = ".env") -> None:
    """Simple .env loader so output paths can be customized per machine."""
    try:
        if not os.path.isfile(path):
            return
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                os.environ.setdefault(key, value)
    except Exception:
        pass


_load_env_file()

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (16, 10)
plt.rcParams['font.size'] = 10

# Output directory (overridable via DATA228_BLOOM_STREAMING_PLOTS_DIR)
_DEFAULT_STREAMING_DIR = "/Users/vidushi/Documents/bubu/big_data/git/bloom_streaming_plots"
output_dir = os.environ.get("DATA228_BLOOM_STREAMING_PLOTS_DIR", _DEFAULT_STREAMING_DIR)
os.makedirs(output_dir, exist_ok=True)

# ============================================================================
# DATA: Historical (Build) vs Streaming (2025)
# ============================================================================

# Historical data (used to BUILD bloom filter)
historical_data = {
    'Year': [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 
             2019, 2020, 2021, 2022, 2023, 2024],
    'Records_M': [15, 18, 20, 22, 24, 28, 30, 32, 28, 25, 35, 20, 30, 35, 40, 42]
}
df_historical = pd.DataFrame(historical_data)
df_historical['Cumulative_M'] = df_historical['Records_M'].cumsum()

# 2025 Streaming data (NEW data being deduplicated)
streaming_2025 = {
    'Batch': [1, 2, 3, 4, 5, 6, 7, 8],
    'Month': ['Mar', 'Apr', 'Jan', 'Mar', 'May', 'Jun', 'May', 'Jun'],
    'Type': ['FHV', 'FHV', 'FHVHV', 'FHVHV', 'Green', 'Green', 'Yellow', 'Yellow'],
    'Total_Rows': [2182992, 1699478, 20405666, 20536879, 54441, 48352, 4242250, 4024343],
    'Unique_Rows': [2161008, 1682322, 20198108, 20327220, 53862, 47868, 4199381, 3983395],
    'Duplicates': [21984, 17156, 207558, 209659, 579, 484, 42869, 40948]
}
df_stream = pd.DataFrame(streaming_2025)
df_stream['Dup_Rate'] = (df_stream['Duplicates'] / df_stream['Total_Rows']) * 100

# ============================================================================
# PLOT 1: TIMELINE - Historical Build vs Streaming Phase
# ============================================================================

fig, ax = plt.subplots(figsize=(18, 10))

# Historical phase background
ax.axvspan(2009, 2024.5, alpha=0.2, color='blue', label='Historical Phase (Build Filter)')
# Streaming phase background
ax.axvspan(2024.5, 2025.8, alpha=0.2, color='green', label='Streaming Phase (2025)')

# Historical data bars
ax.bar(df_historical['Year'], df_historical['Records_M'], 
       color='#3498db', alpha=0.7, edgecolor='black', linewidth=1, width=0.8,
       label='Historical Records (Building Bloom Filter)')

# Add streaming batches as scatter/bars on 2025
stream_x_positions = [2025.1, 2025.2, 2025.3, 2025.4, 2025.5, 2025.6, 2025.7, 2025.75]
stream_heights = [r/1e6 for r in df_stream['Total_Rows']]

ax.bar(stream_x_positions, stream_heights,
       color='#2ecc71', alpha=0.8, edgecolor='black', linewidth=2, width=0.05,
       label='2025 Streaming Batches (Being Deduplicated)')

# Add vertical line separating phases
ax.axvline(x=2024.5, color='red', linestyle='--', linewidth=3, 
           label='Bloom Filter Built Here →', alpha=0.7)

# Annotations
ax.text(2016.5, 42, 'HISTORICAL DATA\n(2009-2024)\n\nBuilding Bloom Filter\n250M trips total',
        fontsize=14, fontweight='bold', ha='center',
        bbox=dict(boxstyle='round,pad=1', facecolor='#3498db', alpha=0.3, edgecolor='black', linewidth=2))

ax.text(2025.4, 25, '2025 STREAMING\n(NEW DATA)\n\nChecking against\nBloom Filter\n53M trips',
        fontsize=14, fontweight='bold', ha='center',
        bbox=dict(boxstyle='round,pad=1', facecolor='#2ecc71', alpha=0.3, edgecolor='black', linewidth=2))

# Arrow showing process flow
ax.annotate('', xy=(2025.1, 35), xytext=(2024, 35),
            arrowprops=dict(arrowstyle='->', lw=4, color='red'))
ax.text(2024.5, 37, 'Filter Applied', ha='center', fontsize=11, fontweight='bold', color='red')

ax.set_xlabel('Year', fontsize=14, fontweight='bold')
ax.set_ylabel('Records (Millions)', fontsize=14, fontweight='bold')
ax.set_title('Bloom Filter Pipeline: Historical Build Phase (2009-2024) → Streaming Deduplication Phase (2025)\n' +
             'Historical data builds the filter | 2025 data streams through for deduplication',
             fontsize=16, fontweight='bold', pad=20)
ax.legend(fontsize=11, loc='upper left')
ax.set_xlim(2008.5, 2026)
ax.grid(True, alpha=0.3, axis='y')

plt.tight_layout()
plt.savefig(f"{output_dir}/timeline_1_historical_vs_streaming.png", dpi=300, bbox_inches='tight')
print(f"✅ Saved: timeline_1_historical_vs_streaming.png")
plt.close()

# ============================================================================
# PLOT 2: Two-Phase Architecture Diagram
# ============================================================================

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 8))

# LEFT PANEL: Historical Build Phase
ax1.axis('off')
ax1.set_xlim(0, 10)
ax1.set_ylim(0, 10)

ax1.text(5, 9.5, 'PHASE 1: Build Bloom Filter\n(Historical Data 2009-2024)',
         ha='center', fontsize=14, fontweight='bold',
         bbox=dict(boxstyle='round', facecolor='#3498db', alpha=0.3, edgecolor='black', linewidth=2))

# Historical data sources
sources = ['Yellow\nTaxi', 'Green\nTaxi', 'FHV', 'FHVHV']
y_positions = [7, 6, 5, 4]
for i, (source, y) in enumerate(zip(sources, y_positions)):
    rect = plt.Rectangle((0.5, y-0.3), 2, 0.6, facecolor='#3498db', 
                         edgecolor='black', linewidth=1.5, alpha=0.7)
    ax1.add_patch(rect)
    ax1.text(1.5, y, source, ha='center', va='center', fontsize=10, fontweight='bold', color='white')
    # Arrow to bloom filter
    ax1.arrow(2.6, y, 1.5, 1.5-i*0.5, head_width=0.15, head_length=0.2,
             fc='black', ec='black', linewidth=2)

# Bloom filter
rect_bloom = plt.Rectangle((4.5, 4), 3, 2.5, facecolor='#2ecc71',
                           edgecolor='black', linewidth=3, alpha=0.7)
ax1.add_patch(rect_bloom)
ax1.text(6, 5.5, 'BLOOM FILTER\n\n250M Elements\n286 MB\n7 Hash Functions',
        ha='center', va='center', fontsize=11, fontweight='bold', color='white')

# Arrow down to storage
ax1.arrow(6, 3.8, 0, -1.5, head_width=0.3, head_length=0.2,
         fc='#2ecc71', ec='black', linewidth=3)

# Storage
rect_storage = plt.Rectangle((4.5, 0.8), 3, 1, facecolor='#9b59b6',
                            edgecolor='black', linewidth=2, alpha=0.7)
ax1.add_patch(rect_storage)
ax1.text(6, 1.3, 'Saved Filter\nbloom_all_years.pkl',
        ha='center', va='center', fontsize=10, fontweight='bold', color='white')

ax1.text(5, 0.2, 'Duration: One-time build', ha='center', fontsize=9, style='italic')

# RIGHT PANEL: Streaming Phase
ax2.axis('off')
ax2.set_xlim(0, 10)
ax2.set_ylim(0, 10)

ax2.text(5, 9.5, 'PHASE 2: Stream Deduplication\n(2025 NEW Data)',
         ha='center', fontsize=14, fontweight='bold',
         bbox=dict(boxstyle='round', facecolor='#2ecc71', alpha=0.3, edgecolor='black', linewidth=2))

# Load bloom filter
rect_load = plt.Rectangle((3.5, 7.5), 3, 1, facecolor='#9b59b6',
                         edgecolor='black', linewidth=2, alpha=0.7)
ax2.add_patch(rect_load)
ax2.text(5, 8, 'Load Filter\nbloom_all_years.pkl',
        ha='center', va='center', fontsize=10, fontweight='bold', color='white')

# Arrow down
ax2.arrow(5, 7.3, 0, -0.8, head_width=0.3, head_length=0.15,
         fc='black', ec='black', linewidth=2)

# Streaming batches
rect_stream = plt.Rectangle((0.5, 5), 4, 1.5, facecolor='#e67e22',
                           edgecolor='black', linewidth=2, alpha=0.7)
ax2.add_patch(rect_stream)
ax2.text(2.5, 5.75, '2025 STREAMING\n8 Batches Arriving\n53M New Trips',
        ha='center', va='center', fontsize=10, fontweight='bold', color='white')

# Processing
rect_process = plt.Rectangle((5.5, 5), 4, 1.5, facecolor='#9b59b6',
                            edgecolor='black', linewidth=2, alpha=0.7)
ax2.add_patch(rect_process)
ax2.text(7.5, 5.75, 'Check Filter\nDeduplicate\n1.02% Dups Found',
        ha='center', va='center', fontsize=10, fontweight='bold', color='white')

# Arrows
ax2.arrow(4.6, 5.75, 0.8, 0, head_width=0.15, head_length=0.2,
         fc='#e67e22', ec='black', linewidth=2)

# Results
rect_unique = plt.Rectangle((1, 2.5), 3.5, 1.2, facecolor='#2ecc71',
                           edgecolor='black', linewidth=2, alpha=0.7)
ax2.add_patch(rect_unique)
ax2.text(2.75, 3.1, 'UNIQUE\n52.7M Trips\n(99.0%)',
        ha='center', va='center', fontsize=10, fontweight='bold', color='white')

rect_dups = plt.Rectangle((5.5, 2.5), 3.5, 1.2, facecolor='#e74c3c',
                         edgecolor='black', linewidth=2, alpha=0.7)
ax2.add_patch(rect_dups)
ax2.text(7.25, 3.1, 'DUPLICATES\n541K Trips\n(1.0%)',
        ha='center', va='center', fontsize=10, fontweight='bold', color='white')

# Arrows to results
ax2.arrow(6, 4.9, -2.5, -1.1, head_width=0.15, head_length=0.2,
         fc='#2ecc71', ec='#2ecc71', linewidth=3)
ax2.arrow(8, 4.9, 0, -1.1, head_width=0.15, head_length=0.2,
         fc='#e74c3c', ec='#e74c3c', linewidth=3)

ax2.text(5, 0.2, 'Duration: Real-time (3.5 min)', ha='center', fontsize=9, style='italic')

plt.tight_layout()
plt.savefig(f"{output_dir}/timeline_2_two_phase_architecture.png", dpi=300, bbox_inches='tight')
print(f"✅ Saved: timeline_2_two_phase_architecture.png")
plt.close()

# ============================================================================
# PLOT 3: Streaming Simulation - Batch Arrival Timeline
# ============================================================================

fig, ax = plt.subplots(figsize=(16, 8))

# Time axis (simulating arrival times)
time_intervals = [0, 30, 60, 150, 240, 270, 300, 330]  # seconds
colors_type = {'FHV': '#3498db', 'FHVHV': '#e67e22', 'Green': '#27ae60', 'Yellow': '#f1c40f'}

# Plot each batch as it "arrives"
for i, (time, batch, file, type_, total, unique, dups) in enumerate(zip(
    time_intervals, df_stream['Batch'], df_stream['Month'], 
    df_stream['Type'], df_stream['Total_Rows'], 
    df_stream['Unique_Rows'], df_stream['Duplicates'])):
    
    # Batch arrival point
    ax.scatter(time, total/1e6, s=500, color=colors_type[type_], 
              edgecolors='black', linewidth=2, zorder=3, alpha=0.8)
    
    # Label
    ax.text(time, total/1e6 + 1.5, f'Batch {batch}\n{type_} {file}\n{total/1e6:.1f}M',
           ha='center', fontsize=9, fontweight='bold')
    
    # Connection line (cumulative)
    if i > 0:
        ax.plot([time_intervals[i-1], time], 
               [streaming_2025['Total_Rows'][i-1]/1e6, total/1e6],
               '--', color='gray', alpha=0.5, linewidth=1)

# Add bloom filter check indication
for time in time_intervals:
    ax.axvline(x=time, color='red', linestyle=':', alpha=0.3, linewidth=1)

ax.set_xlabel('Processing Time (seconds)', fontsize=12, fontweight='bold')
ax.set_ylabel('Batch Size (Million Records)', fontsize=12, fontweight='bold')
ax.set_title('2025 Streaming Data Arrival Timeline\n(Each batch checked against historical Bloom filter as it arrives)',
             fontsize=14, fontweight='bold', pad=20)
ax.grid(True, alpha=0.3)

# Add legend for trip types
legend_elements = [plt.Line2D([0], [0], marker='o', color='w', 
                             markerfacecolor=colors_type[t], markersize=12, 
                             label=t, markeredgecolor='black', markeredgewidth=1.5)
                  for t in ['FHV', 'FHVHV', 'Green', 'Yellow']]
ax.legend(handles=legend_elements, fontsize=11, loc='upper left', title='Trip Type')

# Add info box
info_text = 'Bloom Filter: Loaded once\nChecked for each arriving batch\nMemory: 286 MB constant'
ax.text(0.98, 0.5, info_text, transform=ax.transAxes,
       fontsize=11, verticalalignment='center', horizontalalignment='right',
       bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))

plt.tight_layout()
plt.savefig(f"{output_dir}/timeline_3_batch_arrival_simulation.png", dpi=300, bbox_inches='tight')
print(f"✅ Saved: timeline_3_batch_arrival_simulation.png")
plt.close()

# ============================================================================
# PLOT 4: Data Volume Comparison - Historical vs Streaming
# ============================================================================

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7))

# Left: Volume comparison
phases = ['Historical\n(2009-2024)\nBuild Filter', '2025\nStreaming\nDedup']
volumes = [250, 53.2]  # Millions
colors_vol = ['#3498db', '#2ecc71']
purposes = ['BUILD', 'CHECK']

bars = ax1.bar(phases, volumes, color=colors_vol, alpha=0.7, 
              edgecolor='black', linewidth=2, width=0.5)

for bar, vol, purpose in zip(bars, volumes, purposes):
    height = bar.get_height()
    ax1.text(bar.get_x() + bar.get_width()/2., height/2,
            f'{vol}M\nRecords\n\n({purpose})', ha='center', va='center',
            fontsize=13, fontweight='bold', color='white')

ax1.set_ylabel('Records (Millions)', fontsize=12, fontweight='bold')
ax1.set_title('Data Volume: Historical (Build) vs Streaming (Check)',
             fontsize=13, fontweight='bold', pad=15)
ax1.set_ylim(0, 280)
ax1.yaxis.grid(True, alpha=0.3)

# Right: Purpose breakdown
purposes_detailed = ['Build\nBloom Filter', 'Load Filter\n(Memory)', 'Stream\nDeduplication', 'Save\nResults']
times = [180, 2, 211, 10]  # seconds (estimated)
colors_purpose = ['#3498db', '#9b59b6', '#2ecc71', '#e67e22']
phases_label = ['Historical', 'Setup', 'Streaming', 'Output']

bars2 = ax2.barh(purposes_detailed, times, color=colors_purpose, 
                alpha=0.8, edgecolor='black', linewidth=2)

ax2.set_xlabel('Time (seconds)', fontsize=12, fontweight='bold')
ax2.set_title('Processing Time Breakdown',
             fontsize=13, fontweight='bold', pad=15)
ax2.xaxis.grid(True, alpha=0.3)

for bar, time_val, phase in zip(bars2, times, phases_label):
    width = bar.get_width()
    ax2.text(width/2, bar.get_y() + bar.get_height()/2.,
            f'{time_val}s\n({phase})', va='center', ha='center',
            fontsize=10, fontweight='bold', color='white')

plt.tight_layout()
plt.savefig(f"{output_dir}/timeline_4_volume_comparison.png", dpi=300, bbox_inches='tight')
print(f"✅ Saved: timeline_4_volume_comparison.png")
plt.close()

# ============================================================================
# PLOT 5: Streaming Process Flow
# ============================================================================

fig, ax = plt.subplots(figsize=(16, 10))
ax.set_xlim(0, 10)
ax.set_ylim(0, 10)
ax.axis('off')

# Title
ax.text(5, 9.7, 'Bloom Filter Streaming Deduplication Process',
       ha='center', fontsize=18, fontweight='bold')
ax.text(5, 9.3, 'Historical (2009-2024) → Build Filter | 2025 → Stream & Deduplicate',
       ha='center', fontsize=12, style='italic', color='gray')

# Historical section
ax.text(1.5, 8.5, 'HISTORICAL PHASE', ha='center', fontsize=11, 
       fontweight='bold', color='#3498db')
rect1 = plt.Rectangle((0.2, 6.5), 2.6, 1.5, facecolor='#3498db',
                      edgecolor='black', linewidth=2, alpha=0.3)
ax.add_patch(rect1)
ax.text(1.5, 7.5, '2009-2024 Data\n250M Trips', ha='center', va='center',
       fontsize=10, fontweight='bold')
ax.text(1.5, 6.8, 'Yellow, Green,\nFHV, FHVHV', ha='center', va='center',
       fontsize=8, style='italic')

# Arrow to bloom filter
ax.annotate('', xy=(3.2, 7.2), xytext=(2.9, 7.2),
           arrowprops=dict(arrowstyle='->', lw=3, color='black'))
ax.text(3.05, 7.5, 'Build', ha='center', fontsize=9, fontweight='bold')

# Bloom filter (center)
rect2 = plt.Rectangle((3.2, 6.5), 2, 1.5, facecolor='#2ecc71',
                      edgecolor='black', linewidth=3, alpha=0.7)
ax.add_patch(rect2)
ax.text(4.2, 7.5, 'BLOOM\nFILTER', ha='center', va='center',
       fontsize=11, fontweight='bold', color='white')
ax.text(4.2, 6.8, '286 MB', ha='center', va='center',
       fontsize=8, color='white')

# Streaming section
ax.text(7.5, 8.5, 'STREAMING PHASE', ha='center', fontsize=11,
       fontweight='bold', color='#2ecc71')
rect3 = plt.Rectangle((6.2, 6.5), 2.6, 1.5, facecolor='#e67e22',
                      edgecolor='black', linewidth=2, alpha=0.3)
ax.add_patch(rect3)
ax.text(7.5, 7.5, '2025 Stream\n53M Trips', ha='center', va='center',
       fontsize=10, fontweight='bold')
ax.text(7.5, 6.8, '8 Batches\nArriving', ha='center', va='center',
       fontsize=8, style='italic')

# Arrow to bloom filter
ax.annotate('', xy=(5.3, 7.2), xytext=(6.1, 7.2),
           arrowprops=dict(arrowstyle='->', lw=3, color='#e67e22'))
ax.text(5.7, 7.5, 'Check', ha='center', fontsize=9, fontweight='bold', color='#e67e22')

# Processing detail
ax.text(5, 5.5, 'FOR EACH STREAMING BATCH:', ha='center', fontsize=11,
       fontweight='bold', bbox=dict(boxstyle='round', facecolor='yellow', alpha=0.3))

# Steps
steps = [
    '1. Read batch from stream',
    '2. Build trip_id signature',
    '3. Check: trip_id IN bloom_filter?',
    '4. IF YES → Duplicate (remove)',
    '5. IF NO → Unique (keep)',
    '6. Write unique records'
]

for i, step in enumerate(steps):
    y_pos = 4.8 - i*0.5
    color = '#e74c3c' if 'YES' in step else '#2ecc71' if 'NO' in step else 'black'
    ax.text(5, y_pos, step, ha='center', fontsize=9, color=color, fontweight='bold')

# Results
rect4 = plt.Rectangle((1.5, 0.5), 2.5, 1, facecolor='#2ecc71',
                      edgecolor='black', linewidth=2, alpha=0.7)
ax.add_patch(rect4)
ax.text(2.75, 1, 'UNIQUE\n52.7M (99%)', ha='center', va='center',
       fontsize=10, fontweight='bold', color='white')

rect5 = plt.Rectangle((5.5, 0.5), 2.5, 1, facecolor='#e74c3c',
                      edgecolor='black', linewidth=2, alpha=0.7)
ax.add_patch(rect5)
ax.text(6.75, 1, 'DUPLICATES\n541K (1%)', ha='center', va='center',
       fontsize=10, fontweight='bold', color='white')

# Arrows to results
ax.annotate('', xy=(2.75, 0.6), xytext=(4, 1.5),
           arrowprops=dict(arrowstyle='->', lw=2, color='#2ecc71'))
ax.annotate('', xy=(6.75, 0.6), xytext=(6, 1.5),
           arrowprops=dict(arrowstyle='->', lw=2, color='#e74c3c'))

plt.tight_layout()
plt.savefig(f"{output_dir}/timeline_5_streaming_process_flow.png", dpi=300, bbox_inches='tight')
print(f"✅ Saved: timeline_5_streaming_process_flow.png")
plt.close()

# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "="*70)
print("✅ STREAMING TIMELINE VISUALIZATIONS COMPLETE!")
print("="*70)
print(f"\nOutput directory: {output_dir}")
print("\nGenerated 5 new timeline-focused plots:")
print("  1. Historical (2009-2024) vs Streaming (2025) timeline")
print("  2. Two-phase architecture (Build vs Stream)")
print("  3. Batch arrival simulation (real-time processing)")
print("  4. Volume comparison (250M historical vs 53M streaming)")
print("  5. Streaming process flow (detailed steps)")
print("\nThese clearly show:")
print("  ✓ 2009-2024 = Historical data (BUILDS bloom filter)")
print("  ✓ 2025 = NEW streaming data (CHECKED against filter)")
print("  ✓ Clear separation of build vs streaming phases")
print("="*70)

