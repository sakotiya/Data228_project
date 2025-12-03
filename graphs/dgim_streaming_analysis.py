"""
DGIM Streaming Analysis - All Graphs
Each graph saved as separate PNG file
"""

import matplotlib.pyplot as plt
import numpy as np

# =============================================================================
# YOUR DGIM METRICS DATA
# =============================================================================
snapshots = [
    {"record_num": 2659720, "high_fare": {"estimated": 4171, "actual": 3107, "error_percentage": 34.25, "buckets": 17}, "long_distance": {"estimated": 2473, "actual": 2126, "error_percentage": 16.32, "buckets": 16}, "high_tip": {"estimated": 919, "actual": 597, "error_percentage": 53.94, "buckets": 13}, "airport": {"estimated": 2231, "actual": 1601, "error_percentage": 39.35, "buckets": 17}},
    {"record_num": 5319440, "high_fare": {"estimated": 3767, "actual": 2617, "error_percentage": 43.94, "buckets": 17}, "long_distance": {"estimated": 2171, "actual": 1676, "error_percentage": 29.53, "buckets": 16}, "high_tip": {"estimated": 815, "actual": 596, "error_percentage": 36.74, "buckets": 14}, "airport": {"estimated": 1055, "actual": 950, "error_percentage": 11.05, "buckets": 11}},
    {"record_num": 7979160, "high_fare": {"estimated": 6483, "actual": 5320, "error_percentage": 21.86, "buckets": 19}, "long_distance": {"estimated": 2081, "actual": 1661, "error_percentage": 25.29, "buckets": 15}, "high_tip": {"estimated": 15, "actual": 30, "error_percentage": 50.0, "buckets": 5}, "airport": {"estimated": 197, "actual": 163, "error_percentage": 20.86, "buckets": 12}},
    {"record_num": 10638880, "high_fare": {"estimated": 0, "actual": 0, "error_percentage": 0, "buckets": 0}, "long_distance": {"estimated": 0, "actual": 0, "error_percentage": 0, "buckets": 0}, "high_tip": {"estimated": 0, "actual": 0, "error_percentage": 0, "buckets": 0}, "airport": {"estimated": 247, "actual": 195, "error_percentage": 26.67, "buckets": 11}},
    {"record_num": 13298600, "high_fare": {"estimated": 4405, "actual": 3947, "error_percentage": 11.6, "buckets": 19}, "long_distance": {"estimated": 3853, "actual": 2417, "error_percentage": 59.41, "buckets": 17}, "high_tip": {"estimated": 2239, "actual": 1821, "error_percentage": 22.95, "buckets": 15}, "airport": {"estimated": 931, "actual": 550, "error_percentage": 69.27, "buckets": 13}},
    {"record_num": 15958320, "high_fare": {"estimated": 7341, "actual": 4731, "error_percentage": 55.17, "buckets": 19}, "long_distance": {"estimated": 4095, "actual": 2569, "error_percentage": 59.4, "buckets": 14}, "high_tip": {"estimated": 2251, "actual": 1743, "error_percentage": 29.15, "buckets": 15}, "airport": {"estimated": 639, "actual": 510, "error_percentage": 25.29, "buckets": 12}},
    {"record_num": 18618040, "high_fare": {"estimated": 6503, "actual": 4841, "error_percentage": 34.33, "buckets": 19}, "long_distance": {"estimated": 4479, "actual": 4008, "error_percentage": 11.75, "buckets": 14}, "high_tip": {"estimated": 1847, "actual": 1411, "error_percentage": 30.9, "buckets": 15}, "airport": {"estimated": 1081, "actual": 699, "error_percentage": 54.65, "buckets": 16}},
    {"record_num": 21277760, "high_fare": {"estimated": 4915, "actual": 4441, "error_percentage": 10.67, "buckets": 17}, "long_distance": {"estimated": 3991, "actual": 2515, "error_percentage": 58.69, "buckets": 17}, "high_tip": {"estimated": 2145, "actual": 1883, "error_percentage": 13.91, "buckets": 16}, "airport": {"estimated": 931, "actual": 653, "error_percentage": 42.57, "buckets": 13}},
    {"record_num": 23937480, "high_fare": {"estimated": 6073, "actual": 4777, "error_percentage": 27.13, "buckets": 20}, "long_distance": {"estimated": 4233, "actual": 3422, "error_percentage": 23.7, "buckets": 17}, "high_tip": {"estimated": 2623, "actual": 1634, "error_percentage": 60.53, "buckets": 15}, "airport": {"estimated": 387, "actual": 256, "error_percentage": 51.17, "buckets": 12}},
    {"record_num": 26597200, "high_fare": {"estimated": 5711, "actual": 4805, "error_percentage": 18.86, "buckets": 16}, "long_distance": {"estimated": 4403, "actual": 3043, "error_percentage": 44.69, "buckets": 18}, "high_tip": {"estimated": 1959, "actual": 1666, "error_percentage": 17.59, "buckets": 15}, "airport": {"estimated": 1023, "actual": 697, "error_percentage": 46.77, "buckets": 12}},
    {"record_num": 29256920, "high_fare": {"estimated": 4637, "actual": 4084, "error_percentage": 13.54, "buckets": 19}, "long_distance": {"estimated": 4119, "actual": 3083, "error_percentage": 33.6, "buckets": 16}, "high_tip": {"estimated": 2277, "actual": 1686, "error_percentage": 35.05, "buckets": 18}, "airport": {"estimated": 923, "actual": 745, "error_percentage": 23.89, "buckets": 14}},
    {"record_num": 31916640, "high_fare": {"estimated": 8335, "actual": 5808, "error_percentage": 43.51, "buckets": 17}, "long_distance": {"estimated": 4323, "actual": 3304, "error_percentage": 30.84, "buckets": 18}, "high_tip": {"estimated": 2487, "actual": 1516, "error_percentage": 64.05, "buckets": 16}, "airport": {"estimated": 311, "actual": 287, "error_percentage": 8.36, "buckets": 11}},
    {"record_num": 34576360, "high_fare": {"estimated": 7603, "actual": 6112, "error_percentage": 24.39, "buckets": 19}, "long_distance": {"estimated": 8139, "actual": 5342, "error_percentage": 52.36, "buckets": 20}, "high_tip": {"estimated": 2207, "actual": 1755, "error_percentage": 25.75, "buckets": 15}, "airport": {"estimated": 4579, "actual": 3126, "error_percentage": 46.48, "buckets": 19}},
    {"record_num": 37236080, "high_fare": {"estimated": 5327, "actual": 4750, "error_percentage": 12.15, "buckets": 18}, "long_distance": {"estimated": 4287, "actual": 3335, "error_percentage": 28.55, "buckets": 14}, "high_tip": {"estimated": 2083, "actual": 1566, "error_percentage": 33.01, "buckets": 15}, "airport": {"estimated": 1479, "actual": 959, "error_percentage": 54.22, "buckets": 14}},
    {"record_num": 39895800, "high_fare": {"estimated": 8761, "actual": 6534, "error_percentage": 34.08, "buckets": 20}, "long_distance": {"estimated": 8635, "actual": 5582, "error_percentage": 54.69, "buckets": 21}, "high_tip": {"estimated": 2247, "actual": 1751, "error_percentage": 28.33, "buckets": 16}, "airport": {"estimated": 4241, "actual": 3327, "error_percentage": 27.47, "buckets": 17}},
    {"record_num": 42555520, "high_fare": {"estimated": 7741, "actual": 4824, "error_percentage": 60.47, "buckets": 20}, "long_distance": {"estimated": 4527, "actual": 3599, "error_percentage": 25.78, "buckets": 18}, "high_tip": {"estimated": 2887, "actual": 1754, "error_percentage": 64.6, "buckets": 15}, "airport": {"estimated": 1143, "actual": 818, "error_percentage": 39.73, "buckets": 14}},
    {"record_num": 45215240, "high_fare": {"estimated": 7441, "actual": 4665, "error_percentage": 59.51, "buckets": 17}, "long_distance": {"estimated": 4351, "actual": 3318, "error_percentage": 31.13, "buckets": 15}, "high_tip": {"estimated": 1837, "actual": 1539, "error_percentage": 19.36, "buckets": 16}, "airport": {"estimated": 617, "actual": 405, "error_percentage": 52.35, "buckets": 13}},
    {"record_num": 47874960, "high_fare": {"estimated": 6367, "actual": 5511, "error_percentage": 15.53, "buckets": 18}, "long_distance": {"estimated": 4595, "actual": 3966, "error_percentage": 15.86, "buckets": 20}, "high_tip": {"estimated": 1981, "actual": 1641, "error_percentage": 20.72, "buckets": 18}, "airport": {"estimated": 2055, "actual": 1334, "error_percentage": 54.05, "buckets": 14}},
    {"record_num": 50534680, "high_fare": {"estimated": 9079, "actual": 6732, "error_percentage": 34.86, "buckets": 19}, "long_distance": {"estimated": 4617, "actual": 3175, "error_percentage": 45.42, "buckets": 17}, "high_tip": {"estimated": 1983, "actual": 1307, "error_percentage": 51.72, "buckets": 14}, "airport": {"estimated": 223, "actual": 170, "error_percentage": 31.18, "buckets": 9}},
    {"record_num": 53194400, "high_fare": {"estimated": 7445, "actual": 4759, "error_percentage": 56.44, "buckets": 18}, "long_distance": {"estimated": 5779, "actual": 3655, "error_percentage": 58.11, "buckets": 17}, "high_tip": {"estimated": 2075, "actual": 1675, "error_percentage": 23.88, "buckets": 16}, "airport": {"estimated": 625, "actual": 541, "error_percentage": 15.53, "buckets": 13}}
]

# Configuration
metrics = ['high_fare', 'long_distance', 'high_tip', 'airport']
colors = {'high_fare': '#e74c3c', 'long_distance': '#3498db', 'high_tip': '#2ecc71', 'airport': '#9b59b6'}
labels = {'high_fare': 'High Fare (>$20)', 'long_distance': 'Long Distance (>5mi)',
          'high_tip': 'High Tip (>20%)', 'airport': 'Airport Trips'}

record_nums = [s['record_num'] / 1e6 for s in snapshots]
output_dir = '/Users/shreya/SJSU-Github/DA228/'

# =============================================================================
# GRAPH 1: Real-Time Stream Estimates vs Historical Ground Truth
# =============================================================================
print("Creating Graph 1: Stream vs Ground Truth...")

fig1, ax1 = plt.subplots(figsize=(14, 8))

for metric in metrics:
    estimated = [s[metric]['estimated'] for s in snapshots]
    actual = [s[metric]['actual'] for s in snapshots]

    ax1.plot(record_nums, estimated, '-o', color=colors[metric], alpha=0.9,
             linewidth=2, markersize=4, label=f'{labels[metric]} - Stream')
    ax1.plot(record_nums, actual, '--', color=colors[metric], alpha=0.5,
             linewidth=1.5, label=f'{labels[metric]} - Ground Truth')

ax1.axvline(x=record_nums[-1], color='green', linestyle=':', alpha=0.7, linewidth=2)
ax1.text(record_nums[-1] + 0.5, ax1.get_ylim()[1] * 0.85, 'Current\nStream\nPosition',
         fontsize=9, color='green', fontweight='bold')

ax1.set_xlabel('Stream Position (Millions of Records Processed)', fontsize=12)
ax1.set_ylabel('Count in Sliding Window (10K records)', fontsize=12)
ax1.set_title('Real-Time DGIM Stream Estimates vs Historical Ground Truth\nNYC Taxi Data 2025',
              fontsize=14, fontweight='bold')
ax1.legend(loc='upper left', ncol=2, fontsize=9)
ax1.grid(True, alpha=0.3)
ax1.annotate('Solid = Real-time stream    Dashed = Batch ground truth',
            xy=(0.98, 0.02), xycoords='axes fraction', fontsize=10,
            ha='right', va='bottom', style='italic',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))

plt.tight_layout()
plt.savefig(f'{output_dir}graph1_stream_vs_groundtruth.png', dpi=150)
print("  Saved: graph1_stream_vs_groundtruth.png")

# =============================================================================
# GRAPH 2: Streaming Accuracy - Error Over Time
# =============================================================================
print("Creating Graph 2: Error Over Time...")

fig2, ax2 = plt.subplots(figsize=(12, 7))

for metric in metrics:
    errors = [s[metric]['error_percentage'] for s in snapshots]
    ax2.plot(record_nums, errors, '-o', color=colors[metric], markersize=5,
             linewidth=2, label=labels[metric], alpha=0.8)

ax2.axhline(y=50, color='red', linestyle='--', alpha=0.7, linewidth=2)
ax2.text(record_nums[0] + 1, 52, 'DGIM Theoretical Bound (50%)', fontsize=10, color='red', fontweight='bold')

ax2.set_xlabel('Stream Position (Millions of Records Processed)', fontsize=12)
ax2.set_ylabel('Approximation Error (%)', fontsize=12)
ax2.set_title('Streaming Accuracy: Real-Time DGIM Approximation Error\nNYC Taxi Data 2025',
              fontsize=14, fontweight='bold')
ax2.legend(loc='upper right', fontsize=10)
ax2.grid(True, alpha=0.3)
ax2.set_ylim(0, 80)

plt.tight_layout()
plt.savefig(f'{output_dir}graph2_error_over_time.png', dpi=150)
print("  Saved: graph2_error_over_time.png")

# =============================================================================
# GRAPH 3: Memory Efficiency - Bucket Count Over Stream
# =============================================================================
print("Creating Graph 3: Memory Efficiency...")

fig3, ax3 = plt.subplots(figsize=(12, 7))

for metric in metrics:
    buckets = [s[metric]['buckets'] for s in snapshots]
    ax3.plot(record_nums, buckets, '-s', color=colors[metric], markersize=5,
             linewidth=2, label=labels[metric], alpha=0.8)

# Theoretical O(log N)
ax3.axhline(y=np.log2(10000) * 2, color='gray', linestyle='--', alpha=0.6, linewidth=2)
ax3.text(record_nums[-1] * 0.6, np.log2(10000) * 2 + 1,
         f'~2*log2(10000) = {2*np.log2(10000):.0f} buckets', fontsize=10, color='gray')

ax3.set_xlabel('Stream Position (Millions of Records Processed)', fontsize=12)
ax3.set_ylabel('Number of DGIM Buckets', fontsize=12)
ax3.set_title('Memory Efficiency: DGIM Uses O(log N) Space\nNYC Taxi Data 2025',
              fontsize=14, fontweight='bold')
ax3.legend(loc='upper right', fontsize=10)
ax3.grid(True, alpha=0.3)
ax3.annotate('DGIM stores ~20 buckets instead of 10,000 records (99.8% savings)',
            xy=(0.02, 0.02), xycoords='axes fraction', fontsize=10,
            ha='left', va='bottom', style='italic',
            bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.8))

plt.tight_layout()
plt.savefig(f'{output_dir}graph3_memory_efficiency.png', dpi=150)
print("  Saved: graph3_memory_efficiency.png")

# =============================================================================
# GRAPH 4: Current Window - Bar Chart Comparison
# =============================================================================
print("Creating Graph 4: Current Window Comparison...")

fig4, ax4 = plt.subplots(figsize=(10, 7))

x = np.arange(len(metrics))
width = 0.35

final_estimated = [snapshots[-1][m]['estimated'] for m in metrics]
final_actual = [snapshots[-1][m]['actual'] for m in metrics]

bars1 = ax4.bar(x - width/2, final_estimated, width,
                label='Real-Time Stream Estimate',
                color=[colors[m] for m in metrics], alpha=0.9, edgecolor='black')
bars2 = ax4.bar(x + width/2, final_actual, width,
                label='Historical Ground Truth',
                color=[colors[m] for m in metrics], alpha=0.4, hatch='//', edgecolor='black')

ax4.set_xlabel('Trip Type', fontsize=12)
ax4.set_ylabel('Count in Current Window', fontsize=12)
ax4.set_title('Current Window: Stream Estimate vs Ground Truth\nNYC Taxi Data 2025',
              fontsize=14, fontweight='bold')
ax4.set_xticks(x)
ax4.set_xticklabels([labels[m].split('(')[0].strip() for m in metrics], fontsize=10)
ax4.legend(fontsize=10, loc='upper right')
ax4.grid(True, alpha=0.3, axis='y')

# Add error labels
for i, (est, act) in enumerate(zip(final_estimated, final_actual)):
    if act > 0:
        err = ((est - act) / act) * 100
        y_pos = max(est, act) + 200
        ax4.annotate(f'+{err:.1f}%' if err > 0 else f'{err:.1f}%',
                    xy=(i, y_pos), ha='center', fontsize=10,
                    color='red' if err > 30 else 'green', fontweight='bold')

plt.tight_layout()
plt.savefig(f'{output_dir}graph4_current_window.png', dpi=150)
print("  Saved: graph4_current_window.png")

# =============================================================================
# Show all graphs
# =============================================================================
print("\n" + "="*50)
print("All graphs saved successfully!")
print("="*50)

plt.show()
