"""
Graph 1: Real-Time Stream Estimates vs Historical Ground Truth
Shows DGIM streaming estimates compared to batch-computed ground truth over time
"""

import matplotlib.pyplot as plt
import numpy as np

# Your DGIM metrics data (hardcoded)
snapshots = [
    {"record_num": 2659720, "high_fare": {"estimated": 4171, "actual": 3107}, "long_distance": {"estimated": 2473, "actual": 2126}, "high_tip": {"estimated": 919, "actual": 597}, "airport": {"estimated": 2231, "actual": 1601}},
    {"record_num": 5319440, "high_fare": {"estimated": 3767, "actual": 2617}, "long_distance": {"estimated": 2171, "actual": 1676}, "high_tip": {"estimated": 815, "actual": 596}, "airport": {"estimated": 1055, "actual": 950}},
    {"record_num": 7979160, "high_fare": {"estimated": 6483, "actual": 5320}, "long_distance": {"estimated": 2081, "actual": 1661}, "high_tip": {"estimated": 15, "actual": 30}, "airport": {"estimated": 197, "actual": 163}},
    {"record_num": 10638880, "high_fare": {"estimated": 0, "actual": 0}, "long_distance": {"estimated": 0, "actual": 0}, "high_tip": {"estimated": 0, "actual": 0}, "airport": {"estimated": 247, "actual": 195}},
    {"record_num": 13298600, "high_fare": {"estimated": 4405, "actual": 3947}, "long_distance": {"estimated": 3853, "actual": 2417}, "high_tip": {"estimated": 2239, "actual": 1821}, "airport": {"estimated": 931, "actual": 550}},
    {"record_num": 15958320, "high_fare": {"estimated": 7341, "actual": 4731}, "long_distance": {"estimated": 4095, "actual": 2569}, "high_tip": {"estimated": 2251, "actual": 1743}, "airport": {"estimated": 639, "actual": 510}},
    {"record_num": 18618040, "high_fare": {"estimated": 6503, "actual": 4841}, "long_distance": {"estimated": 4479, "actual": 4008}, "high_tip": {"estimated": 1847, "actual": 1411}, "airport": {"estimated": 1081, "actual": 699}},
    {"record_num": 21277760, "high_fare": {"estimated": 4915, "actual": 4441}, "long_distance": {"estimated": 3991, "actual": 2515}, "high_tip": {"estimated": 2145, "actual": 1883}, "airport": {"estimated": 931, "actual": 653}},
    {"record_num": 23937480, "high_fare": {"estimated": 6073, "actual": 4777}, "long_distance": {"estimated": 4233, "actual": 3422}, "high_tip": {"estimated": 2623, "actual": 1634}, "airport": {"estimated": 387, "actual": 256}},
    {"record_num": 26597200, "high_fare": {"estimated": 5711, "actual": 4805}, "long_distance": {"estimated": 4403, "actual": 3043}, "high_tip": {"estimated": 1959, "actual": 1666}, "airport": {"estimated": 1023, "actual": 697}},
    {"record_num": 29256920, "high_fare": {"estimated": 4637, "actual": 4084}, "long_distance": {"estimated": 4119, "actual": 3083}, "high_tip": {"estimated": 2277, "actual": 1686}, "airport": {"estimated": 923, "actual": 745}},
    {"record_num": 31916640, "high_fare": {"estimated": 8335, "actual": 5808}, "long_distance": {"estimated": 4323, "actual": 3304}, "high_tip": {"estimated": 2487, "actual": 1516}, "airport": {"estimated": 311, "actual": 287}},
    {"record_num": 34576360, "high_fare": {"estimated": 7603, "actual": 6112}, "long_distance": {"estimated": 8139, "actual": 5342}, "high_tip": {"estimated": 2207, "actual": 1755}, "airport": {"estimated": 4579, "actual": 3126}},
    {"record_num": 37236080, "high_fare": {"estimated": 5327, "actual": 4750}, "long_distance": {"estimated": 4287, "actual": 3335}, "high_tip": {"estimated": 2083, "actual": 1566}, "airport": {"estimated": 1479, "actual": 959}},
    {"record_num": 39895800, "high_fare": {"estimated": 8761, "actual": 6534}, "long_distance": {"estimated": 8635, "actual": 5582}, "high_tip": {"estimated": 2247, "actual": 1751}, "airport": {"estimated": 4241, "actual": 3327}},
    {"record_num": 42555520, "high_fare": {"estimated": 7741, "actual": 4824}, "long_distance": {"estimated": 4527, "actual": 3599}, "high_tip": {"estimated": 2887, "actual": 1754}, "airport": {"estimated": 1143, "actual": 818}},
    {"record_num": 45215240, "high_fare": {"estimated": 7441, "actual": 4665}, "long_distance": {"estimated": 4351, "actual": 3318}, "high_tip": {"estimated": 1837, "actual": 1539}, "airport": {"estimated": 617, "actual": 405}},
    {"record_num": 47874960, "high_fare": {"estimated": 6367, "actual": 5511}, "long_distance": {"estimated": 4595, "actual": 3966}, "high_tip": {"estimated": 1981, "actual": 1641}, "airport": {"estimated": 2055, "actual": 1334}},
    {"record_num": 50534680, "high_fare": {"estimated": 9079, "actual": 6732}, "long_distance": {"estimated": 4617, "actual": 3175}, "high_tip": {"estimated": 1983, "actual": 1307}, "airport": {"estimated": 223, "actual": 170}},
    {"record_num": 53194400, "high_fare": {"estimated": 7445, "actual": 4759}, "long_distance": {"estimated": 5779, "actual": 3655}, "high_tip": {"estimated": 2075, "actual": 1675}, "airport": {"estimated": 625, "actual": 541}}
]

# Configuration
metrics = ['high_fare', 'long_distance', 'high_tip', 'airport']
colors = {'high_fare': '#e74c3c', 'long_distance': '#3498db', 'high_tip': '#2ecc71', 'airport': '#9b59b6'}
labels = {'high_fare': 'High Fare (>$20)', 'long_distance': 'Long Distance (>5mi)',
          'high_tip': 'High Tip (>20%)', 'airport': 'Airport Trips'}

record_nums = [s['record_num'] / 1e6 for s in snapshots]

# Create figure
fig, ax = plt.subplots(figsize=(14, 8))

for metric in metrics:
    estimated = [s[metric]['estimated'] for s in snapshots]
    actual = [s[metric]['actual'] for s in snapshots]

    # Stream estimate (solid, prominent)
    ax.plot(record_nums, estimated, '-o', color=colors[metric], alpha=0.9,
            linewidth=2, markersize=4, label=f'{labels[metric]} - Stream')
    # Ground truth (dashed, reference)
    ax.plot(record_nums, actual, '--', color=colors[metric], alpha=0.5,
            linewidth=1.5, label=f'{labels[metric]} - Ground Truth')

# Add stream progress indicator
ax.axvline(x=record_nums[-1], color='green', linestyle=':', alpha=0.7, linewidth=2)
ax.text(record_nums[-1] + 0.5, ax.get_ylim()[1] * 0.85, 'Current\nStream\nPosition',
        fontsize=9, color='green', fontweight='bold')

ax.set_xlabel('Stream Position (Millions of Records Processed)', fontsize=12)
ax.set_ylabel('Count in Sliding Window (10K records)', fontsize=12)
ax.set_title('Real-Time DGIM Stream Estimates vs Historical Ground Truth\nNYC Taxi Data 2025',
             fontsize=14, fontweight='bold')
ax.legend(loc='upper left', ncol=2, fontsize=9)
ax.grid(True, alpha=0.3)

# Add annotation
ax.annotate('Solid = Real-time stream    Dashed = Batch ground truth',
           xy=(0.98, 0.02), xycoords='axes fraction', fontsize=10,
           ha='right', va='bottom', style='italic',
           bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))

plt.tight_layout()
plt.savefig('/Users/shreya/SJSU-Github/DA228/graph1_stream_vs_groundtruth.png', dpi=150)
plt.show()

print("Saved: graph1_stream_vs_groundtruth.png")
