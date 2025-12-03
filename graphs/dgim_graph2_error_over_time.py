"""
Graph 2: Streaming Accuracy - Error Percentage Over Time
Shows how DGIM approximation error fluctuates as stream progresses
"""

import matplotlib.pyplot as plt
import numpy as np

# Your DGIM metrics data (hardcoded)
snapshots = [
    {"record_num": 2659720, "high_fare": {"error_percentage": 34.25}, "long_distance": {"error_percentage": 16.32}, "high_tip": {"error_percentage": 53.94}, "airport": {"error_percentage": 39.35}},
    {"record_num": 5319440, "high_fare": {"error_percentage": 43.94}, "long_distance": {"error_percentage": 29.53}, "high_tip": {"error_percentage": 36.74}, "airport": {"error_percentage": 11.05}},
    {"record_num": 7979160, "high_fare": {"error_percentage": 21.86}, "long_distance": {"error_percentage": 25.29}, "high_tip": {"error_percentage": 50.0}, "airport": {"error_percentage": 20.86}},
    {"record_num": 10638880, "high_fare": {"error_percentage": 0}, "long_distance": {"error_percentage": 0}, "high_tip": {"error_percentage": 0}, "airport": {"error_percentage": 26.67}},
    {"record_num": 13298600, "high_fare": {"error_percentage": 11.6}, "long_distance": {"error_percentage": 59.41}, "high_tip": {"error_percentage": 22.95}, "airport": {"error_percentage": 69.27}},
    {"record_num": 15958320, "high_fare": {"error_percentage": 55.17}, "long_distance": {"error_percentage": 59.4}, "high_tip": {"error_percentage": 29.15}, "airport": {"error_percentage": 25.29}},
    {"record_num": 18618040, "high_fare": {"error_percentage": 34.33}, "long_distance": {"error_percentage": 11.75}, "high_tip": {"error_percentage": 30.9}, "airport": {"error_percentage": 54.65}},
    {"record_num": 21277760, "high_fare": {"error_percentage": 10.67}, "long_distance": {"error_percentage": 58.69}, "high_tip": {"error_percentage": 13.91}, "airport": {"error_percentage": 42.57}},
    {"record_num": 23937480, "high_fare": {"error_percentage": 27.13}, "long_distance": {"error_percentage": 23.7}, "high_tip": {"error_percentage": 60.53}, "airport": {"error_percentage": 51.17}},
    {"record_num": 26597200, "high_fare": {"error_percentage": 18.86}, "long_distance": {"error_percentage": 44.69}, "high_tip": {"error_percentage": 17.59}, "airport": {"error_percentage": 46.77}},
    {"record_num": 29256920, "high_fare": {"error_percentage": 13.54}, "long_distance": {"error_percentage": 33.6}, "high_tip": {"error_percentage": 35.05}, "airport": {"error_percentage": 23.89}},
    {"record_num": 31916640, "high_fare": {"error_percentage": 43.51}, "long_distance": {"error_percentage": 30.84}, "high_tip": {"error_percentage": 64.05}, "airport": {"error_percentage": 8.36}},
    {"record_num": 34576360, "high_fare": {"error_percentage": 24.39}, "long_distance": {"error_percentage": 52.36}, "high_tip": {"error_percentage": 25.75}, "airport": {"error_percentage": 46.48}},
    {"record_num": 37236080, "high_fare": {"error_percentage": 12.15}, "long_distance": {"error_percentage": 28.55}, "high_tip": {"error_percentage": 33.01}, "airport": {"error_percentage": 54.22}},
    {"record_num": 39895800, "high_fare": {"error_percentage": 34.08}, "long_distance": {"error_percentage": 54.69}, "high_tip": {"error_percentage": 28.33}, "airport": {"error_percentage": 27.47}},
    {"record_num": 42555520, "high_fare": {"error_percentage": 60.47}, "long_distance": {"error_percentage": 25.78}, "high_tip": {"error_percentage": 64.6}, "airport": {"error_percentage": 39.73}},
    {"record_num": 45215240, "high_fare": {"error_percentage": 59.51}, "long_distance": {"error_percentage": 31.13}, "high_tip": {"error_percentage": 19.36}, "airport": {"error_percentage": 52.35}},
    {"record_num": 47874960, "high_fare": {"error_percentage": 15.53}, "long_distance": {"error_percentage": 15.86}, "high_tip": {"error_percentage": 20.72}, "airport": {"error_percentage": 54.05}},
    {"record_num": 50534680, "high_fare": {"error_percentage": 34.86}, "long_distance": {"error_percentage": 45.42}, "high_tip": {"error_percentage": 51.72}, "airport": {"error_percentage": 31.18}},
    {"record_num": 53194400, "high_fare": {"error_percentage": 56.44}, "long_distance": {"error_percentage": 58.11}, "high_tip": {"error_percentage": 23.88}, "airport": {"error_percentage": 15.53}}
]

# Configuration
metrics = ['high_fare', 'long_distance', 'high_tip', 'airport']
colors = {'high_fare': '#e74c3c', 'long_distance': '#3498db', 'high_tip': '#2ecc71', 'airport': '#9b59b6'}
labels = {'high_fare': 'High Fare (>$20)', 'long_distance': 'Long Distance (>5mi)',
          'high_tip': 'High Tip (>20%)', 'airport': 'Airport Trips'}

record_nums = [s['record_num'] / 1e6 for s in snapshots]

# Create figure
fig, ax = plt.subplots(figsize=(12, 7))

for metric in metrics:
    errors = [s[metric]['error_percentage'] for s in snapshots]
    ax.plot(record_nums, errors, '-o', color=colors[metric], markersize=5,
            linewidth=2, label=labels[metric], alpha=0.8)

# DGIM theoretical bound (50%)
ax.axhline(y=50, color='red', linestyle='--', alpha=0.7, linewidth=2)
ax.text(record_nums[0] + 1, 52, 'DGIM Theoretical Bound (50%)', fontsize=10, color='red', fontweight='bold')

ax.set_xlabel('Stream Position (Millions of Records Processed)', fontsize=12)
ax.set_ylabel('Approximation Error (%)', fontsize=12)
ax.set_title('Streaming Accuracy: Real-Time DGIM Approximation Error\nNYC Taxi Data 2025',
             fontsize=14, fontweight='bold')
ax.legend(loc='upper right', fontsize=10)
ax.grid(True, alpha=0.3)
ax.set_ylim(0, 80)

# Add annotation
ax.annotate('Lower is better - DGIM guarantees max 50% error',
           xy=(0.02, 0.02), xycoords='axes fraction', fontsize=10,
           ha='left', va='bottom', style='italic',
           bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.8))

plt.tight_layout()
plt.savefig('/Users/shreya/SJSU-Github/DA228/graph2_error_over_time.png', dpi=150)
plt.show()

print("Saved: graph2_error_over_time.png")
