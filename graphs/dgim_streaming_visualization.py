import json
import sys
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.gridspec import GridSpec
from matplotlib.patches import FancyBboxPatch, Arrow

# ============================================================================
# Load DGIM metrics from JSON file
# ============================================================================
# Usage: python dgim_streaming_visualization.py <path_to_dgim_metrics.json>
# If no argument provided, uses default path

if len(sys.argv) > 1:
    json_file_path = sys.argv[1]
else:
    # Default path - update this to your DGIM output location
    json_file_path = '/Users/shreya/SJSU-Github/DA228/dgim_metrics.json'

print(f"Loading DGIM metrics from: {json_file_path}")

try:
    with open(json_file_path, 'r') as f:
        dgim_data = json.load(f)
    print("Successfully loaded DGIM metrics!")
except FileNotFoundError:
    print(f"Error: File not found at {json_file_path}")
    print("Please provide the path to your DGIM metrics JSON file:")
    print("  python dgim_streaming_visualization.py <path_to_json>")
    sys.exit(1)
except json.JSONDecodeError as e:
    print(f"Error: Invalid JSON format - {e}")
    sys.exit(1)

# Configuration
metrics = ['high_fare', 'long_distance', 'high_tip', 'airport']
colors = {'high_fare': '#e74c3c', 'long_distance': '#3498db', 'high_tip': '#2ecc71', 'airport': '#9b59b6'}
labels = {'high_fare': 'High Fare (>$20)', 'long_distance': 'Long Distance (>5mi)',
          'high_tip': 'High Tip (>20%)', 'airport': 'Airport Trips'}

snapshots = dgim_data['window_snapshots']
record_nums = [s['record_num'] / 1e6 for s in snapshots]
total_records = dgim_data['run_metadata']['total_records']

# Create main figure
fig = plt.figure(figsize=(18, 16))
gs = GridSpec(4, 2, figure=fig, hspace=0.35, wspace=0.25, height_ratios=[0.8, 1, 1, 1])

# ============================================================================
# Plot 0: Stream Processing Concept Diagram
# ============================================================================
ax0 = fig.add_subplot(gs[0, :])
ax0.set_xlim(0, 10)
ax0.set_ylim(0, 3)
ax0.axis('off')

# Draw stream flow
ax0.annotate('', xy=(9, 1.5), xytext=(1, 1.5),
            arrowprops=dict(arrowstyle='->', color='#3498db', lw=3))
ax0.text(5, 2.5, 'NYC Taxi Data Stream (53M+ Records)', fontsize=12, ha='center', fontweight='bold')
ax0.text(0.5, 1.5, 'INPUT\nSTREAM', fontsize=9, ha='center', va='center', fontweight='bold')

# Sliding window box
rect = FancyBboxPatch((4, 0.8), 2, 1.4, boxstyle="round,pad=0.05",
                       facecolor='#f39c12', alpha=0.3, edgecolor='#f39c12', linewidth=2)
ax0.add_patch(rect)
ax0.text(5, 1.5, 'DGIM\nSliding Window\n(10K records)', fontsize=9, ha='center', va='center', fontweight='bold')

# Output
ax0.text(9.5, 1.5, 'REAL-TIME\nESTIMATES', fontsize=9, ha='center', va='center', fontweight='bold', color='#27ae60')

# Memory indicator
ax0.text(5, 0.3, 'Memory: O(log N) buckets only', fontsize=8, ha='center', style='italic', color='#7f8c8d')

# ============================================================================
# Plot 1: Real-time Stream Estimates vs Historical Ground Truth
# ============================================================================
ax1 = fig.add_subplot(gs[1, :])

for metric in metrics:
    estimated = [s[metric]['estimated'] for s in snapshots]
    actual = [s[metric]['actual'] for s in snapshots]

    # Stream estimate (solid, prominent)
    ax1.plot(record_nums, estimated, '-', color=colors[metric], alpha=0.9,
             linewidth=2.5, label=f'{labels[metric]} - Stream Estimate')
    # Ground truth (dashed, reference)
    ax1.plot(record_nums, actual, '--', color=colors[metric], alpha=0.4,
             linewidth=1.5, label=f'{labels[metric]} - Ground Truth')

# Add stream progress indicator
ax1.axvline(x=record_nums[-1], color='green', linestyle=':', alpha=0.7, linewidth=2)
ax1.text(record_nums[-1] + 0.5, ax1.get_ylim()[1] * 0.9, 'Current\nStream\nPosition',
         fontsize=8, color='green', fontweight='bold')

ax1.set_xlabel('Stream Position (Millions of Records Processed)', fontsize=11, fontweight='bold')
ax1.set_ylabel('Count in Current Sliding Window', fontsize=11)
ax1.set_title('Real-Time DGIM Stream Estimates vs Historical Ground Truth',
              fontsize=14, fontweight='bold', color='#2c3e50')
ax1.legend(loc='upper left', ncol=2, fontsize=8, framealpha=0.9)
ax1.grid(True, alpha=0.3)

# Add annotation explaining the comparison
ax1.annotate('Solid lines = Real-time streaming estimates\nDashed lines = Batch-computed ground truth',
            xy=(0.98, 0.02), xycoords='axes fraction', fontsize=9,
            ha='right', va='bottom', style='italic',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))

# ============================================================================
# Plot 2: Streaming Accuracy Over Time
# ============================================================================
ax2 = fig.add_subplot(gs[2, 0])

for metric in metrics:
    errors = [s[metric]['error_percentage'] for s in snapshots]
    ax2.plot(record_nums, errors, '-o', color=colors[metric], markersize=4,
             linewidth=1.5, label=labels[metric], alpha=0.8)

# DGIM theoretical bound (50%)
ax2.axhline(y=50, color='red', linestyle='--', alpha=0.6, linewidth=2)
ax2.text(record_nums[0], 52, 'DGIM Theoretical Bound (50%)', fontsize=8, color='red')

ax2.set_xlabel('Stream Position (Millions)', fontsize=11)
ax2.set_ylabel('Approximation Error (%)', fontsize=11)
ax2.set_title('Streaming Accuracy: Real-Time Approximation Error',
              fontsize=12, fontweight='bold', color='#2c3e50')
ax2.legend(loc='upper right', fontsize=8)
ax2.grid(True, alpha=0.3)
ax2.set_ylim(0, 80)

# ============================================================================
# Plot 3: Memory Efficiency - Buckets Used Over Stream
# ============================================================================
ax3 = fig.add_subplot(gs[2, 1])

for metric in metrics:
    buckets = [s[metric]['buckets'] for s in snapshots]
    ax3.plot(record_nums, buckets, '-s', color=colors[metric], markersize=4,
             linewidth=1.5, label=labels[metric], alpha=0.8)

ax3.axhline(y=np.log2(10000) * 2, color='gray', linestyle='--', alpha=0.5)
ax3.text(record_nums[-1] * 0.7, np.log2(10000) * 2 + 1,
         f'~2*log2({dgim_data["window_configuration"]["window_size"]}) = {2*np.log2(10000):.0f}',
         fontsize=8, color='gray')

ax3.set_xlabel('Stream Position (Millions)', fontsize=11)
ax3.set_ylabel('Number of DGIM Buckets', fontsize=11)
ax3.set_title('Memory Efficiency: DGIM Bucket Count (O(log N) Space)',
              fontsize=12, fontweight='bold', color='#2c3e50')
ax3.legend(loc='upper right', fontsize=8)
ax3.grid(True, alpha=0.3)

# ============================================================================
# Plot 4: Current Window State - Stream vs Ground Truth Comparison
# ============================================================================
ax4 = fig.add_subplot(gs[3, 0])

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

ax4.set_xlabel('Trip Type', fontsize=11)
ax4.set_ylabel('Count in Current Window', fontsize=11)
ax4.set_title('Current Window State: Stream Estimate vs Ground Truth',
              fontsize=12, fontweight='bold', color='#2c3e50')
ax4.set_xticks(x)
ax4.set_xticklabels([labels[m].split('(')[0].strip() for m in metrics], rotation=15, ha='right')
ax4.legend(fontsize=9, loc='upper right')
ax4.grid(True, alpha=0.3, axis='y')

# Add error labels
for i, (est, act) in enumerate(zip(final_estimated, final_actual)):
    if act > 0:
        err = ((est - act) / act) * 100
        y_pos = max(est, act) + 200
        ax4.annotate(f'+{err:.1f}%' if err > 0 else f'{err:.1f}%',
                    xy=(i, y_pos), ha='center', fontsize=9,
                    color='red' if err > 30 else 'orange', fontweight='bold')

# ============================================================================
# Plot 5: Summary Statistics Panel
# ============================================================================
ax5 = fig.add_subplot(gs[3, 1])
ax5.axis('off')

# Create comprehensive summary
accuracy_lines = '\n'.join([f"  - {labels[m].split('(')[0].strip()}: {dgim_data['accuracy_metrics'][m]['error_percentage']:.1f}% error"
                            for m in metrics])

summary_text = f"""
REAL-TIME STREAM PROCESSING SUMMARY
============================================

Stream Configuration:
  - Total Records Processed: {total_records:,}
  - Sliding Window Size: {dgim_data['window_configuration']['window_size']:,} records
  - Processing Time: {dgim_data['run_metadata']['duration_seconds']:.1f} seconds
  - Throughput: {total_records / dgim_data['run_metadata']['duration_seconds']:,.0f} records/sec

DGIM Memory Efficiency:
  - Space Complexity: O(log N) per metric
  - Max Buckets Used: ~20 (vs 10,000 window)
  - Memory Savings: >99.8%

Streaming vs Batch Accuracy:
{accuracy_lines}

Key Insight:
  DGIM provides real-time estimates using
  minimal memory while batch ground truth
  requires storing all historical data.
"""

ax5.text(0.05, 0.95, summary_text, transform=ax5.transAxes, fontsize=9,
         verticalalignment='top', fontfamily='monospace',
         bbox=dict(boxstyle='round', facecolor='#ecf0f1', alpha=0.9, edgecolor='#bdc3c7'))

# Main title
plt.suptitle('DGIM Algorithm: Real-Time Stream Processing vs Historical Ground Truth\n'
             'NYC Taxi Trip Data - 2025 Dataset',
             fontsize=16, fontweight='bold', y=1.02, color='#2c3e50')

plt.tight_layout()
plt.savefig('/Users/shreya/SJSU-Github/DA228/dgim_streaming_analysis.png', dpi=150, bbox_inches='tight')
plt.show()

print("\n" + "="*60)
print("Visualization saved to: dgim_streaming_analysis.png")
print("="*60)

# ============================================================================
# Additional: Individual Metric Deep Dive
# ============================================================================
fig2, axes = plt.subplots(2, 2, figsize=(14, 10))
axes = axes.flatten()

for idx, metric in enumerate(metrics):
    ax = axes[idx]
    estimated = [s[metric]['estimated'] for s in snapshots]
    actual = [s[metric]['actual'] for s in snapshots]

    # Show the gap between stream and ground truth
    ax.fill_between(record_nums, actual, estimated, alpha=0.3, color=colors[metric],
                    label='Estimation Gap')
    ax.plot(record_nums, estimated, '-o', color=colors[metric], markersize=5,
            linewidth=2, label='Real-Time Stream')
    ax.plot(record_nums, actual, '--', color='black',
            linewidth=2, alpha=0.6, label='Ground Truth (Batch)')

    # Mark current position
    ax.axvline(x=record_nums[-1], color='green', linestyle=':', alpha=0.5)

    ax.set_xlabel('Stream Position (Millions)', fontsize=10)
    ax.set_ylabel('Count in Window', fontsize=10)
    ax.set_title(f'{labels[metric]}', fontsize=12, fontweight='bold')
    ax.legend(loc='upper left', fontsize=8)
    ax.grid(True, alpha=0.3)

    # Add final error annotation
    final_err = snapshots[-1][metric]['error_percentage']
    ax.annotate(f'Current Error: {final_err:.1f}%',
                xy=(0.98, 0.02), xycoords='axes fraction',
                fontsize=9, ha='right', va='bottom',
                bbox=dict(boxstyle='round', facecolor='yellow', alpha=0.7))

plt.suptitle('Real-Time Stream Estimates vs Historical Ground Truth\n'
             'Individual Metric Analysis - NYC Taxi 2025',
             fontsize=14, fontweight='bold')
plt.tight_layout()
plt.savefig('/Users/shreya/SJSU-Github/DA228/dgim_individual_stream_metrics.png', dpi=150, bbox_inches='tight')
plt.show()

print("Individual metrics saved to: dgim_individual_stream_metrics.png")
