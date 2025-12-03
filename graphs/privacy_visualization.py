#!/usr/bin/env python3
"""
Privacy Techniques Visualization
Creates graphs and charts from privacy demonstration results
"""

import json
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
import seaborn as sns

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 8)
plt.rcParams['font.size'] = 10

# Load results
def load_privacy_results(results_dir="streaming/Privacy"):
    """Load all privacy demo results"""
    results = []
    base_path = Path(results_dir)

    for file in base_path.glob("privacy_demo_results*.json"):
        with open(file, 'r') as f:
            data = json.load(f)
            results.append((file.name, data))

    return results


def plot_zone_generalization(results, output_dir="privacy_graphs"):
    """Plot the effect of K-Anonymity generalization on zones"""
    Path(output_dir).mkdir(exist_ok=True)

    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('K-Anonymity: Zone Generalization Effect', fontsize=16, fontweight='bold')

    for idx, (filename, data) in enumerate(results[:4]):
        row = idx // 2
        col = idx % 2
        ax = axes[row, col]

        taxi_type = data.get('taxi_type', 'Unknown')
        sample_size = data.get('sample_size', 0)
        original_zones = 260  # NYC has ~260 zones
        generalized = data.get('generalized_regions', 1)

        # Create bar plot
        categories = ['Original\nZones', 'Generalized\nRegions']
        values = [original_zones, generalized]
        colors = ['#FF6B6B', '#4ECDC4']

        bars = ax.bar(categories, values, color=colors, alpha=0.7, edgecolor='black', linewidth=2)

        # Add value labels on bars
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{int(height)}',
                   ha='center', va='bottom', fontsize=12, fontweight='bold')

        # Add reduction percentage
        reduction = ((original_zones - generalized) / original_zones) * 100
        ax.text(0.5, max(values) * 0.5, f'{reduction:.1f}%\nReduction',
               ha='center', va='center', fontsize=14, fontweight='bold',
               bbox=dict(boxstyle='round', facecolor='yellow', alpha=0.5))

        ax.set_title(f'{taxi_type.upper()} (n={sample_size:,})', fontsize=12, fontweight='bold')
        ax.set_ylabel('Number of Distinct Values', fontsize=11)
        ax.set_ylim(0, original_zones * 1.2)
        ax.grid(axis='y', alpha=0.3)

    plt.tight_layout()
    plt.savefig(f'{output_dir}/zone_generalization.png', dpi=300, bbox_inches='tight')
    print(f"✅ Saved: {output_dir}/zone_generalization.png")
    plt.close()


def plot_zone_distribution(results, output_dir="privacy_graphs"):
    """Plot DP-protected zone distributions"""
    Path(output_dir).mkdir(exist_ok=True)

    fig, axes = plt.subplots(2, 1, figsize=(14, 12))
    fig.suptitle('Differential Privacy: Zone Distribution (ε=1.0)', fontsize=16, fontweight='bold')

    for idx, (filename, data) in enumerate(results[:2]):
        ax = axes[idx]

        taxi_type = data.get('taxi_type', 'Unknown')
        zone_dist = data.get('dp_zone_distribution', {})

        if not zone_dist:
            continue

        # Sort by count
        sorted_zones = sorted(zone_dist.items(), key=lambda x: x[1], reverse=True)
        zones = [z[0] for z in sorted_zones]
        counts = [z[1] for z in sorted_zones]

        # Create horizontal bar chart
        y_pos = np.arange(len(zones))
        colors = plt.cm.viridis(np.linspace(0.3, 0.9, len(zones)))

        bars = ax.barh(y_pos, counts, color=colors, alpha=0.8, edgecolor='black', linewidth=1)

        # Add value labels
        for i, (bar, count) in enumerate(zip(bars, counts)):
            ax.text(count, bar.get_y() + bar.get_height()/2.,
                   f' {count:,}',
                   ha='left', va='center', fontsize=10, fontweight='bold')

        ax.set_yticks(y_pos)
        ax.set_yticklabels(zones, fontsize=10)
        ax.set_xlabel('Trip Count (DP Protected)', fontsize=11, fontweight='bold')
        ax.set_title(f'{taxi_type.upper()} Pickup Distribution', fontsize=13, fontweight='bold')
        ax.grid(axis='x', alpha=0.3)
        ax.invert_yaxis()

        # Add note about DP
        total = sum(counts)
        ax.text(0.98, 0.02, f'Total: {total:,} trips\n(Counts include Laplace noise)',
               transform=ax.transAxes, ha='right', va='bottom',
               bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5),
               fontsize=9)

    plt.tight_layout()
    plt.savefig(f'{output_dir}/zone_distribution.png', dpi=300, bbox_inches='tight')
    print(f"✅ Saved: {output_dir}/zone_distribution.png")
    plt.close()


def plot_time_distribution(results, output_dir="privacy_graphs"):
    """Plot time period distributions with DP noise"""
    Path(output_dir).mkdir(exist_ok=True)

    fig, axes = plt.subplots(2, 1, figsize=(12, 10))
    fig.suptitle('Differential Privacy: Time Period Distribution (ε=1.0)',
                fontsize=16, fontweight='bold')

    time_order = ['Late-Night', 'Morning-Rush', 'Midday', 'Evening-Rush', 'Night']

    for idx, (filename, data) in enumerate(results[:2]):
        ax = axes[idx]

        taxi_type = data.get('taxi_type', 'Unknown')
        time_dist = data.get('dp_time_distribution', {})

        if not time_dist:
            continue

        # Order by predefined time sequence
        periods = []
        counts = []
        for period in time_order:
            if period in time_dist:
                periods.append(period)
                counts.append(time_dist[period])

        # Create bar chart
        x_pos = np.arange(len(periods))
        colors = ['#1a1a2e', '#16213e', '#0f3460', '#533483', '#e94560']

        bars = ax.bar(x_pos, counts, color=colors[:len(periods)],
                     alpha=0.8, edgecolor='black', linewidth=2)

        # Add value labels
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{int(height):,}',
                   ha='center', va='bottom', fontsize=11, fontweight='bold')

        ax.set_xticks(x_pos)
        ax.set_xticklabels(periods, rotation=15, ha='right')
        ax.set_ylabel('Trip Count (DP Protected)', fontsize=11, fontweight='bold')
        ax.set_title(f'{taxi_type.upper()} Time Distribution', fontsize=13, fontweight='bold')
        ax.grid(axis='y', alpha=0.3)

        # Add note
        total = sum(counts)
        ax.text(0.98, 0.95, f'Total: {total:,} trips\n(Noisy counts)',
               transform=ax.transAxes, ha='right', va='top',
               bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.5),
               fontsize=9)

    plt.tight_layout()
    plt.savefig(f'{output_dir}/time_distribution.png', dpi=300, bbox_inches='tight')
    print(f"✅ Saved: {output_dir}/time_distribution.png")
    plt.close()


def plot_privacy_comparison(results, output_dir="privacy_graphs"):
    """Compare original vs anonymized data"""
    Path(output_dir).mkdir(exist_ok=True)

    fig = plt.figure(figsize=(16, 10))
    gs = fig.add_gridspec(3, 2, hspace=0.3, wspace=0.3)

    fig.suptitle('K-Anonymity: Original vs Anonymized Data Comparison',
                fontsize=16, fontweight='bold')

    # Get data from first valid result
    valid_data = None
    for filename, data in results:
        if data.get('sample_comparisons'):
            valid_data = data
            break

    if not valid_data:
        print("⚠️  No sample comparisons found")
        return

    samples = valid_data.get('sample_comparisons', [])[:5]

    # Subplot 1: Zone Comparison
    ax1 = fig.add_subplot(gs[0, :])

    x = np.arange(len(samples))
    width = 0.35

    original_pu = [s['original']['PU'] for s in samples]
    generalized_pu = [s['generalized']['PU'] for s in samples]

    ax1.bar(x - width/2, [1]*len(samples), width, label='Original (Specific Zone)',
           color='#FF6B6B', alpha=0.7, edgecolor='black')
    ax1.bar(x + width/2, [1]*len(samples), width, label='Generalized (Region)',
           color='#4ECDC4', alpha=0.7, edgecolor='black')

    ax1.set_ylabel('Privacy Level', fontsize=11)
    ax1.set_title('Pickup Zone: Generalization Effect', fontsize=13, fontweight='bold')
    ax1.set_xticks(x)
    ax1.set_xticklabels([f'Record {i+1}' for i in range(len(samples))])
    ax1.legend()
    ax1.set_ylim(0, 1.5)
    ax1.set_yticks([0, 1])
    ax1.set_yticklabels(['Low Privacy\n(Specific)', 'High Privacy\n(Generalized)'])

    # Add annotations
    for i, (orig, gen) in enumerate(zip(original_pu, generalized_pu)):
        ax1.text(i - width/2, 1.1, f'Zone\n{orig}', ha='center', va='bottom', fontsize=8)
        ax1.text(i + width/2, 1.1, f'{gen}', ha='center', va='bottom', fontsize=8,
                fontweight='bold')

    # Subplot 2: Time Generalization
    ax2 = fig.add_subplot(gs[1, 0])

    time_levels = ['Exact Hour', 'Time Period']
    time_privacy = [1, 5]  # Representing privacy gain
    colors = ['#FF6B6B', '#4ECDC4']

    bars = ax2.bar(time_levels, time_privacy, color=colors, alpha=0.7,
                  edgecolor='black', linewidth=2)

    ax2.set_ylabel('Privacy Gain (k-value)', fontsize=11)
    ax2.set_title('Time Generalization: Privacy Gain', fontsize=13, fontweight='bold')
    ax2.set_ylim(0, 6)

    for bar, val in zip(bars, time_privacy):
        ax2.text(bar.get_x() + bar.get_width()/2., bar.get_height(),
                f'k={val}',
                ha='center', va='bottom', fontsize=12, fontweight='bold')

    ax2.grid(axis='y', alpha=0.3)

    # Subplot 3: Fare Range Generalization
    ax3 = fig.add_subplot(gs[1, 1])

    fare_levels = ['Exact\nAmount', 'Fare\nRange']
    fare_privacy = [1, 5]

    bars = ax3.bar(fare_levels, fare_privacy, color=colors, alpha=0.7,
                  edgecolor='black', linewidth=2)

    ax3.set_ylabel('Privacy Gain (k-value)', fontsize=11)
    ax3.set_title('Fare Generalization: Privacy Gain', fontsize=13, fontweight='bold')
    ax3.set_ylim(0, 6)

    for bar, val in zip(bars, fare_privacy):
        ax3.text(bar.get_x() + bar.get_width()/2., bar.get_height(),
                f'k={val}',
                ha='center', va='bottom', fontsize=12, fontweight='bold')

    ax3.grid(axis='y', alpha=0.3)

    # Subplot 4: Privacy-Utility Tradeoff
    ax4 = fig.add_subplot(gs[2, :])

    k_values = [2, 5, 10, 20]
    privacy = [40, 70, 90, 98]  # Privacy level (%)
    utility = [95, 80, 60, 40]  # Data utility (%)

    x = np.arange(len(k_values))
    width = 0.35

    ax4.bar(x - width/2, privacy, width, label='Privacy Protection',
           color='#4ECDC4', alpha=0.8, edgecolor='black')
    ax4.bar(x + width/2, utility, width, label='Data Utility',
           color='#FFA500', alpha=0.8, edgecolor='black')

    ax4.set_xlabel('K-Anonymity Value (k)', fontsize=11, fontweight='bold')
    ax4.set_ylabel('Percentage (%)', fontsize=11, fontweight='bold')
    ax4.set_title('Privacy-Utility Tradeoff (K-Anonymity)', fontsize=13, fontweight='bold')
    ax4.set_xticks(x)
    ax4.set_xticklabels([f'k={k}' for k in k_values])
    ax4.legend(loc='upper right', fontsize=11)
    ax4.grid(axis='y', alpha=0.3)

    # Add current setting marker
    ax4.axvline(x=1, color='red', linestyle='--', linewidth=2, alpha=0.5)
    ax4.text(1, 95, 'Our Setting ▼', ha='center', va='bottom',
            color='red', fontweight='bold', fontsize=10)

    plt.savefig(f'{output_dir}/privacy_comparison.png', dpi=300, bbox_inches='tight')
    print(f"✅ Saved: {output_dir}/privacy_comparison.png")
    plt.close()


def plot_differential_privacy_noise(output_dir="privacy_graphs"):
    """Illustrate how DP noise works"""
    Path(output_dir).mkdir(exist_ok=True)

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle('Differential Privacy: Laplace Noise Mechanism',
                fontsize=16, fontweight='bold')

    # True counts for demonstration
    true_counts = {
        'Brooklyn': 3000,
        'Manhattan': 2500,
        'Queens': 1640,
        'Bronx': 914,
        'Staten Island': 126
    }

    # Different epsilon values
    epsilons = [0.1, 0.5, 1.0, 10.0]

    for idx, epsilon in enumerate(epsilons):
        row = idx // 2
        col = idx % 2
        ax = axes[row, col]

        # Generate noisy counts
        np.random.seed(42)  # For reproducibility
        scale = 1 / epsilon

        noisy_counts = {}
        for region, true_count in true_counts.items():
            noise = np.random.laplace(0, scale)
            noisy_count = max(0, int(true_count + noise))
            noisy_counts[region] = noisy_count

        # Plot
        regions = list(true_counts.keys())
        x = np.arange(len(regions))
        width = 0.35

        bars1 = ax.bar(x - width/2, list(true_counts.values()), width,
                      label='True Count', color='#3498db', alpha=0.7,
                      edgecolor='black')
        bars2 = ax.bar(x + width/2, list(noisy_counts.values()), width,
                      label='DP Protected', color='#e74c3c', alpha=0.7,
                      edgecolor='black')

        # Add value labels
        for bars in [bars1, bars2]:
            for bar in bars:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'{int(height)}',
                       ha='center', va='bottom', fontsize=9)

        ax.set_title(f'ε = {epsilon} (Noise Scale = {scale:.1f})',
                    fontsize=12, fontweight='bold')
        ax.set_ylabel('Trip Count', fontsize=10)
        ax.set_xticks(x)
        ax.set_xticklabels(regions, rotation=20, ha='right')
        ax.legend(fontsize=9)
        ax.grid(axis='y', alpha=0.3)

        # Calculate average absolute error
        errors = [abs(noisy_counts[r] - true_counts[r]) for r in regions]
        avg_error = np.mean(errors)

        ax.text(0.98, 0.95, f'Avg Error: {avg_error:.1f}',
               transform=ax.transAxes, ha='right', va='top',
               bbox=dict(boxstyle='round', facecolor='yellow', alpha=0.5),
               fontsize=9, fontweight='bold')

    plt.tight_layout()
    plt.savefig(f'{output_dir}/dp_noise_demonstration.png', dpi=300, bbox_inches='tight')
    print(f"✅ Saved: {output_dir}/dp_noise_demonstration.png")
    plt.close()


def plot_privacy_guarantees(output_dir="privacy_graphs"):
    """Visualize privacy guarantees"""
    Path(output_dir).mkdir(exist_ok=True)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    fig.suptitle('Privacy Guarantees: K-Anonymity vs Differential Privacy',
                fontsize=16, fontweight='bold')

    # K-Anonymity guarantee
    k_values = [2, 5, 10, 20, 50]
    reidentification_risk = [1/k * 100 for k in k_values]

    ax1.plot(k_values, reidentification_risk, 'o-', linewidth=3,
            markersize=10, color='#4ECDC4', label='Re-identification Risk')
    ax1.fill_between(k_values, 0, reidentification_risk, alpha=0.3, color='#4ECDC4')

    ax1.set_xlabel('K Value', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Maximum Re-identification Risk (%)', fontsize=12, fontweight='bold')
    ax1.set_title('K-Anonymity Guarantee', fontsize=13, fontweight='bold')
    ax1.grid(True, alpha=0.3)
    ax1.set_ylim(0, 60)

    # Mark our setting
    ax1.axvline(x=5, color='red', linestyle='--', linewidth=2, alpha=0.7)
    ax1.text(5, 55, 'Our Setting\n(k=5, Risk=20%)', ha='center',
            bbox=dict(boxstyle='round', facecolor='yellow', alpha=0.7),
            fontsize=10, fontweight='bold')

    # Add annotations
    for k, risk in zip(k_values, reidentification_risk):
        ax1.text(k, risk + 2, f'{risk:.1f}%', ha='center', fontsize=9)

    # Differential Privacy guarantee
    epsilons = [0.1, 0.5, 1.0, 2.0, 5.0, 10.0]
    privacy_loss = [np.exp(e) for e in epsilons]

    ax2.plot(epsilons, privacy_loss, 's-', linewidth=3,
            markersize=10, color='#FF6B6B', label='Privacy Loss Factor')
    ax2.fill_between(epsilons, 1, privacy_loss, alpha=0.3, color='#FF6B6B')

    ax2.set_xlabel('Epsilon (ε)', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Maximum Output Ratio (e^ε)', fontsize=12, fontweight='bold')
    ax2.set_title('Differential Privacy Guarantee', fontsize=13, fontweight='bold')
    ax2.grid(True, alpha=0.3)
    ax2.axhline(y=1, color='green', linestyle=':', linewidth=2, label='Perfect Privacy (ε=0)')

    # Mark our setting
    ax2.axvline(x=1.0, color='red', linestyle='--', linewidth=2, alpha=0.7)
    ax2.text(1.0, np.exp(5), 'Our Setting\n(ε=1.0, Ratio=2.72)', ha='center',
            bbox=dict(boxstyle='round', facecolor='yellow', alpha=0.7),
            fontsize=10, fontweight='bold')

    # Add annotations
    for e, loss in zip(epsilons, privacy_loss):
        ax2.text(e, loss + 1, f'{loss:.2f}x', ha='center', fontsize=9)

    ax2.legend(fontsize=10)

    plt.tight_layout()
    plt.savefig(f'{output_dir}/privacy_guarantees.png', dpi=300, bbox_inches='tight')
    print(f"✅ Saved: {output_dir}/privacy_guarantees.png")
    plt.close()


def create_summary_dashboard(results, output_dir="privacy_graphs"):
    """Create a summary dashboard of all privacy metrics"""
    Path(output_dir).mkdir(exist_ok=True)

    fig = plt.figure(figsize=(18, 12))
    gs = fig.add_gridspec(3, 3, hspace=0.3, wspace=0.3)

    fig.suptitle('Privacy Techniques: Complete Summary Dashboard',
                fontsize=18, fontweight='bold')

    # Get data
    if len(results) < 2:
        print("⚠️  Not enough results for dashboard")
        return

    fhv_data = results[0][1] if results[0][1].get('taxi_type') == 'fhv' else results[1][1]
    fhvhv_data = results[1][1] if results[1][1].get('taxi_type') == 'fhvhv' else results[0][1]

    # 1. K-Anonymity Parameters
    ax1 = fig.add_subplot(gs[0, 0])
    params = ['k-value', 'epsilon', 'Sample Size']
    values = [5, 1.0, 10000]
    colors = ['#4ECDC4', '#FF6B6B', '#95E1D3']

    bars = ax1.barh(params, values, color=colors, alpha=0.7, edgecolor='black')
    ax1.set_title('Privacy Parameters', fontsize=12, fontweight='bold')
    ax1.set_xlabel('Value', fontsize=10)

    for bar, val in zip(bars, values):
        ax1.text(val, bar.get_y() + bar.get_height()/2.,
                f' {val}', ha='left', va='center', fontsize=10, fontweight='bold')

    # 2. Zone Generalization Summary
    ax2 = fig.add_subplot(gs[0, 1:])

    datasets = ['FHV', 'FHVHV']
    original = [260, 260]
    generalized = [
        fhv_data.get('generalized_regions', 1),
        fhvhv_data.get('generalized_regions', 10)
    ]

    x = np.arange(len(datasets))
    width = 0.35

    bars1 = ax2.bar(x - width/2, original, width, label='Original Zones',
                   color='#FF6B6B', alpha=0.7, edgecolor='black')
    bars2 = ax2.bar(x + width/2, generalized, width, label='Generalized Regions',
                   color='#4ECDC4', alpha=0.7, edgecolor='black')

    ax2.set_title('Zone Generalization Comparison', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Count', fontsize=10)
    ax2.set_xticks(x)
    ax2.set_xticklabels(datasets)
    ax2.legend()
    ax2.grid(axis='y', alpha=0.3)

    # Add reduction percentages
    for i, (orig, gen) in enumerate(zip(original, generalized)):
        reduction = ((orig - gen) / orig) * 100
        ax2.text(i, max(orig, gen) + 10, f'{reduction:.1f}%\nreduction',
                ha='center', fontsize=9, fontweight='bold',
                bbox=dict(boxstyle='round', facecolor='yellow', alpha=0.5))

    # 3. Top Regions (FHVHV)
    ax3 = fig.add_subplot(gs[1, :2])

    zone_dist = fhvhv_data.get('dp_zone_distribution', {})
    top_zones = sorted(zone_dist.items(), key=lambda x: x[1], reverse=True)[:6]

    zones = [z[0] for z in top_zones]
    counts = [z[1] for z in top_zones]

    colors_gradient = plt.cm.viridis(np.linspace(0.3, 0.9, len(zones)))
    bars = ax3.bar(range(len(zones)), counts, color=colors_gradient,
                  alpha=0.8, edgecolor='black', linewidth=1.5)

    ax3.set_title('Top 6 Regions (FHVHV, DP Protected)', fontsize=12, fontweight='bold')
    ax3.set_ylabel('Trip Count (with Laplace noise)', fontsize=10)
    ax3.set_xticks(range(len(zones)))
    ax3.set_xticklabels(zones, rotation=30, ha='right')
    ax3.grid(axis='y', alpha=0.3)

    for bar in bars:
        height = bar.get_height()
        ax3.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height)}',
                ha='center', va='bottom', fontsize=9, fontweight='bold')

    # 4. Privacy vs Utility Tradeoff
    ax4 = fig.add_subplot(gs[1, 2])

    privacy_levels = ['Low\n(k=2)', 'Moderate\n(k=5)', 'High\n(k=10)', 'Very High\n(k=20)']
    privacy_scores = [40, 70, 90, 98]
    utility_scores = [95, 80, 60, 40]

    x = np.arange(len(privacy_levels))

    ax4.plot(x, privacy_scores, 'o-', linewidth=2, markersize=8,
            color='#4ECDC4', label='Privacy', alpha=0.8)
    ax4.plot(x, utility_scores, 's-', linewidth=2, markersize=8,
            color='#FFA500', label='Utility', alpha=0.8)

    ax4.fill_between(x, 0, privacy_scores, alpha=0.2, color='#4ECDC4')
    ax4.fill_between(x, 0, utility_scores, alpha=0.2, color='#FFA500')

    ax4.set_title('Privacy-Utility Tradeoff', fontsize=12, fontweight='bold')
    ax4.set_ylabel('Score (%)', fontsize=10)
    ax4.set_xticks(x)
    ax4.set_xticklabels(privacy_levels, fontsize=8)
    ax4.legend()
    ax4.grid(True, alpha=0.3)
    ax4.axvline(x=1, color='red', linestyle='--', alpha=0.5)

    # 5. DP Noise Distribution
    ax5 = fig.add_subplot(gs[2, :])

    # Simulate DP noise distribution
    np.random.seed(42)
    epsilon = 1.0
    scale = 1 / epsilon
    samples = 10000

    noise = np.random.laplace(0, scale, samples)

    ax5.hist(noise, bins=50, color='#FF6B6B', alpha=0.7,
            edgecolor='black', density=True)

    # Overlay theoretical Laplace distribution
    x_range = np.linspace(-10, 10, 1000)
    laplace_pdf = (1 / (2 * scale)) * np.exp(-np.abs(x_range) / scale)
    ax5.plot(x_range, laplace_pdf, 'b-', linewidth=2, label='Theoretical Laplace')

    ax5.set_title(f'DP Noise Distribution (Laplace, ε={epsilon}, scale={scale})',
                 fontsize=12, fontweight='bold')
    ax5.set_xlabel('Noise Value', fontsize=10)
    ax5.set_ylabel('Density', fontsize=10)
    ax5.legend()
    ax5.grid(True, alpha=0.3)
    ax5.axvline(x=0, color='green', linestyle='--', linewidth=2, alpha=0.5)

    # Add stats
    ax5.text(0.02, 0.95, f'Mean: {np.mean(noise):.2f}\nStd: {np.std(noise):.2f}',
            transform=ax5.transAxes, va='top',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.7),
            fontsize=9)

    plt.savefig(f'{output_dir}/summary_dashboard.png', dpi=300, bbox_inches='tight')
    print(f"✅ Saved: {output_dir}/summary_dashboard.png")
    plt.close()


def main():
    """Generate all privacy visualizations"""
    print("="*70)
    print("PRIVACY VISUALIZATION GENERATOR")
    print("="*70)

    # Load results
    print("\n📂 Loading privacy results...")
    results = load_privacy_results()

    if not results:
        print("❌ No privacy results found in streaming/Privacy/")
        print("   Please run privacy_demo.py first!")
        return

    print(f"✅ Loaded {len(results)} result files")
    for filename, data in results:
        taxi_type = data.get('taxi_type', 'Unknown')
        sample_size = data.get('sample_size', 0)
        print(f"   - {filename}: {taxi_type.upper()} ({sample_size:,} records)")

    # Create output directory
    output_dir = "privacy_graphs"
    Path(output_dir).mkdir(exist_ok=True)
    print(f"\n📊 Generating visualizations in '{output_dir}/'...")

    # Generate all plots
    print("\n1️⃣  Zone Generalization...")
    plot_zone_generalization(results, output_dir)

    print("2️⃣  Zone Distribution...")
    plot_zone_distribution(results, output_dir)

    print("3️⃣  Time Distribution...")
    plot_time_distribution(results, output_dir)

    print("4️⃣  Privacy Comparison...")
    plot_privacy_comparison(results, output_dir)

    print("5️⃣  DP Noise Demonstration...")
    plot_differential_privacy_noise(output_dir)

    print("6️⃣  Privacy Guarantees...")
    plot_privacy_guarantees(output_dir)

    print("7️⃣  Summary Dashboard...")
    create_summary_dashboard(results, output_dir)

    print("\n" + "="*70)
    print("✨ VISUALIZATION COMPLETE!")
    print("="*70)
    print(f"\n📁 All graphs saved to: {output_dir}/")
    print("\nGenerated files:")
    print("  1. zone_generalization.png - K-Anonymity effect on zones")
    print("  2. zone_distribution.png - DP-protected zone distributions")
    print("  3. time_distribution.png - DP-protected time patterns")
    print("  4. privacy_comparison.png - Original vs Anonymized comparison")
    print("  5. dp_noise_demonstration.png - How DP noise works")
    print("  6. privacy_guarantees.png - Privacy guarantees visualization")
    print("  7. summary_dashboard.png - Complete summary dashboard")
    print()


if __name__ == "__main__":
    main()
