#!/usr/bin/env python3
"""
LSH S-Curve Graph
Shows probability of becoming a candidate pair vs similarity
"""

import matplotlib.pyplot as plt
import numpy as np

plt.style.use('seaborn-v0_8-whitegrid')

def lsh_probability(s, b, r):
    """
    Calculate probability that two items become candidates
    s: similarity (Jaccard)
    b: number of bands
    r: rows per band
    P = 1 - (1 - s^r)^b
    """
    return 1 - (1 - s**r)**b

def create_lsh_scurve():
    """Create LSH S-Curve showing threshold behavior"""

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    # Similarity range
    s = np.linspace(0, 1, 1000)

    # === Plot 1: S-Curve for different configurations ===

    # Different LSH configurations (b bands, r rows per band)
    configs = [
        (20, 5, '#3498db', 'b=20, r=5 (100 hashes)'),
        (25, 4, '#e74c3c', 'b=25, r=4 (100 hashes)'),
        (10, 10, '#2ecc71', 'b=10, r=10 (100 hashes)'),
        (50, 2, '#9b59b6', 'b=50, r=2 (100 hashes)')
    ]

    for b, r, color, label in configs:
        prob = lsh_probability(s, b, r)
        ax1.plot(s, prob, color=color, linewidth=2.5, label=label)

        # Find threshold (where P = 0.5)
        threshold = (1/b)**(1/r)
        ax1.axvline(x=threshold, color=color, linestyle=':', alpha=0.5)

    ax1.set_xlabel('Jaccard Similarity', fontsize=12)
    ax1.set_ylabel('Probability of Being Candidate', fontsize=12)
    ax1.set_title('LSH S-Curve: Different Configurations', fontsize=13, fontweight='bold')
    ax1.legend(loc='lower right', fontsize=9)
    ax1.grid(True, alpha=0.3)

    # Add annotation
    ax1.annotate('Threshold region\n(steep transition)',
                xy=(0.5, 0.5), xytext=(0.7, 0.3),
                fontsize=10, arrowprops=dict(arrowstyle='->', color='black'),
                bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.8))

    # === Plot 2: Our configuration with ground truth comparison ===

    # Our LSH configuration: b=20, r=5
    b, r = 20, 5
    prob = lsh_probability(s, b, r)
    threshold = (1/b)**(1/r)

    ax2.plot(s, prob, color='#3498db', linewidth=3, label='LSH Probability')
    ax2.axhline(y=0.5, color='gray', linestyle='--', alpha=0.7)
    ax2.axvline(x=threshold, color='red', linestyle='--', linewidth=2,
                label=f'Threshold ≈ {threshold:.2f}')

    # Fill regions
    ax2.fill_between(s, prob, where=(s < threshold), alpha=0.3, color='#e74c3c',
                     label='Low similarity (filtered)')
    ax2.fill_between(s, prob, where=(s >= threshold), alpha=0.3, color='#2ecc71',
                     label='High similarity (candidates)')

    ax2.set_xlabel('Jaccard Similarity', fontsize=12)
    ax2.set_ylabel('Probability of Being Candidate', fontsize=12)
    ax2.set_title(f'Our LSH Configuration (b={b}, r={r})\nThreshold ≈ {threshold:.2f}',
                  fontsize=13, fontweight='bold')
    ax2.legend(loc='lower right', fontsize=9)
    ax2.grid(True, alpha=0.3)

    # Add key insight
    ax2.annotate(f'Items with similarity > {threshold:.2f}\nhave >50% chance of detection',
                xy=(threshold, 0.5), xytext=(0.65, 0.25),
                fontsize=10, arrowprops=dict(arrowstyle='->', color='red'),
                bbox=dict(boxstyle='round', facecolor='white', alpha=0.9))

    plt.suptitle('LSH S-Curve: Probability of Candidate Selection\nNYC Taxi Route Similarity Analysis',
                 fontsize=14, fontweight='bold', y=1.02)

    plt.tight_layout()
    plt.savefig('/Users/shreya/SJSU-Github/DA228/lsh_scurve.png', dpi=150, bbox_inches='tight')
    plt.close()

    print("Saved: /Users/shreya/SJSU-Github/DA228/lsh_scurve.png")
    print(f"\nLSH Configuration: {b} bands × {r} rows = {b*r} hash functions")
    print(f"Threshold similarity: {threshold:.3f}")
    print(f"Items with similarity ≥ {threshold:.2f} have >50% probability of being candidates")


if __name__ == "__main__":
    print("Generating LSH S-Curve graph...")
    create_lsh_scurve()
