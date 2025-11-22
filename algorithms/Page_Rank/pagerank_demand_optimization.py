"""
PageRank for Taxi Demand Modeling & Fare Optimization
======================================================

Aligns with StreamTide project goals:
1. Demand Modeling: Identify high-demand zones using PageRank
2. Fare Optimization: Price based on zone importance
3. Surge Detection: Monitor PageRank changes in real-time

"""

import pandas as pd
import numpy as np
import pyarrow.parquet as pq
from pathlib import Path
import json
from datetime import datetime
from typing import Dict, List, Tuple
import warnings
warnings.filterwarnings('ignore')

import os

# Visualization
import matplotlib.pyplot as plt
import seaborn as sns
import networkx as nx

sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)


def _load_env_file(path: str = ".env") -> None:
    """
    Lightweight .env loader so local data directories can be configured
    without hard‑coding user‑specific absolute paths.
    """
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


class TaxiZoneGraph:
    """Build zone-to-zone graph from taxi trips"""
    
    def __init__(self):
        self.graph = nx.DiGraph()
        self.zone_names = {}
        self.trip_counts = {}
        
    def build_from_trips(self, df: pd.DataFrame) -> nx.DiGraph:
        """
        Build directed graph from trip data
        
        Nodes: Taxi zones
        Edges: Trips from zone A → zone B
        Edge weights: Number of trips
        """
        print("\n" + "="*70)
        print("BUILDING TAXI ZONE GRAPH")
        print("="*70)
        
        # Ensure we have zone columns
        if 'pulocationid' not in df.columns and 'PULocationID' not in df.columns:
            print("⚠️  No pickup zone column found")
            return self.graph
            
        # Count trips between zones
        if 'pulocationid' in df.columns and 'dolocationid' in df.columns:
            # Drop any duplicates and ensure proper dtypes
            zone_df = df[['pulocationid', 'dolocationid']].copy()
            zone_df = zone_df.dropna()
            zone_pairs = zone_df.groupby(['pulocationid', 'dolocationid'], as_index=False).size()
            zone_pairs.columns = ['pulocationid', 'dolocationid', 'trip_count']
            
            print(f"  • Total unique zone pairs: {len(zone_pairs):,}")
            print(f"  • Total trips: {zone_pairs['trip_count'].sum():,}")
            
            # Add edges
            for _, row in zone_pairs.iterrows():
                pickup = int(row['pulocationid'])
                dropoff = int(row['dolocationid'])
                count = int(row['trip_count'])
                
                # Skip self-loops (same pickup/dropoff)
                if pickup == dropoff:
                    continue
                    
                self.graph.add_edge(pickup, dropoff, weight=count)
                self.trip_counts[(pickup, dropoff)] = count
            
            print(f"  • Graph nodes (zones): {self.graph.number_of_nodes()}")
            print(f"  • Graph edges (routes): {self.graph.number_of_edges()}")
            
        return self.graph
    
    def get_statistics(self) -> Dict:
        """Compute graph statistics"""
        if self.graph.number_of_nodes() == 0:
            return {}
            
        stats = {
            'num_nodes': self.graph.number_of_nodes(),
            'num_edges': self.graph.number_of_edges(),
            'density': nx.density(self.graph),
            'is_strongly_connected': nx.is_strongly_connected(self.graph),
            'num_weakly_connected_components': nx.number_weakly_connected_components(self.graph)
        }
        
        # Degree statistics
        in_degrees = dict(self.graph.in_degree())
        out_degrees = dict(self.graph.out_degree())
        
        stats['avg_in_degree'] = np.mean(list(in_degrees.values()))
        stats['avg_out_degree'] = np.mean(list(out_degrees.values()))
        stats['max_in_degree'] = max(in_degrees.values())
        stats['max_out_degree'] = max(out_degrees.values())
        
        return stats


class PageRankDemandAnalyzer:
    """
    Use PageRank to identify important zones for demand modeling
    
    Key Insight: High PageRank = High demand hub
    - Zones that receive trips from many other important zones
    - Central locations in the taxi network
    - Prime candidates for surge pricing
    """
    
    def __init__(self, graph: nx.DiGraph):
        self.graph = graph
        self.pagerank_scores = {}
        self.zone_rankings = None
        
    def compute_pagerank(self, damping_factor: float = 0.85, max_iter: int = 100) -> Dict:
        """
        Compute PageRank scores for all zones
        
        Args:
            damping_factor: Probability of following a link (default 0.85)
            max_iter: Maximum iterations
            
        Returns:
            Dictionary of {zone_id: pagerank_score}
        """
        print("\n" + "="*70)
        print("COMPUTING PAGERANK FOR DEMAND MODELING")
        print("="*70)
        
        if self.graph.number_of_nodes() == 0:
            print("⚠️  Empty graph")
            return {}
        
        # Compute PageRank using edge weights (trip counts)
        self.pagerank_scores = nx.pagerank(
            self.graph,
            alpha=damping_factor,
            max_iter=max_iter,
            weight='weight'
        )
        
        # Create ranked list
        self.zone_rankings = pd.DataFrame([
            {'zone_id': zone, 'pagerank': score}
            for zone, score in self.pagerank_scores.items()
        ]).sort_values('pagerank', ascending=False).reset_index(drop=True)
        
        self.zone_rankings['rank'] = range(1, len(self.zone_rankings) + 1)
        
        print(f"\n✅ PageRank computed for {len(self.pagerank_scores)} zones")
        print(f"   Damping factor: {damping_factor}")
        print(f"   Iterations: {max_iter}")
        
        return self.pagerank_scores
    
    def identify_demand_hubs(self, top_n: int = 20) -> pd.DataFrame:
        """
        Identify top demand hubs based on PageRank
        
        These are zones where:
        - Taxis should be pre-positioned
        - Surge pricing is most effective
        - Demand is consistently high
        """
        print("\n" + "="*70)
        print(f"TOP {top_n} DEMAND HUBS (by PageRank)")
        print("="*70)
        
        top_hubs = self.zone_rankings.head(top_n).copy()
        
        # Add degree information
        in_degrees = dict(self.graph.in_degree())
        out_degrees = dict(self.graph.out_degree())
        
        top_hubs['in_degree'] = top_hubs['zone_id'].map(in_degrees)
        top_hubs['out_degree'] = top_hubs['zone_id'].map(out_degrees)
        top_hubs['total_degree'] = top_hubs['in_degree'] + top_hubs['out_degree']
        
        print("\n📊 Top 10 Demand Hubs:")
        print("-" * 70)
        for idx, row in top_hubs.head(10).iterrows():
            print(f"  {int(row['rank']):2d}. Zone {int(row['zone_id']):3d} | "
                  f"PageRank: {row['pagerank']:.6f} | "
                  f"In: {int(row['in_degree']):3d} | Out: {int(row['out_degree']):3d}")
        
        return top_hubs
    
    def compute_surge_pricing_zones(self, percentile: float = 90) -> List[int]:
        """
        Identify zones for surge pricing based on PageRank
        
        Args:
            percentile: Top percentile for surge pricing (default 90th)
            
        Returns:
            List of zone IDs for surge pricing
        """
        threshold = np.percentile(list(self.pagerank_scores.values()), percentile)
        surge_zones = [zone for zone, score in self.pagerank_scores.items() if score >= threshold]
        
        print(f"\n💰 Surge Pricing Recommendation:")
        print(f"   • Threshold: {percentile}th percentile (PageRank ≥ {threshold:.6f})")
        print(f"   • Zones for surge pricing: {len(surge_zones)}")
        print(f"   • Coverage: Top {100-percentile:.0f}% of network importance")
        
        return surge_zones
    
    def compare_pagerank_vs_volume(self, trip_counts: Dict) -> pd.DataFrame:
        """
        Compare PageRank (importance) vs Trip Volume (popularity)
        
        Key Insight: High PageRank but low volume = underserved hub
        """
        print("\n" + "="*70)
        print("PAGERANK vs TRIP VOLUME ANALYSIS")
        print("="*70)
        
        # Count trips per zone (pickup)
        zone_volumes = {}
        for (pickup, dropoff), count in trip_counts.items():
            zone_volumes[pickup] = zone_volumes.get(pickup, 0) + count
        
        # Combine with PageRank
        comparison = self.zone_rankings.copy()
        comparison['trip_volume'] = comparison['zone_id'].map(zone_volumes).fillna(0)
        
        # Normalize scores for comparison
        comparison['pagerank_normalized'] = (
            (comparison['pagerank'] - comparison['pagerank'].min()) /
            (comparison['pagerank'].max() - comparison['pagerank'].min())
        )
        comparison['volume_normalized'] = (
            (comparison['trip_volume'] - comparison['trip_volume'].min()) /
            (comparison['trip_volume'].max() - comparison['trip_volume'].min())
        )
        
        # Calculate discrepancy
        comparison['discrepancy'] = comparison['pagerank_normalized'] - comparison['volume_normalized']
        
        # Identify underserved hubs (high PageRank, low volume)
        underserved = comparison[
            (comparison['pagerank_normalized'] > 0.7) &
            (comparison['volume_normalized'] < 0.3)
        ].sort_values('discrepancy', ascending=False)
        
        if len(underserved) > 0:
            print(f"\n⚠️  Underserved Hubs (High Importance, Low Service):")
            for idx, row in underserved.head(5).iterrows():
                print(f"   Zone {int(row['zone_id']):3d}: PageRank={row['pagerank']:.6f}, "
                      f"Volume={int(row['trip_volume'])}")
        
        return comparison


class FareOptimizationEngine:
    """
    Use PageRank for dynamic fare optimization
    
    Strategy: Higher PageRank → Higher base fare multiplier
    """
    
    def __init__(self, pagerank_scores: Dict):
        self.pagerank_scores = pagerank_scores
        self.fare_multipliers = {}
        
    def compute_fare_multipliers(self, base_multiplier: float = 1.0, 
                                 max_multiplier: float = 2.0) -> Dict:
        """
        Compute fare multipliers based on PageRank
        
        Args:
            base_multiplier: Minimum multiplier (default 1.0 = no change)
            max_multiplier: Maximum multiplier for highest PageRank zone
            
        Returns:
            Dictionary of {zone_id: fare_multiplier}
        """
        print("\n" + "="*70)
        print("DYNAMIC FARE OPTIMIZATION (PageRank-based)")
        print("="*70)
        
        # Normalize PageRank scores
        pr_values = np.array(list(self.pagerank_scores.values()))
        pr_min, pr_max = pr_values.min(), pr_values.max()
        
        for zone, pr_score in self.pagerank_scores.items():
            # Linear scaling from base to max multiplier
            normalized_pr = (pr_score - pr_min) / (pr_max - pr_min)
            multiplier = base_multiplier + (max_multiplier - base_multiplier) * normalized_pr
            self.fare_multipliers[zone] = multiplier
        
        print(f"\n💵 Fare Multiplier Strategy:")
        print(f"   • Base multiplier: {base_multiplier:.2f}x")
        print(f"   • Max multiplier: {max_multiplier:.2f}x")
        print(f"   • Applied to {len(self.fare_multipliers)} zones")
        
        # Show examples
        sorted_zones = sorted(self.fare_multipliers.items(), key=lambda x: x[1], reverse=True)
        print(f"\n   Top 5 zones (highest multipliers):")
        for zone, mult in sorted_zones[:5]:
            print(f"     Zone {zone:3d}: {mult:.2f}x fare")
        
        return self.fare_multipliers
    
    def estimate_revenue_impact(self, trip_counts: Dict, avg_fare: float = 25.0) -> Dict:
        """
        Estimate revenue impact of PageRank-based pricing
        
        Args:
            trip_counts: Dictionary of (pickup, dropoff) → trip_count
            avg_fare: Average base fare
            
        Returns:
            Revenue analysis
        """
        print("\n" + "="*70)
        print("REVENUE IMPACT ANALYSIS")
        print("="*70)
        
        # Calculate baseline revenue
        total_trips = sum(trip_counts.values())
        baseline_revenue = total_trips * avg_fare
        
        # Calculate optimized revenue
        optimized_revenue = 0
        for (pickup, dropoff), count in trip_counts.items():
            multiplier = self.fare_multipliers.get(pickup, 1.0)
            optimized_revenue += count * avg_fare * multiplier
        
        revenue_increase = optimized_revenue - baseline_revenue
        revenue_increase_pct = (revenue_increase / baseline_revenue) * 100
        
        results = {
            'baseline_revenue': baseline_revenue,
            'optimized_revenue': optimized_revenue,
            'revenue_increase': revenue_increase,
            'revenue_increase_pct': revenue_increase_pct,
            'total_trips': total_trips,
            'avg_fare': avg_fare
        }
        
        print(f"\n💰 Revenue Projections:")
        print(f"   • Baseline revenue: ${baseline_revenue:,.2f}")
        print(f"   • Optimized revenue: ${optimized_revenue:,.2f}")
        print(f"   • Increase: ${revenue_increase:,.2f} (+{revenue_increase_pct:.2f}%)")
        
        return results


def visualize_pagerank_analysis(analyzer: PageRankDemandAnalyzer, 
                                comparison: pd.DataFrame,
                                output_dir: str = "pagerank_plots"):
    """Generate comprehensive visualizations"""
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    print("\n" + "="*70)
    print("GENERATING VISUALIZATIONS")
    print("="*70)
    
    # 1. Top 20 Zones by PageRank
    top_20 = analyzer.zone_rankings.head(20)
    
    fig, ax = plt.subplots(figsize=(14, 8))
    bars = ax.barh(range(len(top_20)), top_20['pagerank'], color='steelblue')
    ax.set_yticks(range(len(top_20)))
    ax.set_yticklabels([f"Zone {z}" for z in top_20['zone_id']])
    ax.invert_yaxis()
    ax.set_xlabel('PageRank Score', fontsize=12)
    ax.set_title('Top 20 Taxi Zones by PageRank - Demand Hubs', fontsize=14, fontweight='bold')
    ax.grid(axis='x', alpha=0.3)
    
    # Highlight top 5
    for i in range(min(5, len(bars))):
        bars[i].set_color('coral')
    
    plt.tight_layout()
    plt.savefig(output_path / '1_top_zones_pagerank.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # 2. PageRank vs Trip Volume
    fig, ax = plt.subplots(figsize=(12, 8))
    
    scatter = ax.scatter(
        comparison['trip_volume'],
        comparison['pagerank'],
        s=50,
        alpha=0.6,
        c=comparison['discrepancy'],
        cmap='RdYlGn_r'
    )
    
    ax.set_xlabel('Trip Volume (Total Pickups)', fontsize=12)
    ax.set_ylabel('PageRank Score', fontsize=12)
    ax.set_title('PageRank vs Trip Volume - Identifying Underserved Hubs', 
                 fontsize=14, fontweight='bold')
    ax.grid(True, alpha=0.3)
    
    # Add colorbar
    cbar = plt.colorbar(scatter, ax=ax)
    cbar.set_label('Discrepancy (High = Underserved)', fontsize=10)
    
    # Annotate top PageRank zones
    for idx, row in comparison.head(5).iterrows():
        ax.annotate(f"Zone {row['zone_id']}", 
                   (row['trip_volume'], row['pagerank']),
                   xytext=(10, 5), textcoords='offset points',
                   fontsize=8, alpha=0.7)
    
    plt.tight_layout()
    plt.savefig(output_path / '2_pagerank_vs_volume.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # 3. PageRank Distribution
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    
    # Histogram
    ax1.hist(comparison['pagerank'], bins=50, color='teal', alpha=0.7, edgecolor='black')
    ax1.axvline(comparison['pagerank'].median(), color='red', linestyle='--', 
                label=f"Median: {comparison['pagerank'].median():.6f}")
    ax1.set_xlabel('PageRank Score', fontsize=12)
    ax1.set_ylabel('Number of Zones', fontsize=12)
    ax1.set_title('PageRank Distribution Across All Zones', fontsize=12, fontweight='bold')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Cumulative distribution
    sorted_pr = np.sort(comparison['pagerank'])
    cumulative = np.arange(1, len(sorted_pr) + 1) / len(sorted_pr) * 100
    
    ax2.plot(sorted_pr, cumulative, color='darkblue', linewidth=2)
    ax2.axhline(80, color='red', linestyle='--', alpha=0.5, label='80th percentile')
    ax2.axhline(90, color='orange', linestyle='--', alpha=0.5, label='90th percentile')
    ax2.set_xlabel('PageRank Score', fontsize=12)
    ax2.set_ylabel('Cumulative Percentage (%)', fontsize=12)
    ax2.set_title('Cumulative PageRank Distribution', fontsize=12, fontweight='bold')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_path / '3_pagerank_distribution.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # 4. Network visualization (sample of top zones)
    top_zones = set(comparison.head(15)['zone_id'])
    subgraph = analyzer.graph.subgraph(top_zones)
    
    fig, ax = plt.subplots(figsize=(14, 10))
    
    # Node sizes based on PageRank
    node_sizes = [analyzer.pagerank_scores[node] * 50000 for node in subgraph.nodes()]
    
    # Layout
    pos = nx.spring_layout(subgraph, k=2, iterations=50, seed=42)
    
    # Draw
    nx.draw_networkx_nodes(subgraph, pos, node_size=node_sizes, 
                          node_color='lightblue', alpha=0.7, ax=ax)
    nx.draw_networkx_edges(subgraph, pos, alpha=0.2, arrows=True, 
                          arrowsize=10, ax=ax)
    nx.draw_networkx_labels(subgraph, pos, font_size=10, ax=ax)
    
    ax.set_title('Top 15 Zones - Network Connectivity (Node size = PageRank)', 
                fontsize=14, fontweight='bold')
    ax.axis('off')
    
    plt.tight_layout()
    plt.savefig(output_path / '4_network_visualization.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"✅ Saved 4 visualizations to {output_path}/")


def main():
    """Main execution pipeline"""
    print("="*70)
    print("PAGERANK FOR TAXI DEMAND MODELING & FARE OPTIMIZATION")
    print("="*70)
    print("StreamTide Project - Week 9: Link Analysis & PageRank")
    print("="*70)
    
    # Load 2025 data
    default_2025_dir = "/Users/vidushi/Documents/bubu/big_data/old/2025_files"
    data_dir = Path(os.environ.get("DATA228_2025_FILES_DIR", default_2025_dir))
    print(f"\n📂 Loading data from: {data_dir}")
    
    dfs = []
    for file in sorted(data_dir.glob("*.parquet")):
        print(f"   Reading {file.name}...")
        try:
            df = pd.read_parquet(file)
            # Standardize column names immediately
            df.columns = df.columns.str.lower()
            df['file_source'] = file.stem
            dfs.append(df)
        except Exception as e:
            print(f"   Error: {e}")
    
    data = pd.concat(dfs, ignore_index=True, sort=False)
    print(f"\n✅ Loaded {len(data):,} total trips")
    
    # Sample for faster execution (remove for full analysis)
    if len(data) > 500000:
        data = data.sample(500000, random_state=42)
        print(f"   Sampled to {len(data):,} trips for analysis")
    
    # 1. Build Zone Graph
    graph_builder = TaxiZoneGraph()
    graph = graph_builder.build_from_trips(data)
    
    stats = graph_builder.get_statistics()
    print(f"\n📊 Graph Statistics:")
    for key, value in stats.items():
        print(f"   • {key}: {value}")
    
    # 2. Compute PageRank
    pr_analyzer = PageRankDemandAnalyzer(graph)
    pagerank_scores = pr_analyzer.compute_pagerank(damping_factor=0.85)
    
    # 3. Identify Demand Hubs
    top_hubs = pr_analyzer.identify_demand_hubs(top_n=20)
    
    # 4. Surge Pricing Zones
    surge_zones = pr_analyzer.compute_surge_pricing_zones(percentile=90)
    
    # 5. Compare PageRank vs Volume
    comparison = pr_analyzer.compare_pagerank_vs_volume(graph_builder.trip_counts)
    
    # 6. Fare Optimization
    fare_engine = FareOptimizationEngine(pagerank_scores)
    fare_multipliers = fare_engine.compute_fare_multipliers(
        base_multiplier=1.0,
        max_multiplier=1.5
    )
    
    revenue_impact = fare_engine.estimate_revenue_impact(
        graph_builder.trip_counts,
        avg_fare=25.0
    )
    
    # 7. Visualizations
    visualize_pagerank_analysis(pr_analyzer, comparison)
    
    # 8. Save Results
    results = {
        'timestamp': datetime.now().isoformat(),
        'data_summary': {
            'total_trips_analyzed': len(data),
            'unique_zones': graph.number_of_nodes(),
            'unique_routes': graph.number_of_edges()
        },
        'graph_statistics': stats,
        'top_20_hubs': top_hubs.to_dict('records'),
        'surge_pricing_zones': surge_zones,
        'fare_optimization': {
            'multipliers': {int(k): float(v) for k, v in list(fare_multipliers.items())[:20]},
            'revenue_impact': revenue_impact
        },
        'underserved_analysis': comparison[
            (comparison['pagerank_normalized'] > 0.7) &
            (comparison['volume_normalized'] < 0.3)
        ][['zone_id', 'pagerank', 'trip_volume', 'discrepancy']].to_dict('records')
    }
    
    with open('pagerank_demand_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print("\n" + "="*70)
    print("✅ ANALYSIS COMPLETE!")
    print("="*70)
    print("📁 Outputs:")
    print("  • pagerank_demand_results.json")
    print("  • pagerank_plots/")
    print("    ├── 1_top_zones_pagerank.png")
    print("    ├── 2_pagerank_vs_volume.png")
    print("    ├── 3_pagerank_distribution.png")
    print("    └── 4_network_visualization.png")
    print("="*70)
    
    # Key Insights Summary
    print("\n🎯 KEY INSIGHTS FOR STREAMTIDE:")
    print("-" * 70)
    print(f"1. Demand Hubs: {len(top_hubs)} zones identified for taxi pre-positioning")
    print(f"2. Surge Pricing: {len(surge_zones)} zones recommended for dynamic pricing")
    print(f"3. Revenue Impact: +{revenue_impact['revenue_increase_pct']:.2f}% potential increase")
    print(f"4. Underserved Zones: {len(results['underserved_analysis'])} high-importance zones need more service")
    print("="*70)


if __name__ == "__main__":
    main()

