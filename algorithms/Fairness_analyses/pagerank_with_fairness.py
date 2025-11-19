"""
PageRank for Taxi Demand Modeling & Fare Optimization + Fairness Analysis
==========================================================================

Week 9: PageRank & Link Analysis
Week 13-14: Fairness Analysis

Author: DATA 228 Project - Group 9
Date: November 2025
"""

import pandas as pd
import numpy as np
import pyarrow.parquet as pq
from pathlib import Path
import json
from datetime import datetime
from typing import Dict, List, Tuple, Any
import warnings
warnings.filterwarnings('ignore')

# ML Libraries
import networkx as nx

# Plotting
import matplotlib.pyplot as plt
import seaborn as sns

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)


class TaxiZoneGraph:
    """Build zone-to-zone graph from taxi trips"""
    
    def __init__(self):
        self.graph = nx.DiGraph()
        self.zone_names = {}
        self.trip_counts = {}
        
    def build_from_trips(self, df: pd.DataFrame) -> nx.DiGraph:
        """Build directed graph from trip data"""
        print("\n" + "="*70)
        print("BUILDING TAXI ZONE GRAPH")
        print("="*70)
        
        if 'pulocationid' not in df.columns and 'PULocationID' not in df.columns:
            print("⚠️  No pickup zone column found")
            return self.graph
            
        # Count trips between zones
        if 'pulocationid' in df.columns and 'dolocationid' in df.columns:
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
        
        in_degrees = dict(self.graph.in_degree())
        out_degrees = dict(self.graph.out_degree())
        
        stats['avg_in_degree'] = np.mean(list(in_degrees.values()))
        stats['avg_out_degree'] = np.mean(list(out_degrees.values()))
        stats['max_in_degree'] = max(in_degrees.values())
        stats['max_out_degree'] = max(out_degrees.values())
        
        return stats


class PageRankDemandAnalyzer:
    """Use PageRank to identify important zones for demand modeling"""
    
    def __init__(self, graph: nx.DiGraph):
        self.graph = graph
        self.pagerank_scores = {}
        self.zone_rankings = None
        
    def compute_pagerank(self, damping_factor: float = 0.85, max_iter: int = 100) -> Dict:
        """Compute PageRank scores for all zones"""
        print("\n" + "="*70)
        print("COMPUTING PAGERANK FOR DEMAND MODELING")
        print("="*70)
        
        if self.graph.number_of_nodes() == 0:
            print("⚠️  Empty graph")
            return {}
        
        self.pagerank_scores = nx.pagerank(
            self.graph,
            alpha=damping_factor,
            max_iter=max_iter,
            weight='weight'
        )
        
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
        """Identify top demand hubs based on PageRank"""
        print("\n" + "="*70)
        print(f"TOP {top_n} DEMAND HUBS (by PageRank)")
        print("="*70)
        
        top_hubs = self.zone_rankings.head(top_n).copy()
        
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


class FareOptimizationEngine:
    """Use PageRank for dynamic fare optimization"""
    
    def __init__(self, pagerank_scores: Dict):
        self.pagerank_scores = pagerank_scores
        self.fare_multipliers = {}
        
    def compute_fare_multipliers(self, base_multiplier: float = 1.0, 
                                 max_multiplier: float = 1.5) -> Dict:
        """Compute fare multipliers based on PageRank"""
        print("\n" + "="*70)
        print("DYNAMIC FARE OPTIMIZATION (PageRank-based)")
        print("="*70)
        
        pr_values = np.array(list(self.pagerank_scores.values()))
        pr_min, pr_max = pr_values.min(), pr_values.max()
        
        for zone, pr_score in self.pagerank_scores.items():
            normalized_pr = (pr_score - pr_min) / (pr_max - pr_min)
            multiplier = base_multiplier + (max_multiplier - base_multiplier) * normalized_pr
            self.fare_multipliers[zone] = multiplier
        
        print(f"\n💵 Fare Multiplier Strategy:")
        print(f"   • Base multiplier: {base_multiplier:.2f}x")
        print(f"   • Max multiplier: {max_multiplier:.2f}x")
        print(f"   • Applied to {len(self.fare_multipliers)} zones")
        
        sorted_zones = sorted(self.fare_multipliers.items(), key=lambda x: x[1], reverse=True)
        print(f"\n   Top 5 zones (highest multipliers):")
        for zone, mult in sorted_zones[:5]:
            print(f"     Zone {zone:3d}: {mult:.2f}x fare")
        
        return self.fare_multipliers
    
    def estimate_revenue_impact(self, trip_counts: Dict, avg_fare: float = 25.0) -> Dict:
        """Estimate revenue impact of PageRank-based pricing"""
        print("\n" + "="*70)
        print("REVENUE IMPACT ANALYSIS")
        print("="*70)
        
        total_trips = sum(trip_counts.values())
        baseline_revenue = total_trips * avg_fare
        
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


class FairnessAnalyzer:
    """Analyze fairness of PageRank-based pricing (Week 13-14)"""
    
    def __init__(self, fare_multipliers: Dict, pagerank_scores: Dict):
        self.fare_multipliers = fare_multipliers
        self.pagerank_scores = pagerank_scores
        
    def check_pricing_fairness(self) -> Dict:
        """
        Analyze fairness of pricing strategy
        
        Metrics:
        - Disparity Ratio: max/min multiplier
        - Threshold: < 2.0 is considered fair
        """
        print("\n" + "="*70)
        print("FAIRNESS ANALYSIS: Pricing Equity (Week 13-14)")
        print("="*70)
        
        multipliers = list(self.fare_multipliers.values())
        
        max_mult = max(multipliers)
        min_mult = min(multipliers)
        mean_mult = np.mean(multipliers)
        disparity_ratio = max_mult / min_mult
        
        # Identify high and low zones
        sorted_zones = sorted(self.fare_multipliers.items(), 
                            key=lambda x: x[1], reverse=True)
        
        high_zones = sorted_zones[:5]
        low_zones = sorted_zones[-5:]
        
        results = {
            'max_multiplier': float(max_mult),
            'min_multiplier': float(min_mult),
            'mean_multiplier': float(mean_mult),
            'disparity_ratio': float(disparity_ratio),
            'fairness_threshold': 2.0,
            'is_fair': bool(disparity_ratio < 2.0),
            'highest_priced_zones': {int(z): float(m) for z, m in high_zones},
            'lowest_priced_zones': {int(z): float(m) for z, m in low_zones}
        }
        
        print(f"\n📊 Pricing Fairness Metrics:")
        print(f"   • Max multiplier: {max_mult:.2f}x")
        print(f"   • Min multiplier: {min_mult:.2f}x")
        print(f"   • Mean multiplier: {mean_mult:.2f}x")
        print(f"   • Disparity ratio: {disparity_ratio:.2f}")
        print(f"   • Fairness threshold: 2.0")
        
        if disparity_ratio < 2.0:
            print(f"   ✅ FAIR: Disparity ratio ({disparity_ratio:.2f}) < 2.0")
        else:
            print(f"   ⚠️  UNFAIR: Disparity ratio ({disparity_ratio:.2f}) ≥ 2.0")
        
        print(f"\n   Top 3 Highest Priced Zones:")
        for zone, mult in high_zones[:3]:
            pr = self.pagerank_scores.get(zone, 0)
            print(f"     Zone {zone:3d}: {mult:.2f}x (PageRank: {pr:.4f})")
        
        print(f"\n   Top 3 Lowest Priced Zones:")
        for zone, mult in low_zones[:3]:
            pr = self.pagerank_scores.get(zone, 0)
            print(f"     Zone {zone:3d}: {mult:.2f}x (PageRank: {pr:.4f})")
        
        # Additional fairness insight
        print(f"\n💡 Fairness Insight:")
        if disparity_ratio < 1.5:
            print(f"   Excellent equity: Only {(disparity_ratio-1)*100:.0f}% difference between highest and lowest zones")
        elif disparity_ratio < 2.0:
            print(f"   Good equity: {(disparity_ratio-1)*100:.0f}% difference is within acceptable bounds")
        else:
            print(f"   Consider capping multipliers to reduce {(disparity_ratio-1)*100:.0f}% disparity")
        
        return results


def main():
    """Main execution pipeline"""
    print("="*70)
    print("PAGERANK + FAIRNESS ANALYSIS FOR TAXI DEMAND MODELING")
    print("="*70)
    print("Week 9: PageRank & Link Analysis")
    print("Week 13-14: Fairness Analysis")
    print("="*70)
    
    # Load 2025 data
    data_dir = Path("/Users/gouravdhama/Documents/bubu/big_data/old/2025_files")
    print(f"\n📂 Loading data from: {data_dir}")
    
    dfs = []
    for file in sorted(data_dir.glob("*.parquet")):
        print(f"   Reading {file.name}...")
        try:
            df = pd.read_parquet(file)
            df.columns = df.columns.str.lower()
            df['file_source'] = file.stem
            dfs.append(df)
        except Exception as e:
            print(f"   Error: {e}")
    
    data = pd.concat(dfs, ignore_index=True, sort=False)
    print(f"\n✅ Loaded {len(data):,} total trips")
    
    # Sample for faster execution
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
    
    # 4. Fare Optimization
    fare_engine = FareOptimizationEngine(pagerank_scores)
    fare_multipliers = fare_engine.compute_fare_multipliers(
        base_multiplier=1.0,
        max_multiplier=1.5
    )
    
    revenue_impact = fare_engine.estimate_revenue_impact(
        graph_builder.trip_counts,
        avg_fare=25.0
    )
    
    # 5. Fairness Analysis (NEW!)
    fairness_analyzer = FairnessAnalyzer(fare_multipliers, pagerank_scores)
    fairness_results = fairness_analyzer.check_pricing_fairness()
    
    # 6. Save Results
    results = {
        'timestamp': datetime.now().isoformat(),
        'data_summary': {
            'total_trips_analyzed': len(data),
            'unique_zones': graph.number_of_nodes(),
            'unique_routes': graph.number_of_edges()
        },
        'graph_statistics': stats,
        'top_20_hubs': top_hubs.to_dict('records'),
        'fare_optimization': {
            'multipliers': {int(k): float(v) for k, v in list(fare_multipliers.items())[:20]},
            'revenue_impact': revenue_impact
        },
        'fairness_analysis': fairness_results
    }
    
    with open('pagerank_fairness_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print("\n" + "="*70)
    print("✅ ANALYSIS COMPLETE!")
    print("="*70)
    print("📁 Output: pagerank_fairness_results.json")
    print("="*70)
    
    # Key Insights Summary
    print("\n🎯 KEY INSIGHTS:")
    print("-" * 70)
    print(f"1. Top Hub: Zone {top_hubs.iloc[0]['zone_id']} (PageRank: {top_hubs.iloc[0]['pagerank']:.4f})")
    print(f"2. Revenue Impact: +{revenue_impact['revenue_increase_pct']:.2f}% (${revenue_impact['revenue_increase']:,.0f})")
    print(f"3. Fairness: Disparity ratio {fairness_results['disparity_ratio']:.2f} - {'✅ FAIR' if fairness_results['is_fair'] else '⚠️ NEEDS REVIEW'}")
    print("="*70)


if __name__ == "__main__":
    main()

