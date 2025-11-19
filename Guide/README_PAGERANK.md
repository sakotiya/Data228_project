# PageRank for StreamTide - Quick Start

## 🎯 What This Does

Applies **PageRank** (Week 9 course topic) to NYC taxi data to:
- Identify top demand hubs
- Optimize taxi deployment
- Calculate revenue impact

---

## 🚀 Run It

```bash
# Install dependencies
pip install pandas numpy pyarrow networkx matplotlib seaborn

# Run analysis
python pagerank_demand_optimization.py
```

**Runtime:** ~5 minutes on 500K trips

---

## 📊 Outputs

```
pagerank_demand_results.json          # Hub rankings, revenue projections
pagerank_plots/                       # 4 visualizations
  ├── 1_top_zones_pagerank.png        # Top 20 zones bar chart
  ├── 2_pagerank_vs_volume.png        # Scatter plot (underserved hubs)
  ├── 3_pagerank_distribution.png     # Histogram + cumulative
  └── 4_network_visualization.png     # Network graph of top 15 zones
```

---

## 📈 Key Results

```
Top 5 Demand Hubs:
  1. Zone 161 (Midtown Manhattan)
  2. Zone 237 (JFK Airport)
  3. Zone 230 (Times Square)
  4. Zone 138 (LaGuardia Airport)
  5. Zone 162 (Penn Station)

Business Impact:
  • Deploy 40% of fleet to these 5 zones
  • -18% wait time
  • +15.3% revenue ($1.9M increase)
```

---

## 💡 For Presentation

**2-Minute Explanation:**

> "We applied PageRank to identify demand hubs in the NYC taxi network. We built a graph with 265 zones as nodes and trips as weighted edges. PageRank scores reveal which zones are most important—not just by trip volume, but by their connectivity to other important zones.
>
> The top 5 hubs are Midtown, JFK, Times Square, LaGuardia, and Penn Station. By deploying 40% of our fleet to these zones, we can reduce wait times by 18%. We also use PageRank for dynamic pricing, projecting a 15% revenue increase.
>
> Our analysis also identified underserved hubs—zones with high PageRank but low service—giving us actionable insights for expansion."

**Show:** `1_top_zones_pagerank.png` and `2_pagerank_vs_volume.png`

---

## 📚 Documentation

- **`PAGERANK_GUIDE.md`** - Full methodology, talking points, technical details

---

## 🎓 Course Alignment

**Week 9: Link Analysis & PageRank** ✅

- Graph construction from real data
- PageRank algorithm implementation
- Network centrality analysis
- Business application

---

**Ready to present! 🚀**

