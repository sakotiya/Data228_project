# Presentation Guide: PageRank for Taxi Demand Modeling

## 📊 Your 6 Plots Explained

All plots are in `presentation_plots/` folder - ready to use!

---

## Slide 1: Top 10 Demand Hubs
**File:** `1_top_10_hubs.png`

### What to Say:
> "We applied PageRank to identify the most important zones in the NYC taxi network. Zone 265 has the highest PageRank score of 0.034, making it twice as important as the second-ranked zone. This zone receives trips from 250 other zones, indicating it's a major destination hub."

### Key Points:
- Zone 265 is #1 (RED bar - highlighted)
- PageRank scores range from 0.034 to 0.008
- Top 5 zones are critical for taxi deployment

---

## Slide 2: PageRank Distribution
**File:** `2_pagerank_distribution.png`

### What to Say:
> "The PageRank distribution shows that importance is concentrated in a few zones. The mean is 0.010 and median is 0.009, with Zone 265 being a clear outlier at 0.034. This tells us that a small number of zones dominate the network."

### Key Points:
- Most zones cluster around 0.008-0.010
- Zone 265 is an outlier (far right)
- Stats box shows: Max 0.034, Min 0.008, Std 0.006

---

## Slide 3: Hub Type Analysis
**File:** `3_hub_type_analysis.png`

### What to Say:
> "This scatter plot reveals two types of hubs. Zone 265 is a **destination hub** - it receives 250 trips but only sends out 44. In contrast, Zone 132 is a **balanced transit hub** with equal in and out degrees. The diagonal line represents perfect balance."

### Key Points:
- **Above diagonal** = Destination hubs (people arrive, don't leave)
- **On diagonal** = Transit hubs (balanced flow)
- **Bubble size** = PageRank importance
- Zone 265 (RED) is far above the line = destination
- Zone 132 (BLUE) is on the line = transit

### Business Insight:
> "We should deploy taxis to balanced hubs like Zone 132, not destination hubs like Zone 265."

---

## Slide 4: Revenue Impact
**File:** `4_revenue_impact.png`

### What to Say:
> "By implementing PageRank-based fare multipliers, we can increase revenue by 7.6%, or $874,000. The baseline revenue is $11.5 million, and with optimized pricing based on zone importance, we project $12.4 million in revenue."

### Key Points:
- **Baseline:** $11.49M (current flat pricing)
- **Optimized:** $12.37M (PageRank-based pricing)
- **Increase:** +$0.87M (+7.6%)
- High PageRank zones get 1.5x multiplier
- Low PageRank zones stay at 1.0x

### Business Justification:
> "This is fair pricing - customers pay more for trips to/from important, well-connected hubs."

---

## Slide 5: Underserved Hub Analysis
**File:** `5_underserved_analysis.png`

### What to Say:
> "Here's our key finding: Zone 265 has the highest PageRank but the LOWEST trip volume - only 65 trips in our 500,000 trip sample. This is a massive service gap. While it's the most important zone by network connectivity, we're barely serving it."

### Key Points:
- **Left chart:** Zone 265 has highest PageRank (0.034)
- **Right chart:** Zone 265 has lowest volume (65 trips)
- This is a **10x discrepancy**
- Other top zones have 4,000-5,000 trips

### Business Opportunity:
> "This represents untapped demand. We should investigate why service is low and increase taxi deployment to Zone 265."

---

## Slide 6: Surge Pricing Strategy
**File:** `6_surge_pricing_strategy.png`

### What to Say:
> "We recommend surge pricing for 27 zones - the top 10% by PageRank. This represents 10.3% of all zones but covers the most important hubs. The pie chart shows the split, and the bar chart shows which specific zones should have surge pricing."

### Key Points:
- **27 zones** (10.3%) get surge pricing
- **234 zones** (89.7%) keep regular pricing
- Threshold: 90th percentile (PageRank ≥ 0.0068)
- All top 10 zones are included

### Business Balance:
> "This balances revenue optimization with fairness - 90% of zones maintain flat pricing."

---

## 🎤 Complete Presentation Flow (5 minutes)

### Opening (30 sec)
> "For our StreamTide project, we needed to answer: **Where should we deploy taxis?** We analyzed 500,000 trips across 261 NYC zones using PageRank, a link analysis algorithm from Week 9 of our course."

### Slide 1: Top Hubs (45 sec)
> "PageRank identified Zone 265 as the #1 hub with a score of 0.034 - twice as important as the second zone. The top 10 zones shown here are where we should focus our deployment strategy."

### Slide 2: Distribution (30 sec)
> "The distribution shows that importance is concentrated. Most zones cluster around 0.008-0.010, but Zone 265 is a clear outlier, indicating exceptional importance in the network."

### Slide 3: Hub Types (60 sec)
> "This analysis reveals two hub types. Zone 265 is a destination hub - people arrive but don't leave by taxi. Zone 132 is a balanced transit hub with equal flow. For taxi deployment, we want balanced hubs, not destination hubs."

### Slide 4: Revenue (45 sec)
> "Our pricing optimization projects a 7.6% revenue increase - that's $874,000 on an $11.5 million baseline. We achieve this by applying fare multipliers based on PageRank: 1.5x for top zones, 1.0x for peripheral zones."

### Slide 5: Key Finding (60 sec)
> "Here's our most important discovery: Zone 265 has the highest PageRank but the lowest service - only 65 trips. This is a massive service gap representing untapped demand. We recommend immediate investigation and increased deployment."

### Slide 6: Strategy (30 sec)
> "Our surge pricing strategy targets 27 zones - the top 10% by importance. This balances revenue optimization with fairness, as 90% of zones maintain flat pricing."

### Closing (30 sec)
> "In summary, PageRank gave us three actionable insights: deploy to balanced hubs like Zone 132, investigate the service gap at Zone 265, and implement surge pricing in 27 key zones for a 7.6% revenue increase."

---

## 📋 Quick Reference Card

| Plot | Key Message | Number to Remember |
|------|-------------|-------------------|
| **1. Top Hubs** | Zone 265 is #1 | PageRank: 0.034 |
| **2. Distribution** | Importance is concentrated | Mean: 0.010 |
| **3. Hub Types** | Two types: destination vs transit | Zone 265: 250 in, 44 out |
| **4. Revenue** | PageRank pricing increases revenue | +7.6% ($874K) |
| **5. Underserved** | Biggest opportunity | Zone 265: 65 trips only |
| **6. Surge Pricing** | Target top 10% | 27 zones |

---

## 💡 Anticipating Questions

**Q: Why is Zone 265 so important if it has low volume?**
> "PageRank measures network importance, not just volume. Zone 265 receives trips from 250 other zones, making it a critical endpoint. The low volume suggests underservice, not low demand."

**Q: How do you justify higher fares for important zones?**
> "Customers benefit from better connectivity and service at important hubs. The 1.5x multiplier is modest and only applies to the top zones. 90% of zones maintain flat pricing."

**Q: What's the business case for increasing service to Zone 265?**
> "It's the #1 hub by importance but has only 65 trips. This suggests latent demand. Even a 10x increase to 650 trips would still be below other top zones, indicating significant growth potential."

**Q: How does this compare to simple trip volume ranking?**
> "PageRank considers network structure, not just volume. Zone 265 might not be #1 by volume, but it's #1 by connectivity. This reveals hidden opportunities that volume-based ranking would miss."

---

## 🎯 Key Takeaways (Memorize These)

1. **Zone 265 is the #1 hub** (PageRank: 0.034)
2. **Service gap identified** (65 trips vs 0.034 PageRank)
3. **Revenue opportunity** (+7.6% = $874K)
4. **Deployment strategy** (40% of fleet to top 5 balanced hubs)
5. **Surge pricing** (27 zones = top 10%)

---

## 📸 Plot Order for Slides

**Recommended order:**
1. **Slide 1:** Problem statement (text only)
2. **Slide 2:** Method - PageRank explanation (text + small diagram)
3. **Slide 3:** Plot 1 - Top 10 Hubs
4. **Slide 4:** Plot 3 - Hub Type Analysis
5. **Slide 5:** Plot 5 - Underserved Analysis (KEY FINDING)
6. **Slide 6:** Plot 4 - Revenue Impact
7. **Slide 7:** Plot 6 - Surge Pricing Strategy
8. **Slide 8:** Conclusions (text only)

*Skip Plot 2 (distribution) if time is limited - it's more technical.*

---

## ✅ Pre-Presentation Checklist

- [ ] All 6 plots are in `presentation_plots/` folder
- [ ] Plots are high-resolution (300 DPI) ✅
- [ ] Memorized Zone 265 stats (PageRank 0.034, 65 trips)
- [ ] Memorized revenue impact (+7.6%, $874K)
- [ ] Can explain PageRank in 30 seconds
- [ ] Can explain destination vs transit hubs
- [ ] Have backup answers for 4 common questions
- [ ] Practiced 5-minute presentation

---

**You're ready! Good luck! 🚀**

