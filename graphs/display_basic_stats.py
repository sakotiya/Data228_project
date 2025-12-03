#!/usr/bin/env python3
"""
Display Basic Statistics in Table Format
"""

import json
import pandas as pd

# Input data
basic_stats = {
    "total_trips": 330540611,
    "avg_fare": 23.76442729426178,
    "std_fare": 22.15215358213365,
    "median_fare": 17.22,
    "total_revenue": 7063178645.390309,
    "avg_distance": 4.760311502630188,
    "std_distance": 5.656696393216093,
    "total_distance": 1414842871.4249756
}

def display_as_table():
    """Display stats as formatted table"""
    print("\n" + "="*80)
    print("BASIC STATISTICS SUMMARY")
    print("="*80)

    # Overall Statistics
    print("\n📊 OVERALL TRIP STATISTICS")
    print("-"*80)

    overall_data = {
        'Metric': ['Total Trips', 'Total Revenue', 'Total Distance'],
        'Value': [
            f"{basic_stats['total_trips']:,} trips",
            f"${basic_stats['total_revenue']:,.2f}",
            f"{basic_stats['total_distance']:,.2f} miles"
        ]
    }
    df_overall = pd.DataFrame(overall_data)
    print(df_overall.to_string(index=False))

    # Fare Statistics
    print("\n\n💰 FARE STATISTICS")
    print("-"*80)

    fare_data = {
        'Metric': ['Average Fare', 'Standard Deviation', 'Median Fare'],
        'Value': [
            f"${basic_stats['avg_fare']:.2f}",
            f"${basic_stats['std_fare']:.2f}",
            f"${basic_stats['median_fare']:.2f}"
        ]
    }
    df_fare = pd.DataFrame(fare_data)
    print(df_fare.to_string(index=False))

    # Distance Statistics
    print("\n\n📏 DISTANCE STATISTICS")
    print("-"*80)

    distance_data = {
        'Metric': ['Average Distance', 'Standard Deviation'],
        'Value': [
            f"{basic_stats['avg_distance']:.2f} miles",
            f"{basic_stats['std_distance']:.2f} miles"
        ]
    }
    df_distance = pd.DataFrame(distance_data)
    print(df_distance.to_string(index=False))

    # Key Insights
    print("\n\n🔍 KEY INSIGHTS")
    print("-"*80)

    # Calculate additional metrics
    avg_revenue_per_mile = basic_stats['total_revenue'] / basic_stats['total_distance']
    avg_trips_per_day = basic_stats['total_trips'] / 365  # Assuming 1 year of data

    insights = [
        f"• Total trips analyzed: {basic_stats['total_trips']:,}",
        f"• Average trip: ${basic_stats['avg_fare']:.2f} fare, {basic_stats['avg_distance']:.2f} miles",
        f"• Total revenue: ${basic_stats['total_revenue']/1e9:.2f} billion",
        f"• Total distance: {basic_stats['total_distance']/1e9:.2f} billion miles",
        f"• Revenue per mile: ${avg_revenue_per_mile:.2f}",
        f"• Median fare is {((basic_stats['avg_fare'] - basic_stats['median_fare'])/basic_stats['median_fare']*100):.1f}% lower than average",
        f"  (indicating right-skewed distribution with some high-value trips)"
    ]

    for insight in insights:
        print(f"  {insight}")

    print("\n" + "="*80 + "\n")

def export_to_csv():
    """Export to CSV file"""
    # Combine all metrics
    data = {
        'Category': [
            'Trip Volume', 'Trip Volume', 'Trip Volume',
            'Fare', 'Fare', 'Fare',
            'Distance', 'Distance'
        ],
        'Metric': [
            'Total Trips', 'Total Revenue', 'Total Distance',
            'Average Fare', 'Std Dev Fare', 'Median Fare',
            'Average Distance', 'Std Dev Distance'
        ],
        'Value': [
            basic_stats['total_trips'],
            basic_stats['total_revenue'],
            basic_stats['total_distance'],
            basic_stats['avg_fare'],
            basic_stats['std_fare'],
            basic_stats['median_fare'],
            basic_stats['avg_distance'],
            basic_stats['std_distance']
        ]
    }

    df = pd.DataFrame(data)
    filename = '/Users/shreya/SJSU-Github/DA228/basic_stats.csv'
    df.to_csv(filename, index=False)
    print(f"✅ CSV file created: {filename}")

def create_comparison_table():
    """Create a detailed comparison table"""
    print("\n" + "="*80)
    print("DETAILED STATISTICS TABLE")
    print("="*80 + "\n")

    data = {
        'Category': ['Trip Volume', 'Trip Volume', 'Trip Volume', '',
                     'Fare Analysis', 'Fare Analysis', 'Fare Analysis', '',
                     'Distance Analysis', 'Distance Analysis'],
        'Metric': ['Total Trips', 'Total Revenue ($)', 'Total Distance (mi)', '',
                   'Average Fare ($)', 'Median Fare ($)', 'Std Dev ($)', '',
                   'Average Distance (mi)', 'Std Dev (mi)'],
        'Value': [
            f"{basic_stats['total_trips']:,}",
            f"{basic_stats['total_revenue']:,.2f}",
            f"{basic_stats['total_distance']:,.2f}",
            '',
            f"{basic_stats['avg_fare']:.2f}",
            f"{basic_stats['median_fare']:.2f}",
            f"{basic_stats['std_fare']:.2f}",
            '',
            f"{basic_stats['avg_distance']:.2f}",
            f"{basic_stats['std_distance']:.2f}"
        ]
    }

    df = pd.DataFrame(data)
    print(df.to_string(index=False))
    print("\n" + "="*80 + "\n")

if __name__ == "__main__":
    display_as_table()
    create_comparison_table()
    export_to_csv()
