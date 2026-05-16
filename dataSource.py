# ==============================================================================
# MARKETING DATA GENERATION SCRIPT
# Objective: Simulate historical marketing campaign performance data across various 
# channels and platforms for portfolio projects / testing ETL pipelines.
# ==============================================================================

import pandas as pd
import numpy as np
from datetime import datetime
import os

# --- Environment Setup ---
# Creating a dedicated output directory to keep raw data separate and organized
output_dir = "marketing_databases"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# --- Date Configuration ---
# Setting up a 3-year historical window for data trend analysis
start_date = datetime(2022, 1, 1)
end_date = datetime(2024, 12, 31)

# Sampling at a weekly frequency (Friday data points) to simulate regular reporting intervals
date_range = pd.date_range(start_date, end_date, freq='W-FRI')
n_weeks = len(date_range)

# --- Marketing Structure Mapping ---
# Dictionary to map platforms (Sources) to specific marketing tactics (Channels)
source_to_channels = {
    'Amazon_Ad_Server': ['Programmatic'],
    'StackAdapt': ['Programmatic'],
    'DV360': ['Programmatic'],
    'SA360': ['Paid_Search', 'Organic_Search'],
    'Bing_Ads': ['Paid_Search', 'Organic_Search'],
    'LinkedIn': ['Paid_Social', 'Organic_Social'],
    'Facebook': ['Paid_Social', 'Organic_Social']
}

# Standardized campaign naming conventions used across the marketing mix
campaigns = [
    "Business-focused zero tolerance architecture",
    "Persistent 24/7 attitude",
    "Integrated dedicated contingency",
    "Profound intangible policy",
    "Centralized modular throughput",
    "Automated uniform software",
    "Cross-platform static hierarchy",
    "Networked value-added time-frame"
]

# --- Helper Functions for Data Transformation ---

def get_season_id(date):
    """
    Categorizes dates into seasonal numeric IDs to match business quarters/seasons.
    Returns: Spring(01), Summer(02), Autumn(03), Winter(04).
    """
    month = date.month
    if 3 <= month <= 5: return "01"
    elif 6 <= month <= 8: return "02"
    elif 9 <= month <= 11: return "03"
    else: return "04" 

def get_channel_signature(channel, week_index):
    """
    Applies custom logic/curves based on the channel type to simulate realistic trends.
    e.g., Search uses cyclical patterns, Programmatic uses step functions, Social is highly volatile.
    """
    if 'Search' in channel:
        return 1.0 + 0.3 * np.sin(week_index * (np.pi / 2)) # Cyclical/sine wave performance
    elif 'Programmatic' in channel:
        return 1.2 if (week_index // 3) % 2 == 0 else 0.8  # Budget pacing fluctuations
    elif 'Social' in channel:
        return np.random.uniform(0.6, 1.4)                 # Highly dynamic weekly variance
    else:
        return 1.0 + np.random.normal(0, 0.05)             # Default baseline with minor random noise

# Target baseline budget per week to distribute across the mix
TOTAL_WEEKLY_TARGET = 3000000

# --- Main Data Generation Loop ---
# Iterating through platforms to build out the granular tabular rows
for source, associated_channels in source_to_channels.items():
    source_master_data = []
    
    # Generate random weights for each campaign so budget allocation isn't perfectly flat
    camp_weights = np.random.dirichlet(np.ones(len(campaigns)), size=1)[0] * len(campaigns)
    
    for channel in associated_channels:
        for i, date in enumerate(date_range):
            # Calculate intra-quarter seasonality (simulating spikes/dips depending on the month of the quarter)
            month_in_quarter = (date.month - 1) % 3 
            base_seasonality = [1.1, 0.8, 1.3][month_in_quarter]
            
            # Apply the specific channel performance characteristics
            channel_shape = get_channel_signature(channel, i)
            
            # Formulate the baseline weekly spend calculation
            weekly_source_spend = (TOTAL_WEEKLY_TARGET / 7) * base_seasonality * channel_shape
            season_id = get_season_id(date)
            
            for idx, campaign in enumerate(campaigns):
                # Creating standardized tracking keys (Campaign ID and Ad Set codes)
                campaign_id = f"{idx + 1:02d}"
                ad_set = f"{campaign_id}-{season_id}"
                
                # Adding minor variance to campaign spend to look like real-world execution
                camp_noise = np.random.uniform(0.98, 1.02)
                spend = (weekly_source_spend / len(campaigns)) * camp_weights[idx] * camp_noise
                
                # Business Rule Check: Organic channels should have 0 financial cost but still track engagement
                if 'Organic' in channel:
                    spend = 0.0
                    impressions = np.random.randint(150, 400) # Organic baseline reach
                else:
                    # Paid channels calculate reach based on a mock CPM (Cost Per Mille) metric
                    cpm_val = np.random.normal(405000, 10000)
                    impressions = (spend / cpm_val) * 1000
                
                # --- Funnel Performance Simulation (Impressions -> Clicks -> Conversions) ---
                # CTR (Click-Through Rate) logic
                actual_ctr = np.random.normal(0.105, 0.001)
                clicks = impressions * actual_ctr
                
                # CVR (Conversion Rate) logic
                actual_cvr = np.random.normal(0.098, 0.003)
                conversions = clicks * actual_cvr
                
                # Video View Metrics (Only relevant for non-search mediums)
                view_rate = np.random.uniform(1.03, 1.07)
                video_views = impressions * view_rate if 'Search' not in channel else 0
                
                # Append processed row to master data collector list
                source_master_data.append([
                    date, channel, source, campaign, ad_set, round(spend, 2), 
                    int(round(impressions)), int(round(clicks)), 
                    int(round(conversions)), int(round(video_views))
                ])
    
    # Convert list of rows into a clean, structured Pandas DataFrame
    df = pd.DataFrame(
        source_master_data, 
        columns=['Date', 'Channel', 'Source', 'Campaign', 'Ad Set', 'Spend', 
                 'Impressions', 'Clicks', 'Conversions', 'Video Views']
    )

    # Data Type Enforcement: Explicitly casting tracking key to string format
    df['Ad Set'] = df['Ad Set'].astype(str)
    
    # --- Data Export ---
    # Saving individual source files into the outputs directory without index column for clean import later
    file_name = f"{output_dir}/{source}_db.csv"
    df.to_csv(file_name, index=False)
    print(f"Generated uniquely-trending database for {source}")

print(f"\nSuccess! New 'Ad Set' column added with seasonal logic.")
