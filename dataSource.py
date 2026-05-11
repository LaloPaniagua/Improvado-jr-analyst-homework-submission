import pandas as pd
import numpy as np
from datetime import datetime
import os


output_dir = "marketing_databases"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)


start_date = datetime(2022, 1, 1)
end_date = datetime(2024, 12, 31)


date_range = pd.date_range(start_date, end_date, freq='W-FRI')
n_weeks = len(date_range)

source_to_channels = {
    'Amazon_Ad_Server': ['Programmatic'],
    'StackAdapt': ['Programmatic'],
    'DV360': ['Programmatic'],
    'SA360': ['Paid_Search', 'Organic_Search'],
    'Bing_Ads': ['Paid_Search', 'Organic_Search'],
    'LinkedIn': ['Paid_Social', 'Organic_Social'],
    'Facebook': ['Paid_Social', 'Organic_Social']
}

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

def get_season_id(date):
    """Returns a string ID based on the month: Spring(01), Summer(02), Autumn(03), Winter(04)."""
    month = date.month
    if 3 <= month <= 5: return "01"
    elif 6 <= month <= 8: return "02"
    elif 9 <= month <= 11: return "03"
    else: return "04" 

def get_channel_signature(channel, week_index):
    if 'Search' in channel:
        return 1.0 + 0.3 * np.sin(week_index * (np.pi / 2))
    elif 'Programmatic' in channel:
        return 1.2 if (week_index // 3) % 2 == 0 else 0.8
    elif 'Social' in channel:
        return np.random.uniform(0.6, 1.4)
    else:
        return 1.0 + np.random.normal(0, 0.05)

TOTAL_WEEKLY_TARGET = 3000000

for source, associated_channels in source_to_channels.items():
    source_master_data = []
    camp_weights = np.random.dirichlet(np.ones(len(campaigns)), size=1)[0] * len(campaigns)
    
    for channel in associated_channels:
        for i, date in enumerate(date_range):
            month_in_quarter = (date.month - 1) % 3 
            base_seasonality = [1.1, 0.8, 1.3][month_in_quarter]
            channel_shape = get_channel_signature(channel, i)
            weekly_source_spend = (TOTAL_WEEKLY_TARGET / 7) * base_seasonality * channel_shape
            season_id = get_season_id(date)
            
            for idx, campaign in enumerate(campaigns):
                campaign_id = f"{idx + 1:02d}"
                ad_set = f"{campaign_id}-{season_id}"
                
                camp_noise = np.random.uniform(0.98, 1.02)
                spend = (weekly_source_spend / len(campaigns)) * camp_weights[idx] * camp_noise
                
                if 'Organic' in channel:
                    spend = 0.0
                    impressions = np.random.randint(150, 400)
                else:
                    cpm_val = np.random.normal(405000, 10000)
                    impressions = (spend / cpm_val) * 1000
                
                actual_ctr = np.random.normal(0.105, 0.001)
                clicks = impressions * actual_ctr
                actual_cvr = np.random.normal(0.098, 0.003)
                conversions = clicks * actual_cvr
                
                view_rate = np.random.uniform(1.03, 1.07)
                video_views = impressions * view_rate if 'Search' not in channel else 0
                
                source_master_data.append([
                    date, channel, source, campaign, ad_set, round(spend, 2), 
                    int(round(impressions)), int(round(clicks)), 
                    int(round(conversions)), int(round(video_views))
                ])
    
    df = pd.DataFrame(
        source_master_data, 
        columns=['Date', 'Channel', 'Source', 'Campaign', 'Ad Set', 'Spend', 
                 'Impressions', 'Clicks', 'Conversions', 'Video Views']
    )


    df['Ad Set'] = df['Ad Set'].astype(str)
    
    file_name = f"{output_dir}/{source}_db.csv"
    df.to_csv(file_name, index=False)
    print(f"Generated uniquely-trending database for {source}")

print(f"\nSuccess! New 'Ad Set' column added with seasonal logic.")