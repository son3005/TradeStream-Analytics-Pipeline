import csv
from datetime import datetime, timedelta
import os

def generate_dim_date(start_year=2000, end_year=2050, output_path=None):
    if output_path is None:
        # Save to raw_data or project root
        output_path = os.path.join(os.path.dirname(__file__), "dim_date.csv")
        
    start_date = datetime(start_year, 1, 1)
    end_date = datetime(end_year, 12, 31)
    
    headers = [
        "date_key",
        "full_date",
        "year",
        "quarter",
        "month",
        "day",
        "day_of_week",
        "day_name",
        "month_name",
        "is_weekend"
    ]
    
    current_date = start_date
    rows = []
    
    while current_date <= end_date:
        date_key = int(current_date.strftime("%Y%m%d"))
        full_date = current_date.strftime("%Y-%m-%d")
        year = current_date.year
        quarter = (current_date.month - 1) // 3 + 1
        month = current_date.month
        day = current_date.day
        day_of_week = current_date.weekday() + 1  # 1 (Monday) to 7 (Sunday)
        day_name = current_date.strftime("%A")
        month_name = current_date.strftime("%B")
        is_weekend = int(day_of_week in [6, 7])
        
        rows.append([
            date_key,
            full_date,
            year,
            quarter,
            month,
            day,
            day_of_week,
            day_name,
            month_name,
            is_weekend
        ])
        current_date += timedelta(days=1)
        
    # Ensure directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)
    
    print(f"[OK] Generated {len(rows)} date records from {start_year} to {end_year} at:")
    print(f"     {os.path.abspath(output_path)}")

if __name__ == "__main__":
    generate_dim_date()
