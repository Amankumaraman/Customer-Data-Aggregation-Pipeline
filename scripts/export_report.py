import pandas as pd
from scripts.aggregation import get_customer_aggregation  

def export_to_csv():
    data = get_customer_aggregation()
    
    if not data:
        print("⚠️ No data found for export!")
        return
    
    df = pd.DataFrame(data)
    df.to_csv("customer_report.csv", index=False)
    print("✅ Report exported to customer_report.csv")

if __name__ == "__main__":
    export_to_csv()
