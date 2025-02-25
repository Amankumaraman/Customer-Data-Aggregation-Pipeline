from scripts.insert_data import collection
from scripts.aggregation import get_customer_aggregation  # Corrected import
from scripts.export_report import export_to_csv

if __name__ == "__main__":
    print("✅ Running customer data aggregation pipeline...\n")
    
    print("📌 Fetching customer insights...")
    results = get_customer_aggregation()
    for r in results:
        print(r)

    print("\n📌 Exporting to CSV...")
    export_to_csv()
    
    print("\n✅ Process Completed!")
