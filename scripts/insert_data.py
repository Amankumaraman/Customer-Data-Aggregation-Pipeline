from pymongo import MongoClient
import json

client = MongoClient("mongodb://localhost:27017/")
db = client["ecommerce"]
collection = db["customers"]

# Load sample data
with open("data/sample_data.json") as f:
    data = json.load(f)

# Insert into MongoDB
collection.delete_many({})  
collection.insert_many(data)
print("✅ Sample data inserted successfully!")
