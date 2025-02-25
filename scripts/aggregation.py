from pymongo import MongoClient
from datetime import datetime, timedelta

client = MongoClient("mongodb://localhost:27017/")
db = client["ecommerce"]
collection = db["customers"]

six_months_ago = datetime.utcnow() - timedelta(days=180)

pipeline = [
    {"$unwind": "$orders"},
    
    {"$group": {
        "_id": "$customerId",
        "name": {"$first": "$name"},
        "email": {"$first": "$email"},
        "totalSpent": {"$sum": "$orders.amount"},
        "averageOrderValue": {"$avg": "$orders.amount"},
        "categoryCounts": {"$push": "$orders.category"},
        "categoryWiseSpend": {
            "$push": {"category": "$orders.category", "amount": "$orders.amount"}
        },
        "lastPurchaseDate": {"$max": "$orders.date"}
    }},
    
    {"$addFields": {
        "loyaltyTier": {
            "$switch": {
                "branches": [
                    {"case": {"$gte": ["$totalSpent", 3000]}, "then": "Gold"},
                    {"case": {"$gte": ["$totalSpent", 1000]}, "then": "Silver"}
                ],
                "default": "Bronze"
            }
        },
        "isActive": {"$gte": ["$lastPurchaseDate", six_months_ago.isoformat()]}
    }},
    
    {"$project": {
        "_id": 0,
        "customerId": "$_id",
        "name": 1,
        "email": 1,
        "totalSpent": 1,
        "averageOrderValue": 1,
        "favoriteCategory": {
            "$arrayElemAt": [
                {"$sortArray": {"input": "$categoryCounts", "sortBy": 1}}, 0
            ]
        },
        "categoryWiseSpend": 1,
        "loyaltyTier": 1,
        "lastPurchaseDate": 1,
        "isActive": 1
    }},
    
    {"$match": {"totalSpent": {"$gte": 500}}}
]

def get_customer_aggregation():
    return list(collection.aggregate(pipeline))
