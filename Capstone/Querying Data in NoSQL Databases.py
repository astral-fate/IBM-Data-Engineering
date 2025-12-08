# Step 1: Install the mongomock library (simulates MongoDB in Colab)
!pip install mongomock

import json
import mongomock
from pymongo import MongoClient

# Step 2: Create a virtual MongoDB connection
# We use mongomock to simulate a connection without needing a real server
client = mongomock.MongoClient()

# Step 3: Create the Database and Collection
# The quiz asks about the 'catalog' database and 'electronics' collection
db = client.catalog
collection = db.electronics

# Step 4: Load your 'data.json' file
# We read the file line-by-line because your file is in "JSON Lines" format
imported_data = []
try:
    with open('data.json', 'r') as f:
        for line in f:
            if line.strip(): # Skip empty lines
                imported_data.append(json.loads(line))
    
    # Insert the data into the virtual collection
    if imported_data:
        collection.insert_many(imported_data)
        print(f"Successfully loaded {len(imported_data)} documents into 'catalog.electronics'.\n")
    else:
        print("Error: data.json was empty.")

except FileNotFoundError:
    print("Error: Please upload 'data.json' to the Colab Files section first!")

# ==========================================
# ANSWERS TO THE QUIZ QUESTIONS
# ==========================================

print("--- QUIZ ANSWERS ---")

# Question 2: How many collections are in the 'catalog' database?
# (We created 1 collection named 'electronics')
cols = db.list_collection_names()
print(f"Q2: Collections in 'catalog' database: {len(cols)} ({cols})")

# Question 3: How many laptops are listed in the 'electronics' collection?
# We query for documents where "type" is "laptop"
laptop_count = collection.count_documents({"type": "laptop"})
print(f"Q3: Number of laptops: {laptop_count}")

# Question 4: How many documents were imported?
# Total count of all documents
total_docs = collection.count_documents({})
print(f"Q4: Total documents imported: {total_docs}")

# Question 5: What is the average screen size of mobile phones?
# We need to filter for "smart phone" and then calculate the average of "screen size"
pipeline = [
    {
        "$match": { "type": "smart phone" }
    },
    {
        "$group": {
            "_id": None,
            "avg_screen_size": { "$avg": "$screen size" }
        }
    }
]

# Run the aggregation
result = list(collection.aggregate(pipeline))

if result:
    avg_size = result[0]['avg_screen_size']
    print(f"Q5: Average screen size of mobile phones: {avg_size}")
else:
    print("Q5: No mobile phones found to calculate average.")
