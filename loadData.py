import pandas as pd
from pymongo import MongoClient

# MongoDB connection details
MONGO_URI = "mongodb+srv://samirziani:samir5636123@cluster0.ghz8l.mongodb.net/"
DATABASE_NAME = "sample_mflix"
COLLECTION_NAME = "articles"

# Paths to the Excel files
excel_files = ["data/merged data 1.xls", "data/merged data 2.xls", "data/merged data 3.xls"]

# Connect to MongoDB
try:
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]
    print("Connected to MongoDB!")
except Exception as e:
    print(f"Failed to connect to MongoDB: {e}")
    exit()

# Process each Excel file
for file_path in excel_files:
    try:
        # Load Excel data
        data = pd.read_excel(file_path)

        # Convert DataFrame to dictionary
        articles = data.to_dict(orient="records")

        # Insert documents into MongoDB
        for article in articles:
            # Check for duplicates using a combination of `doi`, `author`, and `title`
            query = {
                "doi": article.get("doi", ""),
                "author": article.get("author", ""),
                "title": article.get("title", "")
            }

            # Skip insertion if a duplicate is found
            if collection.find_one(query):
                print(f"Skipped existing article: {article['title']} by {article['author']} (DOI: {article['doi']})")
                continue

            # Insert all fields from the article into the database
            collection.insert_one(article)
            print(f"Inserted article: {article.get('title', 'N/A')} by {article.get('author', 'N/A')} (DOI: {article.get('doi', 'N/A')})")
    except Exception as e:
        print(f"Failed to process file {file_path}: {e}")

# Close MongoDB connection
client.close()
print("Finished processing all files.")
