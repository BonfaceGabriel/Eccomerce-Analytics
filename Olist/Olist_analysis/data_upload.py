
import os
import sys
import json
import pymongo
import datetime
import requests
import pandas as pd
from io import open
from bson import json_util, ObjectId
import random
import string
import time

# HELPER FUNCTIONS
ECOMMERCE_DATA_MONGO_CREDENTIALS = "mongodb://localhost/27017"
ECOMMERCE_DB_NAME = "brazillian_ecommerce_db"

class Starter():

    def set_up_database(self):
        # SET UP MONGO DB
        MONGO_CREDENTIALS = ECOMMERCE_DATA_MONGO_CREDENTIALS
        DB_NAME = ECOMMERCE_DB_NAME
        client = pymongo.MongoClient(MONGO_CREDENTIALS)
        database_client = client[DB_NAME]

        print(f"Connected to ..... {DB_NAME}",file=sys.stderr)

        return database_client

    def mongo_to_dataframe(self,mongo_data):
        """
        transform mongo data to a pandas dataframe
        """
        sanitized = json.loads(json_util.dumps(mongo_data))
        normalized = pd.json_normalize(sanitized)
        
        df = pd.DataFrame(normalized)

        return df


starter = Starter()
db = starter.set_up_database()

# DELETE ALL DOCUMENTS IN A COLLECTION
# collection = db["customer_geolocation"]
# collection.delete_many({})




def generate_custom_id():
    timestamp = datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')
    machine_id = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
    process_id = ''.join(random.choices(string.ascii_lowercase + string.digits, k=5))
    counter = ''.join(random.choices(string.ascii_lowercase + string.digits, k=4))
    return f'{timestamp}{machine_id}{process_id}{counter}'

def upload_customer_data(df,collection_name):
    collection = db[collection_name]
    # Add custom _id column to the DataFrame
    df['_id'] = df.apply(lambda row: generate_custom_id(), axis=1)
    documents = df.to_dict(orient='records')
    result = collection.insert_many(documents)
    inserted_ids = [str(inserted_id) for inserted_id in result.inserted_ids]

    print(f"Uploaded {len(inserted_ids)} documents to the {collection} collection.")

# Load Data
# customers_df = pd.read_csv("./data/olist_customers_dataset.csv")
# print(customers_df.shape) 

# geolocation_df = pd.read_csv("./data/olist_geolocation_dataset.csv")
# print(geolocation_df.shape) 

# order_items_df = pd.read_csv("./data/olist_order_items_dataset.csv")
# print(order_items_df.shape) s

# order_payments_df = pd.read_csv("./data/olist_order_payments_dataset.csv")
# print(order_payments_df.shape) 

# order_reviews_df = pd.read_csv("./data/olist_order_reviews_dataset.csv")
# print(order_reviews_df.shape) 

# order_details_df = pd.read_csv("./data/olist_orders_dataset.csv")
# print(order_details_df.shape) 

# product_details_df = pd.read_csv("./data/olist_products_dataset.csv")
# print(product_details_df.shape) 

# sellers_df = pd.read_csv("./data/olist_sellers_dataset.csv")
# print(sellers_df.shape) 

# product_category_df = pd.read_csv("./data/product_category_name_translation.csv")
# print(product_category_df.shape) 

# closed_deals_df = pd.read_csv("./data/olist_closed_deals_dataset.csv")
# print(closed_deals_df.shape) 

# marketing_leads_df = pd.read_csv("./data/olist_marketing_qualified_leads_dataset.csv")
# print(marketing_leads_df.shape) 

# # Insert in MongoDB
# upload_customer_data(customers_df,"customers")
# time.sleep(60)

# upload_customer_data(geolocation_df,"customer_geolocation")
# time.sleep(60)

# upload_customer_data(order_items_df,"order_items")
# time.sleep(60)

# upload_customer_data(order_payments_df,"order_payments")
# time.sleep(60)

# upload_customer_data(order_reviews_df,"order_reviews")
# time.sleep(60)

# upload_customer_data(order_details_df,"order_details")
# time.sleep(60)

# upload_customer_data(product_details_df,"product_details")
# time.sleep(60)

# upload_customer_data(sellers_df,"sellers")
# time.sleep(60)

# upload_customer_data(product_category_df,"product_category")

# upload_customer_data(closed_deals_df,"closed_deals")
# time.sleep(60)

# upload_customer_data(marketing_leads_df,"marketing_leads")