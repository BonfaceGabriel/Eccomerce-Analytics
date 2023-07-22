import pandas as pd
import numpy as np
from .data_upload import Starter
from rest_framework.response import Response
from rest_framework import status


starter = Starter()
db = starter.set_up_database()


def revenue_growth(date1, date2):
    #dataset
    orders_item = db["order_items"]
    orders_item_df = starter.mongo_to_dataframe(list(orders_item.find()))

    orders = db["order_details"]
    orders_df = starter.mongo_to_dataframe(list(orders.find()))

    merge_data = pd.merge(orders_df, orders_item_df, on='order_id', )
    
    #datetime
    merge_data['order_purchase_timestamp'] = pd.to_datetime(merge_data['order_purchase_timestamp']).dt.date
    
    # if  not merge_data['order_purchase_timestamp'].isin([date1, date2]).all():
    #     return Response({"status": status.HTTP_204_NO_CONTENT, "message": f"No record found for {date1} or {date2}", "payload":None})
    # else:
    #start/end date
    first_date = pd.to_datetime(date1, format='%B %Y').date()
    last_date = pd.to_datetime(date2, format='%B %Y').date() + pd.offsets.MonthEnd(1)
    last_date = last_date.date()

    #filter
    filtered_data = merge_data[(merge_data['order_purchase_timestamp'] >= first_date) &
                                (merge_data['order_purchase_timestamp'] <= last_date)]

    #revenue on orders
    filtered_data['total_revenue'] =   filtered_data['price'] + filtered_data['freight_value']

    #revenue by month
    filtered_data['order_purchase_timestamp'] = pd.to_datetime(filtered_data['order_purchase_timestamp'])
    revenue_by_month = filtered_data.set_index('order_purchase_timestamp')['total_revenue'].resample('M').sum()
    # date_range = pd.date_range(start=first_date, end=last_date, freq='M')
    # missing_months_df = pd.DataFrame({'total_revenue': [0] * len(date_range)}, index=date_range)
    # combined_revenue = pd.concat([missing_months_df, revenue_by_month], axis=1)['total_revenue']



    #percentage
    revenue_growth = revenue_by_month.pct_change() * 100
    revenue_growth = revenue_growth.round(2)

    if first_date in revenue_by_month.index:
        first_date_index = revenue_by_month.index.get_loc(first_date)
        revenue_growth.iloc[first_date_index] = 0.0

    revenue_growth = revenue_growth.dropna()[0:]


    revenue_growth = dict(zip(revenue_growth.index.strftime('%B %Y'), revenue_growth.values))


    return revenue_growth

def best_selling_categories(num_categories):
    #dataset
    orders_item = db["order_items"]
    orders_item_df = starter.mongo_to_dataframe(list(orders_item.find()))

    query = {'product_id' : 1,  'product_category_name' : 1}
    products = db["product_details"]
    products_df = starter.mongo_to_dataframe(list(products.find({}, query)))
     
    translation = db['product_category']
    trans_df =  starter.mongo_to_dataframe(list(translation.find()))

    merge_data = pd.merge(orders_item_df, products_df, on='product_id', )
    merged_data = pd.merge(merge_data, trans_df, on='product_category_name')

    # total quantity sold per category
    product_category_sales = merged_data.groupby('product_category_name_english')['order_item_id'].sum().reset_index()

    # Sort  in descending order
    sorted_categories = product_category_sales.sort_values('order_item_id', ascending=False)

    #  number of categories to show
    top_categories = sorted_categories.head(num_categories)

   #result
    top_categories = dict(zip(top_categories['product_category_name_english'], top_categories['order_item_id']))

    return top_categories

def best_categories_by_revenue(num_categories):
    #dataset
    orders_item = db["order_items"]
    orders_item_df = starter.mongo_to_dataframe(list(orders_item.find()))

    query = {'product_id' : 1,  'product_category_name' : 1}
    products = db["product_details"]
    products_df = starter.mongo_to_dataframe(list(products.find({}, query)))
     
    translation = db['product_category']
    trans_df =  starter.mongo_to_dataframe(list(translation.find()))

    merge_data = pd.merge(orders_item_df, products_df, on='product_id', )
    merged_data = pd.merge(merge_data, trans_df, on='product_category_name')
    
    #revenue
    merged_data['total_revenue'] = merged_data['price'] + merged_data['freight_value']

    # total revenue per category
    product_category_sales = merged_data.groupby('product_category_name_english')['total_revenue'].sum().reset_index()

    # Sort  in descending order
    sorted_categories = product_category_sales.sort_values('total_revenue', ascending=False)

    #  number of categories to show
    top_categories = sorted_categories.head(num_categories)

   #result
    top_categories = dict(zip(top_categories['product_category_name_english'], top_categories['total_revenue']))

    return top_categories

def analyze_order_hours():
    # dataset
    orders = db["order_details"]
    orders_df = starter.mongo_to_dataframe(list(orders.find()))

    # datetime format
    orders_df['order_purchase_timestamp'] = pd.to_datetime(orders_df['order_purchase_timestamp'])

    # Extract hour from the timestamp
    orders_df['hour_of_day'] = orders_df['order_purchase_timestamp'].dt.hour

    # Categorize the hours into morning, afternoon, evening, and night
    bins = [0, 6, 12, 18, 24]
    labels = ['Night', 'Morning', 'Afternoon', 'Evening']
    orders_df['time_category'] = pd.cut(orders_df['hour_of_day'], bins=bins, labels=labels, right=False)

    # Count the number of orders per time category
    order_count = orders_df['time_category'].value_counts().sort_index()
    order_count = order_count.to_dict()

    return order_count

def analyze_order_weekday():
    # dataset
    orders = db["order_details"]
    orders_df = starter.mongo_to_dataframe(list(orders.find()))

    # datetime format
    orders_df['order_purchase_timestamp'] = pd.to_datetime(orders_df['order_purchase_timestamp'])

    # extract weekday names from the timestamp
    orders_df['day_of_week'] = orders_df['order_purchase_timestamp'].dt.day_name()
    
    weekday_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    orders_df['day_of_week'] = pd.Categorical(orders_df['day_of_week'], categories=weekday_order, ordered=True)

    # number of orders per weekday
    order_count = orders_df['day_of_week'].value_counts().sort_index()
    order_count = order_count.to_dict()

    return order_count

def  percentage_change_in_sales(date1, date2):
    orders = db["order_details"]
    orders_df = starter.mongo_to_dataframe(list(orders.find()))

    #datetime
    orders_df['order_purchase_timestamp'] = pd.to_datetime(orders_df['order_purchase_timestamp']).dt.date
    
    #start/end date
    first_date = pd.to_datetime(date1, format='%B %Y').date()
    last_date = pd.to_datetime(date2, format='%B %Y').date() + pd.offsets.MonthEnd(1)
    last_date = last_date.date()
    
    
    #filter
    filtered_data = orders_df[(orders_df['order_purchase_timestamp'] >= first_date) &
                                (orders_df['order_purchase_timestamp'] <= last_date)]
    
    # percentage change in sales
    filtered_data['order_purchase_timestamp'] = pd.to_datetime(filtered_data['order_purchase_timestamp'])
    sales_by_month = filtered_data.set_index('order_purchase_timestamp')['order_id'].resample('M').count()
    
    sales_by_month = sales_by_month.pct_change().fillna(0)* 100
    sales_by_month = sales_by_month.round(2)
    sales_by_month = sales_by_month[1:]
    
    sales_by_month = dict(zip(sales_by_month.index.strftime('%B %Y'), sales_by_month.values))

    
    return sales_by_month




# customer data
# query = {"_id":1,"customer_id":1,"customer_city":1,"customer_state":1}
# query_filter = {"customer_city":"franca"}
# customers = db["customers"]
# customers_df = starter.mongo_to_dataframe(list(customers.find(query_filter, query)))

# query = {"_id":1,"geolocation_zip_code_prefix":1,"geolocation_city":1,"geolocation_state":1}
# query_filter = {"geolocation_city":"sao paulo"}
# location = db["customer_geolocation"]
# geolocation_df = starter.mongo_to_dataframe(list(location.find(query_filter, query)))

# # orders data
# orders_query = {"_id":1,"customer_id":1,"order_status":1, "order_delivered_customer_date":1}
# order_details = db["order_details"]
# order_df = starter.mongo_to_dataframe(list(order_details.find({},orders_query)))

# print("Customers=========================")
# print(customers_df.shape)
# print(customers_df.head(10))

# print("Locations=========================")
# print(geolocation_df.shape)
# print(geolocation_df.head(10))

# print("Orders=========================")
# print(order_df.shape)
# print(order_df.head(10))


# df = customers_df.merge(order_df, on='customer_id', how='left')
# print(df.head())

