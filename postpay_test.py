import sys
import pandas as pd
from datetime import datetime,date, timedelta
#import traceback


custid_order_count = None
shopid_order_count = None
orders = None

def orders_on_customer_id(order_id):

	current_row = orders.loc[orders['order_id'] == order_id]
	cust_id = current_row['customer_id'].item()
	created_date = current_row['created_date'].item().replace(hour=0, minute=0, second=0)
	week_ago = (created_date - timedelta(days=7)).replace(hour=0, minute=0, second=0)

	cust_orders = custid_order_count.get(cust_id)
	num_of_orders = 0

	if cust_orders > 1:
		# customer has more than 1 order, check the number of orders on customer in the last 7 days

		previous_order_in_7days = orders.loc[(orders['customer_id']) == cust_id].loc[orders['created_date'].between(week_ago, created_date)]['created_date']
		if not previous_order_in_7days.empty:
			num_of_orders = len(previous_order_in_7days.index)
			#print("previous_order_dates: ", previous_order_in_7days, "count: ", len(previous_order_in_7days.index))

	return num_of_orders


def customer_paid_previous_order_check(order_id):

	current_row = orders.loc[orders['order_id'] == order_id]
	cust_id = current_row['customer_id'].item()
	created_date = current_row['created_date'].item().replace(hour=0, minute=0)

	cust_orders = custid_order_count.get(cust_id)
	customer_id_has_paid = False

	if cust_orders > 1:
		# customer has more than 1 order, customer_id_has_paid :  True - if the customer has paid prior to this order, False - Otherwise
		previous_paid_order = orders.loc[(orders['customer_id']) == cust_id].loc[orders['created_date'] < created_date].loc[orders['order_status'] == 'paid']['order_status']
		if not previous_paid_order.empty:
			customer_id_has_paid = True

	return customer_id_has_paid

def shopid_previous_90days_paid_count(order_id):

	current_row = orders.loc[orders['order_id'] == order_id]
	shop_id = current_row['shop_id'].item()
	created_date = current_row['created_date'].item().replace(hour=0, minute=0)
	days_ago = (created_date - timedelta(days=90)).replace(hour=0, minute=0, second=0)


	shop_orders = shopid_order_count.get(shop_id)
	num_of_orders = 0

	if shop_orders > 1:
		# more than 1 order, check the number of paid orders on this shop in the last 90 days

		previous_order_in_90days = orders.loc[(orders['shop_id']) == shop_id].loc[orders['created_date'].between(days_ago, created_date)].loc[orders['order_status'] == 'paid']['order_status']
		if not previous_order_in_90days.empty:
			num_of_orders = len(previous_order_in_90days.index)
			#print("previous_order_in_90days: ", previous_order_in_90days, "count: ", len(previous_order_in_90days.index))

	return num_of_orders

try:
	#Read The data from CSV
	csv_file = 'interview_df.csv'

	# This code accepts the file name from commandline as well
	if len(sys.argv) == 2:
		csv_file = sys.argv[1]

	print("processing csv file:", csv_file)
	orders =  pd.read_csv(csv_file, index_col=False)
	orders['created_date']=pd.to_datetime(orders['created_date'])

	#Initialize new columns
	orders["orders_on_customer_id_7D"] = "0"
	orders["customer_id_has_paid"] = "0"
	orders["shop_id_count_paid_orders_90D"] = "0"

	#Grouping and perform count over each group
	shopid_order_count =  orders.groupby('shop_id')['shop_id'].count()

	custid_order_count =  orders.groupby('customer_id')['customer_id'].count()

	# search records and apply the logic for 3 new columns
	orders['orders_on_customer_id_7D'] = orders['order_id'].apply(orders_on_customer_id)
	orders["customer_id_has_paid"] = orders['order_id'].apply(customer_paid_previous_order_check)
	orders["shop_id_count_paid_orders_90D"] = orders['order_id'].apply(shopid_previous_90days_paid_count)

	result_file = 'interview_df_result.csv'
	orders.to_csv(result_file, index=False)
	print("result file processed:", result_file)

except Exception as e:
    #traceback.print_exc()
    print("Exception: ", e) 