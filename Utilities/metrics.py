
from utilities.utils import *


def impute_columns(df):
    
    df['travel_fare']=fill_missing_values(df=df,col_to_impute='travel_fare',grouping_var='customer_id',var_type='continuous',method='median')
    df['business_types_id']=fill_missing_values(df=df,col_to_impute='business_types_id',grouping_var='customer_id',var_type='categorical',method='mode')
    df['pickup_cluster_id']=fill_missing_values(df=df,col_to_impute='pickup_cluster_id',grouping_var='customer_id',var_type='categorical',method='mode')
    df['drop_cluster_id']=fill_missing_values(df=df,col_to_impute='drop_cluster_id',grouping_var='customer_id',var_type='categorical',method='mode')
    df['driver_rating']=fill_missing_values(df=df,col_to_impute='driver_rating',grouping_var='customer_id',var_type='continuous',method='median')
    df['discount'].fillna(0,inplace=True)
    
    return df
    
    
def get_ltv(df,month):
    
    print("Started executing get_ltv function call for {0}".format(month))
    
    temp=pd.DataFrame({'ltv' : df.groupby('customer_id')['travel_fare'].sum()}).reset_index()
    temp['month_start_date']= month
    temp['month_start_date']= pd.to_datetime(temp['month_start_date']).dt.date
    print("Completed executing get_ltv function call for {0}".format(month))
    
    return temp  
      
    
def get_clusters(df,month):
    
    print("Started executing get_clusters function call for {0}".format(month))
    
    temp=df.groupby('customer_id').agg(unique_business_count=('business_types_id', pd.Series.nunique), 
                                            freq_business_id=('business_types_id', lambda x: stats.mode(x)[0][0]),
                                            unique_pickup_cluster_count=('pickup_cluster_id', pd.Series.nunique), 
                                            freq_pickup_cluster_id=('pickup_cluster_id', lambda x: stats.mode(x)[0][0]),
                                            unique_drop_cluster_count=('drop_cluster_id', pd.Series.nunique), 
                                            freq_drop_cluster_id=('drop_cluster_id', lambda x: stats.mode(x)[0][0])
                                           ).reset_index()
    temp['month_start_date']= month
    temp['month_start_date']= pd.to_datetime(temp['month_start_date']).dt.date
    print("Completed executing get_clusters function call for {0}".format(month))
    return temp
    
    
def get_vehicles(df,month):
    
    print("Started executing get_vehicles function call for {0}".format(month))
    
    temp=df.groupby('customer_id').agg(unique_vehicles_count=('vehicle_id', pd.Series.nunique), 
                                            freq_vehicle_id=('vehicle_id', lambda x: stats.mode(x)[0][0])
                                           ).reset_index()
    temp['month_start_date']= month
    temp['month_start_date']= pd.to_datetime(temp['month_start_date']).dt.date
    print("Completed executing get_vehicles function call for {0}".format(month))
    
    return temp
        
    
def get_avg_ordering_days(df,month):
    
    print("Started executing get_avg_ordering_days function call for {0}".format(month))
    
    temp=pd.DataFrame({'avg_ordering_days' : df.groupby( [ 'customer_id'] )['days_gap_betw_orders'].median()}).reset_index()
    temp['month_start_date']= month
    temp['month_start_date']= pd.to_datetime(temp['month_start_date']).dt.date
    print("Completed executing get_avg_ordering_days function call for {0}".format(month))
    
    return temp
    
def get_amount_outstanding(df,month):
    
    print("Started executing get_amount_outstanding function call for {0}".format(month))
    
    customers_to_query = get_list(df,'customer_id')
    temp = fetch_data("get_amount_outstanding.txt", connection = "OMS",end_date=month,customers_to_query=customers_to_query)
    temp['month_start_date']= month
    temp['month_start_date']= pd.to_datetime(temp['month_start_date']).dt.date
    print("Completed executing get_amount_outstanding function call for {0}".format(month))
    
    return temp
    
def get_bad_rated_orders(df,month):
    
    print("Started executing get_bad_rated_orders function call for {0}".format(month))
    
    temp = df.sort_values(['customer_id','order_date']).groupby('customer_id')['driver_rating'].apply(lambda x : x.tail(5).between(1,3).sum()).reset_index()
    temp.columns=['customer_id','bad_rated_orders']
    temp['month_start_date']= month
    temp['month_start_date']= pd.to_datetime(temp['month_start_date']).dt.date
    print("Completed executing get_bad_rated_orders function call for {0}".format(month))
    
    return temp
    
def get_coupon_amount(df,month):
    
    print("Started executing get_coupon_amount function call for {0}".format(month))
    
    temp = df.sort_values(['customer_id','order_date']).groupby('customer_id')['discount'].apply(lambda x : x.tail(5).mean()).reset_index()
    temp.columns=['customer_id','avg_discount_amount']
    temp['month_start_date']= month
    temp['month_start_date']= pd.to_datetime(temp['month_start_date']).dt.date
    print("Completed executing get_coupon_amount function call for {0}".format(month))
    
    return temp

def get_porter_gold(df,month):
    
    print("Started executing get_porter_gold function call for {0}".format(month))
    
    customers_to_query = get_list(df,'customer_id')
    start_date = first_day_of_month(month,delta='-6')
    temp = fetch_data("get_porter_gold_sub.txt", connection = "OMS",start_date=start_date,end_date=month,customers_to_query=customers_to_query)
    temp['month_start_date']= month
    temp['month_start_date']= pd.to_datetime(temp['month_start_date']).dt.date
    print("Completed executing get_porter_gold function call for {0}".format(month))
    
    return temp
    
def get_wallet_transaction(df,month) :
    
    print("Started executing get_wallet_transaction function call for {0}".format(month))

    customers_to_query = get_list(df,'customer_id')
    
    orders_df= pd.DataFrame({'total_orders' : df.groupby( [ 'customer_id'] )['order_id'].count()}).reset_index()
    wallet_df = pd.DataFrame({'wallet_orders' : df[df.payment_type=='wallet_transaction'].groupby( [ 'customer_id'] )['order_id'].count()}).reset_index()
    
    temp = orders_df.merge(wallet_df,on='customer_id',how='left').fillna(0)
    temp['wallet_orders_prop'] = temp['wallet_orders']/temp['total_orders']
    temp['month_start_date'] = month
    temp['month_start_date'] = pd.to_datetime(temp['month_start_date']).dt.date
    temp = temp[['month_start_date','customer_id','wallet_orders_prop']]
    print("Completed executing get_wallet_transaction function call for {0}".format(month))
    
    return temp

    
def get_activity_index(df,month):
    
    print("Started executing get_activity_index function call for {0}".format(month))
    
    current_date = month
    current_date = datetime.datetime.strptime(month, '%Y-%m-%d').date()
    
    active_months_df = pd.DataFrame({'total_active_months' : df.groupby( [ 'customer_id'] )['month_start_date'].nunique()}).reset_index()
    first_order_df = pd.DataFrame({'first_order_date' : df.groupby( [ 'customer_id'] )['first_order_date'].min()}).reset_index()
    first_order_df['age_on_platform'] = (current_date - first_order_df['first_order_date'] ).dt.days//30
    first_order_df = first_order_df.merge(active_months_df,on='customer_id',how='left')
    first_order_df['activity_index'] = first_order_df['total_active_months'] / first_order_df['age_on_platform']
    
    temp = first_order_df[['customer_id','activity_index']]
    temp['month_start_date'] = month
    temp['month_start_date'] = pd.to_datetime(temp['month_start_date']).dt.date
    print("Completed executing get_activity_index function call for {0}".format(month))
    
    return temp
    
def get_order_density(df,month):
    
    print("Started executing get_order_density function call for {0}".format(month))
    
    active_months_df = pd.DataFrame({'total_active_months' : df.groupby( [ 'customer_id'] )['month_start_date'].nunique()}).reset_index()
    total_orders_df = pd.DataFrame({'total_orders' : df.groupby( [ 'customer_id'] )['order_id'].count()}).reset_index()
    total_orders_df = total_orders_df.merge(active_months_df,on='customer_id',how='left')
    total_orders_df['order_density'] = total_orders_df['total_orders']/ total_orders_df['total_active_months']
    
    temp = total_orders_df[['customer_id','order_density']]
    temp['month_start_date'] = month
    temp['month_start_date'] = pd.to_datetime(temp['month_start_date']).dt.date
    print("Completed executing get_order_density function call for {0}".format(month))
    
    return temp
    
    
def get_quality_months_prop(df,month) :
    
    print("Started executing get_quality_months_prop function call for {0}".format(month))
    
    active_months_df = pd.DataFrame({'total_active_months' : df.groupby( [ 'customer_id'] )['month_start_date'].nunique()}).reset_index()
    monthly_orders_df = pd.DataFrame({'total_orders' : df.groupby( [ 'customer_id','month_start_date'] )['order_id'].count()}).reset_index()
    quality_months= monthly_orders_df.groupby('customer_id')['total_orders'].apply(lambda x : (x>2).sum()).reset_index()
    quality_months.columns = ['customer_id','total_quality_months']
    active_months_df = active_months_df.merge(quality_months,on = 'customer_id', how = 'left')
    active_months_df['quality_months_prop'] =  active_months_df['total_quality_months'] / active_months_df['total_active_months']
    
    temp = active_months_df[['customer_id','quality_months_prop']]
    temp['month_start_date'] = month
    temp['month_start_date'] = pd.to_datetime(temp['month_start_date']).dt.date
    print("Completed executing get_quality_months_prop function call for {0}".format(month))
    
    return temp
    
def calculate_weighted_average(x):
    x=x.tolist()
    
    x = [np.nan] * (3 - len(x)) + x
    df=pd.DataFrame({'weights':[2,3,5],'orders':x}).fillna(0)
    return(sum(df['weights']* df['orders']))    
    
def get_order_score(df,month):
    
    print("Started executing get_order_score function call for {0}".format(month))
    
    monthly_orders_df = pd.DataFrame({'total_orders' : df.groupby( [ 'customer_id','month_start_date'] )['order_id'].count()}).reset_index()
    monthly_orders_df = monthly_orders_df.sort_values([ 'customer_id','month_start_date'])
    temp=monthly_orders_df.groupby('customer_id',as_index=True)['total_orders'].apply(lambda x : calculate_weighted_average(x.tail(3))).reset_index()
    temp.columns=['customer_id','order_score']
    
    temp['month_start_date'] = month
    temp['month_start_date'] = pd.to_datetime(temp['month_start_date']).dt.date
    print("Completed executing get_order_score function call for {0}".format(month))
    
    return temp
    

def get_last_month_sessions(df,month):
    
    print("Started executing get_last_month_sessions function call for {0}".format(month))
    
    customers_to_query = get_list(df,'customer_id')
    start_date = first_day_of_month(month,delta='-1')
    
    temp = fetch_data("get_last_month_sessions.txt", connection = "OMS",start_date=start_date,end_date=month,customers_to_query=customers_to_query)
    temp['month_start_date'] = month
    temp['month_start_date'] = pd.to_datetime(temp['month_start_date']).dt.date
    print("Completed executing get_last_month_sessions function call for {0}".format(month))
    
    return temp
    
        
    
   
    
def fetch_combined_results(df,month):
    
    #loom = ProcessLoom(max_runner_cap=4)
    loom = ThreadLoom(max_runner_cap=10)
    
    work = [(get_ltv , [df[['customer_id','travel_fare']],month],{},'ltv'), 
        (get_clusters ,[df[['customer_id','business_types_id','pickup_cluster_id','drop_cluster_id']],month],{},'clusters'), 
        (get_vehicles , [df[['customer_id', 'vehicle_id']],month],{},'vehicles') ,
        (get_avg_ordering_days , [df[['customer_id', 'days_gap_betw_orders']],month],{},'avg_ordering_days'), 
        (get_amount_outstanding , [df[['customer_id']],month],{},'amount_outstanding') ,
        (get_bad_rated_orders , [df[['customer_id', 'driver_rating','order_date']],month],{},'bad_rated_orders'),
        (get_coupon_amount , [df[['customer_id', 'discount','order_date']],month],{},'coupon_orders') ,
        (get_porter_gold , [df[['customer_id']],month],{},'porter_gold') ,
        (get_wallet_transaction , [df[['customer_id','order_id','payment_type']],month],{},'wallet_transaction') ,
        (get_activity_index , [df[['customer_id','first_order_date','month_start_date']],month],{},'activity_index') ,
        (get_order_density , [df[['customer_id','order_id','month_start_date']],month],{},'order_density') ,
        (get_quality_months_prop , [df[['customer_id','month_start_date','order_id']],month],{},'quality_months_prop') ,
        (get_order_score , [df[['customer_id','month_start_date','order_id']],month],{},'order_score') ,
        (get_last_month_sessions , [df[['customer_id']],month],{},'last_month_sessions') 
       ]
        
    loom.add_work(work)
        
    output = loom.execute()
        
    return output
    
    
    
        
    
    
    
    
    
    
    
        
        

        

    

