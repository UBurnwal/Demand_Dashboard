#!/usr/bin/env python
# coding: utf-8

# In[1]:


from utils import select_columns
from utils import movecol

def select_columns(data_frame, column_names):
    new_frame = data_frame.loc[:, column_names]
    return new_frame

def get_lag_columns(acquistion):
    acquistion['customers_lag'] = (acquistion.sort_values(by=['geo_region_id','month'], ascending=True)
                       .groupby(['geo_region_id'])['customers'].shift(1))

    return(acquistion)

def get_lag_columns_ret(retention):
    retention['customers_retained_lag'] = (retention.sort_values(by=['geo_region_id','month'], ascending=True)
                       .groupby(['geo_region_id'])['customers_retained'].shift(1))

    return(retention)

def get_lag_columns_react(reactivation):
    reactivation['orders_lag'] = (reactivation.sort_values(by=['geo_region_id','month'], ascending=True)
                       .groupby(['geo_region_id'])['orders'].shift(1))
    reactivation['reactivated_customers_lag'] = (reactivation.sort_values(by=['geo_region_id','month'], ascending=True)
                       .groupby(['geo_region_id'])['reactivated_customers'].shift(1))
    return(reactivation)


def get_data_subset_react(selected_columns,reactivation):
    temp_df = select_columns(reactivation, selected_columns)
    geo_dict_r = {}
    for i, g in temp_df.groupby('geo_region_id'):
        geo_dict_r[i] = g        
    return geo_dict_r

def get_data_subset(selected_columns,retention):
    temp_df = select_columns(retention, selected_columns)
    geo_dict_r = {}
    for i, g in temp_df.groupby('geo_region_id'):
        geo_dict_r[i] = g
        
    return geo_dict_r

def get_data_subset_acq(selected_columns,acquisition):
    temp_df = select_columns(acquisition, selected_columns)
    geo_dict_r = {}
    for i, g in temp_df.groupby('geo_region_id'):
        geo_dict_r[i] = g
        
    return geo_dict_r


def get_retention_data(retention):
    retention['order_per_customer']=retention['total_orders']/retention['customers_retained']
    retention['zero_order_customer'] = retention['customers_retained_lag']-retention['m1_customers_retained']
    retention['M-1_order_per_customer']=retention['m1_total_orders']/retention['m1_customers_retained']
    retention['%_Retention']=(retention['m1_customers_retained']/retention['customers_retained_lag'])*100.0
    retention['order_per_customer']=retention['order_per_customer'].apply(lambda x:round(x,2))
    retention['M-1_order_per_customer']=retention['M-1_order_per_customer'].apply(lambda x:round(x,2))
    retention['%_Retention']=retention['%_Retention'].apply(lambda x:round(x,2))
    return retention


def get_pan_india_totals_react(df,col_list):
    a=len(df)
    tot=df.head(a)    
    unique_geo_regions = [0]+tot['geo_region_id'].unique().tolist()
    pan_india_totals= tot.reset_index().groupby(['month'])[col_list].sum()
    pan_india_totals['geo_region_id'] = 0
    pan_india_totals=pan_india_totals.reset_index()    
    return(pan_india_totals)


def get_reactivation_data(reactivation):
    #reactivation.sort_values(by=['geo_region_id','month'], inplace=True)
    reactivation['Orders/Customer']=(reactivation['orders']/reactivation['reactivated_customers'])
    reactivation['%Customer>=3/Total Reactivated']=(reactivation['reactivated_g3']/reactivation['reactivated_customers'])*100.0
    reactivation['percentage reactivation']=(reactivation['reactivated_customers']/reactivation['acc_customers'])*100.0
    #reactivation['zero_order_customer'] =reactivation['orders'].shift(1)
    reactivation['zero_order_customer'] =reactivation['reactivated_customers_lag']-reactivation['reactivated_customers_m1']
    reactivation['% Retention']=(reactivation['reactivated_customers_m1']/reactivation['reactivated_customers_lag'])*100.0
    reactivation['Orders/Customer_m1']=reactivation['orders_m1']/reactivation['reactivated_customers_m1']
    reactivation['Orders/Customer']=reactivation['Orders/Customer'].apply(lambda x:round(x,2))
    reactivation['%Customer>=3/Total Reactivated']=reactivation['%Customer>=3/Total Reactivated'].apply(lambda x:round(x,2))
    reactivation['percentage reactivation']=reactivation['percentage reactivation'].apply(lambda x:round(x,2))
    reactivation['% Retention']=reactivation['% Retention'].apply(lambda x:round(x,2))
    reactivation['Orders/Customer_m1']=reactivation['Orders/Customer_m1'].apply(lambda x:round(x,2))
       
    return reactivation

def get_acquistion_data(acquistion):
    acquistion['Conversion %']=(acquistion['customers']/acquistion['leads'])*100.0
    acquistion['order_per_customer']=acquistion['orders']/acquistion['customers']
    acquistion['Order/Customer (>=3)']=acquistion['orders_greater_3']/acquistion['customer_greater_3']
    acquistion['Zero order cust']=acquistion['customers_lag']-acquistion['customers_m_1']
    acquistion['Orders/Customer_m1']=acquistion['orders_m_1']/acquistion['customers_m_1']
    acquistion['% Retention']=(acquistion['customers_m_1']/acquistion['customers_lag'])*100.0
    acquistion['% Customer>=3/Active']=acquistion['customer_greater_3_m_1']/acquistion['customers_m_1']*100.0
    acquistion['Conversion %']=acquistion['Conversion %'].apply(lambda x:round(x,2))
    acquistion['order_per_customer']=acquistion['order_per_customer'].apply(lambda x:round(x,2))
    acquistion['Order/Customer (>=3)']=acquistion['Order/Customer (>=3)'].apply(lambda x:round(x,2))
    acquistion['Orders/Customer_m1']=acquistion['Orders/Customer_m1'].apply(lambda x:round(x,2))
    acquistion['% Retention']=acquistion['% Retention'].apply(lambda x:round(x,2))
    acquistion['% Customer>=3/Active']=acquistion['% Customer>=3/Active'].apply(lambda x:round(x,2))

    return acquistion