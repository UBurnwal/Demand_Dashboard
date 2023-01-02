#!/usr/bin/env python
# coding: utf-8

# In[1]:


#!/usr/bin/env python
# coding: utf-8

# In[1]:


import xlrd as xld
import xlsxwriter
## To create Mini Dataframes & Select Relevant Columns only AS WE Don't Need all Columns | Present Metrics & Last Month metrics Separate
##& Need to Drop gep_region_coumns 

def select_columns(data_frame, column_names):
    new_frame = data_frame.loc[:, column_names]
    return new_frame


def movecol(df, cols_to_move=[], ref_col='', place='After'):
    
    cols = df.columns.tolist()
    if place == 'After':
        seg1 = cols[:list(cols).index(ref_col) + 1]
        seg2 = cols_to_move
    if place == 'Before':
        seg1 = cols[:list(cols).index(ref_col)]
        seg2 = cols_to_move + [ref_col]
    
    seg1 = [i for i in seg1 if i not in seg2]
    seg3 = [i for i in cols if i not in seg1 + seg2]
    
    return(df[seg1 + seg2 + seg3])    

## To Rename First Retention Metric Columns & Pivot
##p.s : We need to drop the geo_region INDEX column & TRANSPOSE

def retention_first_metric(df):
    df=df.drop(['geo_region_id'], axis = 1)
    df.rename(columns={'customers_retained': 'Retained Customers','less_than_two_order_customers' :'Customers <=2 Orders',
                      'greater_than_two_order_customers' : 'Customers >=2 Orders'}, inplace=True)
    df.set_index('month', inplace=True)
    df=df.T
    return(df)

##2nd Set
def retention_second_metric(df):
    df=df.drop(['geo_region_id'], axis = 1)
    df.rename(columns={'total_orders': 'Orders by Retained Customers','orders_less_than_2':'Order <=2',
                      'orders_greater_than_2' : 'Order >=3'}, inplace=True)
    df.set_index('month', inplace=True)
    df=df.T
    return(df)
    
##Last Month set
def retention_last_month_metric(df):
    df=df.drop(['geo_region_id'], axis = 1)
    df.rename(columns={'m1_customers_retained': 'Retained Customers Next Month','m1_less_than_two_order_customers':'Retained Customers Order <=2',
                      'm1_greater_than_two_order_customers' : 'Retained Customers Order >=3','m1_retention_history' : 'Customer Retained','zero_order_customer': '0 Order Customer'}, inplace=True)
    df.set_index('month', inplace=True)
    df=df.T
    return(df)


## Acquisition Metric Helper Functions

def acquisition_first_metric(df):
    df=df.drop(['geo_region_id'], axis = 1)
    df.rename(columns={'leads': 'New_Leads','customers': 'Customer','customer_less_2' :'Customers <=2',
                      'customer_greater_3' : 'Customers >=3','orders': 'Order','orders_less_2':'Order <=2',
                      'orders_greater_3':'Order >=3'}, inplace=True)
    df.set_index('month', inplace=True)
    df=df.T
    return(df)

def acquisition_second_metric(df):
    df=df.drop(['geo_region_id'], axis = 1)
    df.rename(columns={'customers_m_1': 'Customer','customer_less_2_m_1' :'Customer <=2',
                      'customer_greater_3_m_1' : 'Customers >=3','customers_m_2': 'Customer Retained','orders_m_1' :'Orders_m1','orders_less_2_m_1':'Order <=2_m1',
                      'orders_greater_3_m_1':'Order >=3_m1'}, inplace=True)
    df.set_index('month', inplace=True)
    df=df.T
    return(df)

def acquisition_third_metric(df):
    df=df.drop(['geo_region_id'], axis = 1)
    df.rename(columns={'% Retention':'Retention %','% Customer>=3/Active' : 'ActiveCustomer>=3 %'})
    df.set_index('month', inplace=True)
    df=df.T
    return(df)


##Reactivation Metric Helper Function

def reactivation_first_metric(df):
    df=df.drop(['geo_region_id'], axis = 1)
    df.rename(columns={'acc_customers': 'Accumulated_Base','acc_g30_l60' :'30-60_Day_Base',
                      'acc_g60_l90' : '60-90_Day Base','acc_g90_l120': '90-120_Day_Base','acc_g120':'120+_Day_Base'},inplace=True)   
    df.set_index('month', inplace=True)
    df=df.T
    return(df)


def reactivation_customer_metric(df):
    df=df.drop(['geo_region_id'], axis = 1)
    df.rename(columns={'reactivated_customers':'Customer ','reactivated_l2':'Customer <=2','reactivated_g3':'Customer >=3',
                      'orders':'Orders','orders_l2':'Order <=2','orders_g3':'Order >=3'
                      }, inplace=True)
    df.set_index('month', inplace=True)
    df=df.T
    return(df)

def reactivation_days_metric(df):
    df=df.drop(['geo_region_id'], axis = 1)
    df.rename(columns={'reactivated_g30_l60':'30-60 Day Reactivation','reactivated_g60_l90':'60-90 Day Reactivation','reactivated_g90_l120':'90-120 Day Reactivation',
                      'reactivated_g120':'120+ Day Reactivation'},inplace = True)
    df.set_index('month',inplace = True)
    df=df.T
    return(df)

def reactivation_second_metric(df):
    df=df.drop(['geo_region_id'], axis = 1)
    df.rename(columns={'reactivated_customers_m1': 'Reactivated Customers Next Month','reactivated_l2_m1' :'Reactivated Customer <=2',
                      'reactivated_g3_m1' : 'Reactivated Customer >=3','reactivated_customers_m2': 'Customer Retained'}, inplace=True)
    df.set_index('month', inplace=True)
    df=df.T
    return(df)


def reactivation_third_metric(df):
    df=df.drop(['geo_region_id'], axis = 1)
    df.rename(columns={'orders_m1': 'Reactivated Customers Next Month_Order','orders_l2_m1' :'Reactivated Customer Next Month_Order <=2',
                      'orders_g3_m1' : 'Reactivated Customer Next Month_Order >=3','reactivated_g30_l60_m1': 'Customer Reactivated 30-60 Days',
                      'reactivated_g60_l90_m1':'Customer Reactivated 60-90 Days','reactivated_g90_l120_m1':'Customer Reactivated 90-120 Days'}, inplace=True)
    df.set_index('month', inplace=True)
    df=df.T
    return(df)


def generate_excel_workbook():
    """ Creates an excel sheet that has necessary information """
    geo_regions = [1,2,3,4,5,6]
    data_frame_dicts = {}
    for geo_region in geo_regions:
        dataframe_array = []
        for table in df_dict[str(geo_region)]:
            write_to_excel = df_dict[str(geo_region)][table].reset_index()
            #write_to_excel['metric'] = table
            #write_to_excel.rename(columns = col_dictionary, inplace = False)
            dataframe_array.append(write_to_excel)
        sheet_name = "geo_region_id_" + str(geo_region)
        data_frame_dicts[sheet_name] = dataframe_array
    run_time = (str(datetime.datetime.now()).split(" ")[1].split(".")[0]).replace(":","_")
    sheet_name = "./demand_report_workbook" + ".xlsx" 
    writer = pd.ExcelWriter(sheet_name,engine ='xlsxwriter') 
    workbook  = writer.book
    bold = workbook.add_format({'bold': True})
    for geo_region, data in data_frame_dicts.items():
        print(data)
        col = 1
        row = 1
        for df in data :
            print("hello")
            add_col = len(df.columns)
            add_row = len(df.index)
            df.to_excel(writer, sheet_name = str(geo_region), 
                 startrow = row, startcol = col, index = False)
            worksheet = writer.sheets[geo_region]
            header_format = workbook.add_format({
                          'bold': True,
                          'text_wrap': True,
                          'valign': 'top',
                          'fg_color': '#003366',
                          'font_color': 'white',
                          'border': 1,
                          'num_format': 'mmm-yy' })
            #formater = workbook.add_format({'border':2}) ## Tried to add 
            #worksheet.set_column(0, len(df.columns),5, formater)
            for f, b in zip(df.columns.values, list(range(col, col + add_col))):
                worksheet.write(row, b , f, header_format)
            row = row + add_row + 4
    writer.save()

def format_workbook_raw(wb):
  for sheets in wb :
    ws1=wb['geo_region_id_0'].title='India'
    ws1=wb['geo_region_id_1'].title='Mumbai'
    ws1=wb['geo_region_id_2'].title='Delhi'
    ws1=wb['geo_region_id_3'].title='Bangalore'
    ws1=wb['geo_region_id_4'].title='Chennai'
    ws1=wb['geo_region_id_5'].title='Hyderabad'
    ws1=wb['geo_region_id_6'].title='Ahmedabad'
    wb['India'].column_dimensions['B'].width = 36
    wb['Mumbai'].column_dimensions['B'].width = 36
    wb['Delhi'].column_dimensions['B'].width = 36
    wb['Bangalore'].column_dimensions['B'].width = 36
    wb['Chennai'].column_dimensions['B'].width = 36
    wb['Hyderabad'].column_dimensions['B'].width = 36
    wb['Ahmedabad'].column_dimensions['B'].width = 36
    ws1=wb['India'].freeze_panes = 'C3'
    ws2=wb['Mumbai'].freeze_panes = 'C3'
    ws3=wb['Delhi'].freeze_panes = 'C3'
    ws4=wb['Bangalore'].freeze_panes = 'C3'
    ws5=wb['Chennai'].freeze_panes = 'C3'
    ws6=wb['Hyderabad'].freeze_panes = 'C3'
    ws7=wb['Ahmedabad'].freeze_panes = 'C3'
    wb['India'].sheet_properties.tabColor = "00FF6600"
    wb['Mumbai'].sheet_properties.tabColor = "00FFFFFF"
    wb['Delhi'].sheet_properties.tabColor = "0000FF00"
    wb['Bangalore'].sheet_properties.tabColor = "00FF6600"
    wb['Chennai'].sheet_properties.tabColor = "00FFFFFF"
    wb['Hyderabad'].sheet_properties.tabColor = "0000FF00"
    wb['Ahmedabad'].sheet_properties.tabColor = "00FF6600"
    wb=wb.save('demand_report_workbook.xlsx')
  return(wb)
