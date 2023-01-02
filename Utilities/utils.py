from utilities.commonly_used_libraries import *
from utilities.connections import *
import time
import math
from scipy import stats
from dateutil.relativedelta import relativedelta


def get_list(df,column_name):
    
    temp=df[column_name].unique()
    lst=','.join([str(i) for i in temp])
    return lst
    
def datetime_conversion(df,cols):
    """ Converts String Date to Datetime Objects """
    for col in cols:
        df[col]=pd.to_datetime(df[col]).dt.date
        
    return df    


def first_day_of_month(month,delta):
    
     
    """
    Format of Month is "YYYY-MM-dd"
    
    """
    sign = delta[0]
    num_of_months = int(delta[1:])
    month= pd.to_datetime(month)
    
    if sign == '-':
        transformed_date = month - relativedelta(months=num_of_months, day=1)
        
    elif sign == '+' : 
        transformed_date = month + relativedelta(months=num_of_months, day=1)
        
    return str(transformed_date).split()[0]
    
def fill_missing_values(df,col_to_impute,grouping_var,var_type,method):
    
    """ This function is used to fill NA values for a DF column (travel_fare) w.r.t a grouping variable (customer_id) . 
    Methods to impute can be 'mean' / 'median' / 'mode' depending on variable type - 'continuous' / 'categorical' """
    
    if (var_type=='continuous') & (method=='median'):
        grouped_df=pd.DataFrame(df.groupby(grouping_var)[col_to_impute].median())
        
    elif (var_type=='continuous') & (method=='mean'):
        grouped_df=pd.DataFrame(df.groupby(grouping_var)[col_to_impute].mean())
        
    elif (var_type=='categorical') & (method=='mode'):
        grouped_df=pd.DataFrame(df.groupby(grouping_var)[col_to_impute].agg(lambda x: stats.mode(x)[0][0]))
        
    else :
        print('Incompatible Variable Type and Method')
        return
        
    temp=df.set_index(grouping_var)[[col_to_impute]].fillna(grouped_df).set_index(df.index)
    
    return temp
            
            

def get_latest_date(df, column_name, type):
    max_date = max(df[column_name])
    date = max_date
    if type == 'day':
        modified_date = date + timedelta(days=1)
    elif type == 'month':
        modified_date = date + relativedelta(months=+1)
    return str(modified_date)

def read_sql_file(path):
    fd = open(path, 'r')
    sqlFile = fd.read()
    fd.close()
    return sqlFile

def fetch_data(filename, connection, **kwargs):
	query = read_sql_file('./queries/{}'.format(filename))
	no_result = True
	while no_result:
		try:
			conn = get_connections(connection)
			data = pd.read_sql_query(query.format(**kwargs), conn)
			no_result = False
		except Exception as e:
			print(e)
			conn.close()
			print("-------------- Now Retrying --------------")
			time.sleep(100)
			pass	
	return data


# Porter Data Pack Utils
def filter_based_on_rejection(row):
    date_test = row['month_start'].date()
    leeway_date = datetime.date(2017, 1,1)
    if date_test < leeway_date : 
        return 1
    elif row['percent_rejection'] <= 10.0:
        return 1
    else:
        return 0

def get_partner_earnings(row):
    total_trip_fare = row['total_travel_fare']
    commission_to_porter = row['total_commission']
    incentives_given_to_partner = row['incentives']
    net_partner_earning = total_trip_fare - commission_to_porter + incentives_given_to_partner
    return net_partner_earning

def get_bucket_number(value, bucket_dict):
    if value > bucket_dict['90%']:
        return 'B1'
    elif value > bucket_dict['80%']:
        return 'B2'
    elif value > bucket_dict['70%']:
        return 'B3'
    elif value > bucket_dict['60%']:
        return 'B4'
    elif value > bucket_dict['50%']:
        return 'B5'
    elif value > bucket_dict['40%']:
        return 'B6'
    elif value > bucket_dict['30%']:
        return 'B7'
    elif value > bucket_dict['20%']:
        return 'B8'
    elif value > bucket_dict['10%']:
        return 'B9'
    else:
        return 'B10'
    
def get_top_quantile(value, bucket_dict):
    if value > bucket_dict['75%']:
        return 'Q4'
    elif value > bucket_dict['50%']:
        return 'Q3'
    elif value > bucket_dict['25%']:
        return 'Q2'
    else:
        return 'Q1'


def fix_driver_id(row):
    if not (math.isnan(float(row['driver_id']))):
        return int(float(row['driver_id']))
    else:
        return None

def get_bucket(row, earnings_decile_dict, revenue_decile_dict):
    key = str(row['month_start']) + "##" + str(row['geo_region_id'])
    deciles = earnings_decile_dict[key]
    return get_bucket_number(row['net_partner_earning'], deciles)

def get_bucket_revenue(row, earnings_decile_dict, revenue_decile_dict):
    key = str(row['month_start']) + "##" + str(row['geo_region_id'])
    deciles = revenue_decile_dict[key]
    return get_bucket_number(row['total_travel_fare'], deciles)

def get_top_quartile(row, earnings_decile_dict, revenue_decile_dict):
    key = str(row['month_start']) + "##" + str(row['geo_region_id'])
    deciles = revenue_decile_dict[key]
    return get_top_quantile(row['total_travel_fare'], deciles)


# Partner Decile Data Utils
def fix_decile(row):
    if row['decile'] == 'B1':
        return 1
    if row['decile'] == 'B2':
        return 2
    if row['decile'] == 'B3':
        return 3
    if row['decile'] == 'B4':
        return 4
    if row['decile'] == 'B5':
        return 5
    if row['decile'] == 'B6':
        return 6
    if row['decile'] == 'B7':
        return 7
    if row['decile'] == 'B8':
        return 8
    if row['decile'] == 'B9':
        return 9
    if row['decile'] == 'B10':
        return 10



# Mailer Utilities
def export_csv(df):
    with io.StringIO() as buffer:
        df.to_csv(buffer, index = False)
        return buffer.getvalue()

def send_dataframes(subject, body, needed_dataframes):
    context = ssl.create_default_context()
    multipart = MIMEMultipart()
    multipart['From'] = sender_email
    multipart['To'] = receiver_email
    multipart['Subject'] = subject
    for filename in needed_dataframes:
        attachment = MIMEApplication(export_csv(needed_dataframes[filename]))
        attachment['Content-Disposition'] = 'attachment; filename="{}"'.format(filename)
        multipart.attach(attachment)
    multipart.attach(MIMEText(body, 'html'))
    s = smtplib.SMTP_SSL(smtp_server, port, context=context)
    s.login(sender_email, password)
    s.sendmail(sender_email, receiver_email, multipart.as_string())
    s.quit()

