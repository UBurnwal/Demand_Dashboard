from utilities.commonly_used_libraries import *
import datetime

def setup_connection(source):
    if(source == "Redshift"):
        conn = (psycopg2.connect(database="porter",
                             user = "anant",
                             password = "Resfeber123",
                             host = "commons-prod-redshift.porter.in",
                             port = "5439"))
    elif(source == "OMS"):
        conn = (psycopg2.connect(database="porter_order_production",
                             user = "kushal_k",
                             password = "SvwiDg7z1TmynO",
                             host = "oms-prod-psql-replica.porter.in",
                             port = "5432")
               )
        
    elif(source == "SFMS"):
        conn = (psycopg2.connect(database="sfms_production",
                             user = "postgres",
                             password = "ForceHasAwakened2515",
                             host = "sfms-prod.cg6jzsszjsaw.ap-southeast-1.rds.amazonaws.com",
                             port = "5432")
               )        
    else :
        raise Exception ("Enter the correct source")
    
    return conn

def get_sqlalchemy_engine():
    engine_query = (f"""postgresql+psycopg2://anant:Resfeber123@commons-prod-redshift.porter.in:5439/porter""")
    engine = create_engine(engine_query, echo = False)
    return engine

def write_to_redshift(table_name,  dataframe,  engine):
    dataframe['created_at'] = str(datetime.datetime.now())
    dataframe.to_sql(table_name,  engine,  if_exists = 'replace',  chunksize = 1000,  index = False)

def write_to_redshift_append(table_name,  dataframe,  engine):
    print("in append")
    dataframe['created_at'] = str(datetime.datetime.now())
    dataframe.to_sql(table_name,  engine,  if_exists = 'append',  chunksize = 1000,  index = False)



def get_connections(connection):
    return setup_connection(connection)

redshift_engine = get_sqlalchemy_engine()