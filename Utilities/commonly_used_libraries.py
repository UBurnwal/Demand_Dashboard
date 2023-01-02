#Standard Libraries
from functools import reduce
import io
import os
from itertools import tee
from functools import reduce
import statistics
from scipy import stats
import datetime
from datetime_truncate import truncate
from dateutil.relativedelta import *
from datetime import date , datetime


#Third-Party Libraries
from datetime import timedelta
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import numpy as np
import pandas as pd
from pandas.io.sql import SQLTable
def _execute_insert(self, conn, keys, data_iter):
    print("Using monkey-patched _execute_insert")
    data = [dict(zip(keys, row)) for row in data_iter]
    conn.execute(self.table.insert().values(data))
SQLTable._execute_insert = _execute_insert
import pandas.io.sql as sqlio
import psycopg2
from pandasql import sqldf
from sqlalchemy import create_engine, event
import smtplib, ssl
import xlsxwriter
#from pexecute.process import ProcessLoom
#from pexecute.thread import ThreadLoom
