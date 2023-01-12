import os

from configparser import ConfigParser
import pandas as pd
from sqlalchemy import create_engine # New Redshift upload
import psycopg2 as db # Access Redshift
import pandas_redshift as pr # Upload to Redshift

# Get AWS keys
def get_keys():
    config = ConfigParser()
    config.read([os.path.join(os.path.expanduser("~"),'.aws/credentials')])
    access_key_id = config.get("default", 'aws_access_key_id')
    secret_access_key = config.get("default", 'aws_secret_access_key')
    return([access_key_id, secret_access_key])

# Get Redshift information
def get_redshift_creds():
    config = ConfigParser()
    config.read([os.path.join(os.path.expanduser("~"),'.aws/redshift')])
    info = [config.get("default", 'usr'),
            config.get("default", 'pwd'),
            config.get("default", 'host'),
            config.get("default", 'port'),
            config.get("default", 'dbname')]
    return(info)

# Open connections, cursors
def access_redshift():
    redshift_info = get_redshift_creds()
    con=db.connect(user=redshift_info[0], password=redshift_info[1],
                   host=redshift_info[2], port=redshift_info[3],
                   dbname = redshift_info[4])
    cur=con.cursor()
    return(con, cur)


def upload(data_to_add, table_name = 'crt_ds_etl.etl_unrolled', show = True):
    redshift_info = get_redshift_creds()
    aws_creds = get_keys()
    # Access Redshift
    pr.connect_to_redshift(user = redshift_info[0],
                           password = redshift_info[1],
                           host = redshift_info[2],
                           port = redshift_info[3],
                           dbname = redshift_info[4])
    # Access S3
    pr.connect_to_s3(
        aws_access_key_id=aws_creds[0],
        aws_secret_access_key=aws_creds[1],
        bucket = 'ri-etl-eucentral',
        subdirectory = 'Pandas_to_Redshift')
    pr.pandas_to_redshift(data_frame = data_to_add,
                          delimiter='\t',
                          redshift_table_name = table_name,
                          append = True)
    print("Upload complete.")


# Acquire data already available on redshift.
def get_available_data(table_name = 'crt_ds_etl.etl_unrolled', field = 'crt'):
    con, cur = access_redshift()
    print(cur)
    cur.execute("select count({field}), {field} \
                from {table_name} \
                group by {field} \
                having count({field}) > 1;".format(field=field,
                                                   table_name=table_name))
    data=cur.fetchall()
    available_data = pd.DataFrame(data)
    if field == 'crt':
        available_data.columns = ['Counts','Month']
        print("Months stored in RedShift:\n{}".format(available_data['Month'].values))
    else:
        available_data.columns = ['Counts',field]
        print("Months stored in RedShift:\n{}".format(available_data[field].values))
    return(available_data)

# Pulls X month's data
def get_month(crt_month, colnames,
              table_name = 'crt_ds_etl.etl_unrolled', show = False):
    cmd = "select * from {} where crt = {};".format(table_name, crt_month)
    if show: print(cmd)
    con, cur = access_redshift()
    cur.execute(cmd)
    data = cur.fetchall()
    month_df = pd.DataFrame(data)
    month_df.columns = colnames
    return(month_df)


def drop_month(month_to_drop,
               table_name = 'crt_ds_etl.etl_unrolled', show = True):
    cmd = "DELETE from {} where crt = {};".format(table_name, month_to_drop)
    if show: print(cmd)
    con, cur = access_redshift()
    cur.execute(cmd)
    con.commit()
