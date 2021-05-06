import pandas as pd
import numpy as np
import glob
import os
import sqlalchemy
from sqlalchemy_utils import database_exists, create_database
import shutil
import logging
import schedule
import time

logging.basicConfig(filename='logger.log', level=logging.DEBUG,
                    format='%(asctime)s:%(levelname)s:%(message)s')



def main_process():

    path = 'D:/Gazprom/sample_data/files_list'
    files = glob.glob(path + "\*.SMRT")

    destination = 'D:/Gazprom/sample_data/archive'
    archive = os.listdir(destination)

    li = []
    merged = pd.DataFrame()
    for file in files:
        filename = file.replace(path, '')
        filename = filename.replace('\\', '')

        # Check if same file has been already loaded :
        if filename in archive:
            os.remove(file)
        else:
            df = pd.read_csv(file, index_col=None, header=None, engine='python')

            # Check for HEADR and  TRAIL record in given file.
            if 'HEADR' in df.values and 'TRAIL' in df.values:
                shutil.move(file, destination)

                df0 = df.loc[df[0] != 'TRAIL']
                df_rec = df0[5]
                file_no = df_rec[0]
                df1 = df0.fillna(file_no)
                df2 = df1.iloc[0:]
                li.append(df2)
                merged = pd.concat(li)

    # handle recieving the same datetime for a given meter by overwriting the original value with the new one
    if merged.empty:
        df_header = pd.DataFrame()
        df_consu1 = pd.DataFrame()

    else:
        df_consu = merged.loc[merged[0] == 'CONSU']
        del df_consu[0]
        df_consu.columns = ['meter_number', 'mearsurement_date', 'measurement_time', 'consumption', 'file_number']

        df_consu1 = df_consu.drop_duplicates(
            subset=['meter_number', 'mearsurement_date', 'measurement_time', 'file_number'], keep='last').reset_index(
            drop=True)

        df_consu1 = df_consu1.astype(str)
        df_consu1['measurement_time'] = df_consu1['measurement_time'].apply(lambda x: x.split(".")[0])

        df_header = merged.loc[merged[0] == 'HEADR']
        del df_header[0]
        df_header.columns = ['file_type_identifier', 'company_id ', 'file_creation_date', 'file_creation_time ',
                             'file_number']
        df_header = df_header.astype(str)
        df_header['file_creation_date'] = df_header['file_creation_date'].apply(lambda x: x.split(".")[0])

    # returning header and consu data separately
    return (df_header, df_consu1)


# Function to upload records in two table header_tab and consu_tab under reading_db database in PostgreSQL

def upload_db(df_header, df_consu1):

    try:
        engine = sqlalchemy.create_engine('postgresql://postgres:**********@localhost/reading_db1')
        if not database_exists(engine.url):
            create_database(engine.url)

        con = engine.connect()

        df_consu1.to_sql('consu_tab2',
                         con=engine,
                         if_exists='append',
                         index=False,
                         dtype={'meter_number': sqlalchemy.types.Text(),
                                'mearsurement_date': sqlalchemy.types.Date(),
                                'measurement_time': sqlalchemy.types.Integer(),
                                'consumption': sqlalchemy.types.Float(precision=2, asdecimal=True),
                                'file_number': sqlalchemy.types.VARCHAR()})

        df_header.to_sql('header_tab2',
                         con=engine,
                         if_exists='append',
                         index=False,
                         dtype={'file_type_identifier': sqlalchemy.types.Text(),
                                'company_id': sqlalchemy.types.Text(),
                                'file_creation_date': sqlalchemy.types.Date(),
                                'file_creation_time': sqlalchemy.types.Float(precision=0, asdecimal=True),
                                'file_number': sqlalchemy.types.VARCHAR()})

        con.close()

    except Exception as e:
        con.close()
        logging.exception(str(e))

def auto_script():

    print('kamal')
    df_header, df_consu1 = main_process()

    logging.debug("Records to be uploaded in header table: {}".format(df_header))
    logging.debug("Records to be uploaded in consu table: {}".format(df_consu1))

    upload_db(df_header, df_consu1)


# Running Python script daily.

schedule.every(10).seconds.do(auto_script)

while True:
    schedule.run_pending()
    time.sleep(1)
