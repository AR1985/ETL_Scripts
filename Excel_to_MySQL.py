#!python3
"""Extract data from an excel file, perform any necessary transforms, and load into database"""

import pandas as pd
from sqlalchemy import create_engine, text
import pymysql

#df1 = dataframe to hold raw data
#df2 = dataframe to hold transformed data

#Configure global variables
filename = r'[PUT FILENAME OF EXCEL DOCUMENT HERE]'
db_column_names = '[PUT DATABASE COLUMN NAMES HERE]'
db_name = '[PUT DATABASE NAME HERE]'
table_name = '[PUT DATABASE TABLE NAME HERE]'

pwd = input('Please enter db password: ')
engine = create_engine(f'mysql+pymysql://root:{pwd}@localhost:3306/{db_name}')
                       

def extract():
    """Extract raw data from excel file into dataframe df1"""
    global df1
    global filename
    df1 = pd.read_excel(filename)


def transform():
    """Perform any necessary transformations on the data, to create dataframe df2"""
    global df2
    
    #Create a dictionary of the cross reference between existing column headers in dataframe, and column names in database, in order to rename columns in dataframe
    db_column_xref = dict(zip(df1.columns, db_column_names))

    #Transform by creating new dataframe, renaming column names to match column names in database, using column cross reference
    df2 = df1.rename(columns=db_column_xref, errors="raise")

    #Remove any newline characters from the part_serial# column
    df2['part_serial#'] = df2['part_serial#'].str.strip()


def load():
    """Load the transformed dataframe into the database"""
    df2.to_sql(name=table_name, con=engine, index=False, if_exists='append')
    

def validate():
    """Function to run before and after rest of script.  Put anything to validate before or after the script runs here, e.g. number of rows in database"""
    with engine.connect() as con:
        response = con.execute(f'SELECT COUNT(*) FROM {table_name}')

        for row in response:
            print(row)


def main():
    """Main function"""

    print('Rows before: ', validate())
    
    extract()
    print('Extract... Complete')
    
    transform()
    print('Transform... Complete')
    
    load()
    print('Load... Complete')

    print('Rows after: ', validate())
    
    print(df2)



if __name__ == '__main__':
    main()
