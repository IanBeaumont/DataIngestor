# -*- coding: utf-8 -*-
"""
Created on Sun Nov  6 17:00:29 2022

@author: Ian
"""
import pandas as pd
import datetime
import sqlite3
conn = sqlite3.connect("database.db")
c = conn.cursor()
    
    


def time():
    print("NOTE: Make sure the file you want to extract is in the root directory.")
    df = pd.read_csv(input("Enter your Filename.\n"))
    print("Extracting Data. This may take a moment.")
    date = []
    years = list(df["Year"])
    days = list(df["DayOfYear"])
    minutes = list(df["Minute"])
    
     #formatting Hour clean this up
    _ = list(df["Hour"])
    __ = [str(x) for x in _]
    ___= ([s[:-2] or '0' for s in __])
    hours = [int(x) for x in ___]
    ##
    

    #combine into datetime

    for x,z,h,m in zip(years, days, hours, minutes):
      y = (datetime.datetime(x, 1, 1, h, m) + datetime.timedelta(z - 1))
      
      date.append(str(y))
      
      
     #Clean up data and create a temp file. 
      
      clean_data = df.drop(columns=["Year", "DayOfYear","Hour","Minute"])
      clean_data["DateTime"] = pd.Series(date)
      clean_data.to_csv("temp1.dat", index=False)
          
def dupes():
    
    df = pd.read_csv("temp1.dat")
    
    
    
    valid = df.drop_duplicates()
    valid.to_csv("temp1.dat", index=False)

    invalid = df[df.duplicated(keep="first")]
    invalid.to_csv("temp2.dat", index=False)

def gaps():
    df = pd.read_csv("temp1.dat")
    df["DateTime"] = pd.to_datetime(df["DateTime"])
    df = df.set_index('DateTime').asfreq("1H", fill_value="Missing")
    
    df1 = pd.DataFrame([])
    try:
        grouped = df.groupby("AverageVoltage")
    
        df1 = grouped.get_group("Missing")        
    except Exception:
        
        pass
    
    df1.to_csv("temp3.dat")
    
    
    
def table_exists(table_name): 
    c.execute("""SELECT count(name) FROM sqlite_master WHERE TYPE = "table" AND name = "{}" """.format(table_name)) 
    if c.fetchone()[0] == 1: 
        return True 
    return False    
    
    
    
  
def tables():
    if not table_exists("validData"): 
        c.execute(''' 
            CREATE TABLE validData( 
                LoggerCode INTEGER, 
                SiteID INTEGER,
                AverageVoltage FLOAT,
                SensitID INTEGER,
                PulseCountSum INTEGER,
                DateTime STRING
            ) 
        ''')
    else:
        pass
        
        if not table_exists("duplicateTimeStamp"): 
            c.execute(''' 
                CREATE TABLE validData( 
                    LoggerCode INTEGER, 
                    SiteID INTEGER,
                    AverageVoltage FLOAT,
                    SensitID INTEGER,
                    PulseCountSum INTEGER,
                    DateTime STRING
                ) 
            ''')
        else:
            pass
            
            if not table_exists("timeSeriesGap"): 
                c.execute(''' 
                    LoggerCode INTEGER, 
                    SiteID INTEGER,
                    AverageVoltage FLOAT,
                    SensitID INTEGER,
                    PulseCountSum INTEGER,
                    DateTime STRING
                    ) 
                ''')
            else:
                pass
                
def insert():
    df = pd.read_csv("temp1.dat")
    df.to_sql("validData", conn, if_exists='append', index=False)
    conn.commit()   

    df1 = pd.read_csv("temp2.dat")
    df1.to_sql("duplicateTimeStamp", conn, if_exists='append', index=False)
    conn.commit() 

    df2 = pd.read_csv("temp3.dat")
    df2.to_sql("timeSeriesGap", conn, if_exists='append', index=False)
    conn.commit

                             

def main():
    
    
    
    time()
    dupes()
    gaps()

    print("Done.")
    tables()
    print("Inserting records...")
    insert()
    print("Done.")
    



main()




