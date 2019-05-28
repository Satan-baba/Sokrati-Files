import pandas as pd
import os
import numpy as np
from datetime import datetime, timedelta
from stat import S_ISREG, ST_CTIME, ST_MODE
import os, sys, time

def read_last_file():
    dir_path = sys.argv[1] if len(sys.argv) == 2 else r'.'
    data = (os.path.join(dir_path, fn) for fn in os.listdir(dir_path))
    data = ((os.stat(path), path) for path in data)

    data = ((stat[ST_CTIME], path)
           for stat, path in data if S_ISREG(stat[ST_MODE]))
    name= []
    for cdate, path in sorted(data):
        name.append(os.path.basename(path))
    return name

Plac_csv_file_DBM_Reeng = read_last_file()[-1]
Plac_csv_file_DBM = read_last_file()[-2]
Plac_csv_file_af_IOS = read_last_file()[-3]
Plac_csv_file_af_reeng = read_last_file()[-4]
Plac_csv_file_af_android = read_last_file()[-5]
df_DBM_Reeng = pd.read_csv(Plac_csv_file_DBM_Reeng)
df_DBM = pd.read_csv(Plac_csv_file_DBM)
df_af_IOS = pd.read_csv(Plac_csv_file_af_IOS)
df_af_android = pd.read_csv(Plac_csv_file_af_android)
df_af_Reeng = pd.read_csv(Plac_csv_file_af_reeng)
df_DBM_Reeng = df_DBM_Reeng[:-13]
del df_DBM['Advertiser Currency']
del df_DBM_Reeng["Advertiser Currency"]
df_af_android_use = df_af_android[["Date","Campaign (c)","Installs","new_user_registration (Unique users)","appsflyer_new_order_created (Unique users)"]]
df_af_IOS_use = df_af_IOS[["Date","Campaign (c)","Installs","new_user_registration (Unique users)","appsflyer_new_order_created (Unique users)"]]
df_af_Reeng_use = df_af_Reeng[["Date","Campaign (c)","new_user_registration (Unique users)","appsflyer_new_order_created (Unique users)"]]
df_af_IOS_use.columns = ["Date","CM Placement ID", "Installs", "Registrations", "Activations" ] 
df_af_android_use.columns = ["Date","CM Placement ID", "Installs", "Registrations", "Activations" ] 
df_af_Reeng_use.columns = ["Date","CM Placement ID", "Registrations", "Activations" ]
df_DBM = df_DBM[:-14]
df_DBM["CM Placement ID"] = df_DBM["CM Placement ID"].astype(int)
df_DBM["Date"] = pd.to_datetime(df_DBM["Date"])
df_af_android_use["Date"] = pd.to_datetime(df_af_android_use["Date"])
df_af_IOS_use["Date"] = pd.to_datetime(df_af_IOS_use["Date"])
df_af_Reeng_use["Date"] = pd.to_datetime(df_af_Reeng_use["Date"])
df_DBM["CM Placement ID"] = df_DBM["CM Placement ID"].apply(str) 
new_df_android  = pd.merge(df_DBM, df_af_android_use, left_on = ['CM Placement ID', 'Date'], right_on = ['CM Placement ID', 'Date'])
new_df_android = new_df_android.groupby(["Date"]).sum()
new_df_android["CTR"] = (new_df_android["Clicks"]/new_df_android["Impressions"])*100
new_df_android["CPC"] = new_df_android["Revenue (Adv Currency)"]/new_df_android["Clicks"]
new_df_android["CPI"] = new_df_android["Revenue (Adv Currency)"]/new_df_android["Installs"]
new_df_android["CPR"] = new_df_android["Revenue (Adv Currency)"]/new_df_android["Registrations"]
new_df_android["CPA"] = new_df_android["Revenue (Adv Currency)"]/new_df_android["Activations"]
new_df_android["CPI"] = new_df_android.CPI.astype(int)
new_df_android["CPR"] = new_df_android.CPR.astype(int)
new_df_android["CPA"] = new_df_android.CPA.astype(int)
new_df_android["CPC"] = new_df_android.CPC.astype(int)
new_df_android["CTR"] = new_df_android.CTR.round(2)
new_df_android["Revenue (Adv Currency)"] = new_df_android["Revenue (Adv Currency)"].astype(int)
df_af_IOS_use["CM Placement ID"] = df_af_IOS_use["CM Placement ID"].apply(str)
new_df_IOS  = pd.merge(df_DBM, df_af_IOS_use, left_on = ['CM Placement ID', 'Date'], right_on = ['CM Placement ID', 'Date'])
new_df_IOS = new_df_IOS.groupby(["Date"]).sum()
new_df_IOS["CTR"] = (new_df_IOS["Clicks"]/new_df_IOS["Impressions"])*100
new_df_IOS["CPC"] = new_df_IOS["Revenue (Adv Currency)"]/new_df_IOS["Clicks"]
new_df_IOS["CPI"] = new_df_IOS["Revenue (Adv Currency)"]/new_df_IOS["Installs"]
new_df_IOS["CPR"] = new_df_IOS["Revenue (Adv Currency)"]/new_df_IOS["Registrations"]
new_df_IOS["CPA"] = new_df_IOS["Revenue (Adv Currency)"]/new_df_IOS["Activations"]
new_df_IOS["CPI"] = new_df_IOS.CPI.astype(int)
new_df_IOS["CPR"] = new_df_IOS.CPR.astype(int)
new_df_IOS["CPA"] = new_df_IOS.CPA.astype(int)
new_df_IOS["CPC"] = new_df_IOS.CPC.astype(int)
new_df_IOS["CTR"] = new_df_IOS.CTR.round(2)
new_df_IOS["Revenue (Adv Currency)"] = new_df_IOS["Revenue (Adv Currency)"].astype(int)
df_af_Reeng_use["CM Placement ID"] = df_af_Reeng_use["CM Placement ID"].apply(str)
df_DBM_Reeng["Date"] = pd.to_datetime(df_DBM_Reeng["Date"])
new_df_Reeng  = pd.merge(df_DBM_Reeng, df_af_Reeng_use, left_on = ['CM Placement ID', 'Date'], right_on = ['CM Placement ID', 'Date'])
new_df_Reeng = new_df_Reeng.groupby(["Date"]).sum()
new_df_Reeng["Revenue (Adv Currency)"] = new_df_Reeng["Revenue (Adv Currency)"]/2
new_df_Reeng["CTR"] = (new_df_Reeng["Clicks"]/new_df_Reeng["Impressions"])*100
new_df_Reeng["CPC"] = new_df_Reeng["Revenue (Adv Currency)"]/new_df_Reeng["Clicks"]
new_df_Reeng["CPR"] = new_df_Reeng["Revenue (Adv Currency)"]/new_df_Reeng["Registrations"]
new_df_Reeng["CPA"] = new_df_Reeng["Revenue (Adv Currency)"]/new_df_Reeng["Activations"]
new_df_Reeng["CPR"] = new_df_Reeng.CPR.astype(int)
new_df_Reeng["CPA"] = new_df_Reeng.CPA.astype(int)
new_df_Reeng["CPC"] = new_df_Reeng.CPC.astype(int)
new_df_Reeng["CTR"] = new_df_Reeng.CTR.round(2)
new_df_Reeng["Revenue (Adv Currency)"] = new_df_Reeng["Revenue (Adv Currency)"].astype(int)
df_Total = pd.DataFrame()
df_Total["Impressions"] = new_df_android["Impressions"] + new_df_IOS["Impressions"] + new_df_Reeng["Impressions"]
df_Total["Clicks"] = new_df_android["Clicks"] + new_df_IOS["Clicks"] + new_df_Reeng["Clicks"]
df_Total["Revenue (Adv Currency)"] = new_df_android["Revenue (Adv Currency)"] + new_df_IOS["Revenue (Adv Currency)"] + new_df_Reeng["Revenue (Adv Currency)"]
df_Total["Installs"] = new_df_android["Installs"] + new_df_IOS["Installs"]
df_Total["Registrations"] = new_df_android["Registrations"] + new_df_IOS["Registrations"] + new_df_Reeng["Registrations"]
df_Total["Activations"] = new_df_android["Activations"] + new_df_IOS["Activations"] + new_df_Reeng["Activations"]
df_Total["CTR"] = (df_Total["Clicks"]/df_Total["Impressions"])*100
df_Total["CPC"] = df_Total["Revenue (Adv Currency)"]/df_Total["Clicks"]
df_Total["CPI"] = df_Total["Revenue (Adv Currency)"]/df_Total["Installs"]
df_Total["CPR"] = df_Total["Revenue (Adv Currency)"]/df_Total["Registrations"]
df_Total["CPA"] = df_Total["Revenue (Adv Currency)"]/df_Total["Activations"]
df_Total["CPI"] = df_Total.CPI.astype(int)
df_Total["CPR"] = df_Total.CPR.astype(int)
df_Total["CPA"] = df_Total.CPA.astype(int)
df_Total["CPC"] = df_Total.CPC.astype(int)
df_Total["CTR"] = df_Total.CTR.round(2)
df_col_android = pd.merge(df_DBM, df_af_android_use, left_on = ['CM Placement ID', 'Date'], right_on = ['CM Placement ID', 'Date'])
df_col_android["Insertion Order"] = df_col_android["Insertion Order"].map({"App Install_Bangalore_Android":"Bangalore", 
                                                               "App Install_Chennai_Android":"Chennai",
                                                               "App Install_Delhi_Android":"Delhi",
                                                              "App Install_Gurgaon_Android": "Gurgaon",
                                                              "App Install_Hyderabad_Android":"Hyderabad",
                                                              "App Install_Pune_Android":"Pune",
                                                              })
df_col_android = df_col_android.dropna()
df_col_android = df_col_android.groupby(["Date", "Insertion Order"]).sum()
del df_col_android["Line Item ID"]
df_col_android["CTR"] = (df_col_android["Clicks"]/df_col_android["Impressions"])*100
df_col_android["CPC"] = df_col_android["Revenue (Adv Currency)"]/df_col_android["Clicks"]
df_col_android["CPI"] = df_col_android["Revenue (Adv Currency)"]/df_col_android["Installs"]
df_col_android["CPR"] = df_col_android["Revenue (Adv Currency)"]/df_col_android["Registrations"]
df_col_android["CPA"] = df_col_android["Revenue (Adv Currency)"]/df_col_android["Activations"]
df_col_android["CPI"] = df_col_android.CPI.astype(int)
df_col_android["CPR"] = df_col_android.CPR.astype(int)
df_col_android["CPA"] = df_col_android.CPA.astype(int)
df_col_android["CPC"] = df_col_android.CPC.astype(int)
df_col_android["CTR"] = df_col_android.CTR.round(2)

df_col_IOS = pd.merge(df_DBM, df_af_IOS_use, left_on = ['CM Placement ID', 'Date'], right_on = ['CM Placement ID', 'Date'])
df_col_IOS["Insertion Order"] = df_col_IOS["Insertion Order"].map({"App Install_Bangalore_iOS":"Bangalore", 
                                                              "App Install_Gurgaon_iOS": "Gurgaon",
                                                              "App Install_Pune_iOS":"Pune",
                                                              })
df_col_IOS = df_col_IOS.dropna()
df_col_IOS = df_col_IOS.groupby(["Date","Insertion Order"]).sum()
del df_col_IOS["Line Item ID"]
df_col_IOS["CTR"] = (df_col_IOS["Clicks"]/df_col_IOS["Impressions"])*100
df_col_IOS["CPC"] = df_col_IOS["Revenue (Adv Currency)"]/df_col_IOS["Clicks"]
df_col_IOS["CPI"] = df_col_IOS["Revenue (Adv Currency)"]/df_col_IOS["Installs"]
df_col_IOS["CPR"] = df_col_IOS["Revenue (Adv Currency)"]/df_col_IOS["Registrations"]
df_col_IOS["CPA"] = df_col_IOS["Revenue (Adv Currency)"]/df_col_IOS["Activations"]
df_col_IOS["CTR"] = df_col_IOS.CTR.round(2)
df_col_Reeng = pd.merge(df_DBM_Reeng, df_af_Reeng_use, left_on = ['CM Placement ID', 'Date'], right_on = ['CM Placement ID', 'Date'])
df_col_Reeng["Insertion Order"] = df_col_Reeng["Insertion Order"].map({"New user_Bangalore_7days":"Bangalore", 
                                                              "New user_Gurgaon_7days": "Gurgaon",
                                                              "New user_Pune_7days":"Pune",
                                                              })
df_col_Reeng = df_col_Reeng.groupby(['Date','Insertion Order']).sum()
df_col_Reeng.replace(np.inf,0,inplace = True)

df_col_Reeng["CTR"] = (df_col_Reeng["Clicks"]/df_col_Reeng["Impressions"])*100
df_col_Reeng["CPC"] = df_col_Reeng["Revenue (Adv Currency)"]/df_col_Reeng["Clicks"]
df_col_Reeng["CPR"] = df_col_Reeng["Revenue (Adv Currency)"]/df_col_Reeng["Registrations"]
df_col_Reeng["CPA"] = df_col_Reeng["Revenue (Adv Currency)"]/df_col_Reeng["Activations"]

yesterday = datetime.now() - timedelta(days=1)
yesterday = yesterday.strftime('%Y-%m-%d')
cities = ["Bangalore", "Pune", "Gurgaon"]
cities_ser = pd.Series(cities)
impressions = []
impressions = pd.Series([df_col_Reeng.loc[yesterday, city]['Impressions'] + df_col_android.loc[yesterday, city]["Impressions"] + df_col_IOS.loc[yesterday, city]["Impressions"] for city in cities])
clicks = pd.Series([df_col_Reeng.loc[yesterday, city]['Clicks'] + df_col_android.loc[yesterday, city]["Clicks"] + df_col_IOS.loc[yesterday, city]["Clicks"] for city in cities])
Revenue = pd.Series([df_col_Reeng.loc[yesterday, city]['Revenue (Adv Currency)'] + df_col_android.loc[yesterday, city]["Revenue (Adv Currency)"] + df_col_IOS.loc[yesterday, city]["Revenue (Adv Currency)"] for city in cities])
Installs = pd.Series([df_col_android.loc[yesterday, city]["Installs"] + df_col_IOS.loc[yesterday, city]["Installs"]for city in cities])
registrations = pd.Series([df_col_Reeng.loc[yesterday, city]['Registrations'] + df_col_android.loc[yesterday, city]["Registrations"] + df_col_IOS.loc[yesterday, city]["Registrations"] for city in cities])
activations = pd.Series([df_col_Reeng.loc[yesterday, city]['Activations'] + df_col_android.loc[yesterday, city]["Activations"] + df_col_IOS.loc[yesterday, city]["Activations"] for city in cities])
city_total = pd.DataFrame()
city_total["Impressions"] = impressions
city_total["Clicks"] = clicks
city_total["CTR"] = ((city_total["Clicks"]/city_total["Impressions"])*100).round(2)
city_total["Revenue"] = Revenue.round(0)
city_total["CPC"] = (city_total["Revenue"]/city_total["Clicks"]).round(0)
city_total["Installs"] = Installs
city_total["CPI"] = (city_total["Revenue"]/city_total["Installs"]).round(0)
city_total["Registrations"] = registrations
city_total["CPR"] = (city_total["Revenue"]/city_total["Registrations"]).round(0)
city_total["Activations"] = activations
city_total["CPA"] = (city_total["Revenue"]/city_total["Activations"]).round(0)
city_total["City"] = cities_ser
city_total["CPM"] = ((city_total["Revenue"]/city_total["Impressions"])*1000).round(0)
city_total = city_total.set_index(["City"])
city_total["Activation Rate"] = ((city_total["Activations"]/city_total["Registrations"])*100).round(0)
city_total["Click to Install"] = ((city_total["Installs"]/city_total["Clicks"])*100).round(2)


download_name = read_last_file()[-1].split('_')[0]
today = datetime.today()
d3 = today.strftime("%m-%d-%y")
d3 = d3[:5]
name = download_name + "_" + "Tracker_" + d3 + ".xlsx"
writer = pd.ExcelWriter(name)
new_df_android.to_excel(writer, 'Android')
new_df_IOS.to_excel(writer, 'IOS')
new_df_Reeng.to_excel(writer, 'Reengagement')
df_col_android.to_excel(writer, 'Android_City')
df_col_IOS.to_excel(writer, 'IOS_City')
df_col_Reeng.to_excel(writer, 'Reengagement_City')
df_Total.to_excel(writer, "Total tracking")
city_total.to_excel(writer, "City Total")
writer.save()
