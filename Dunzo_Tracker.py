import pandas as pd
import os
import numpy as np
from datetime import datetime, timedelta
from stat import S_ISREG, ST_CTIME, ST_MODE
import os, sys, time
import pyttsx3
import gc
import gspread
import pygsheets



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


yesterday = datetime.now() - timedelta(days=1)
yesterday = yesterday.strftime('%Y-%m-%d')
Plac_csv_file_DBM = read_last_file()[-1]
Plac_csv_file_af_IOS = read_last_file()[-2]
Plac_csv_file_af_reeng = read_last_file()[-3]
Plac_csv_file_af_android = read_last_file()[-4]
df_DBM = pd.read_csv(Plac_csv_file_DBM)
df_af_IOS = pd.read_csv(Plac_csv_file_af_IOS)
df_af_android = pd.read_csv(Plac_csv_file_af_android)
df_af_Reeng = pd.read_csv(Plac_csv_file_af_reeng)
del df_DBM['Advertiser Currency']



df_af_android_use = df_af_android[["Date","Campaign (c)","Installs","new_user_registration (Unique users)","appsflyer_new_order_created (Unique users)"]]
df_af_IOS_use = df_af_IOS[["Date","Campaign (c)","Installs","new_user_registration (Unique users)","appsflyer_new_order_created (Unique users)"]]
df_af_Reeng_use = df_af_Reeng[["Date","Campaign (c)","new_user_registration (Unique users)","appsflyer_new_order_created (Unique users)"]]
df_af_IOS_use.columns = ["Date","CM Placement ID", "Installs", "Registrations", "Activations" ] 
df_af_android_use.columns = ["Date","CM Placement ID", "Installs", "Registrations", "Activations" ] 
df_af_Reeng_use.columns = ["Date","CM Placement ID", "Registrations", "Activations" ]
df_af_Reeng_use["CM Placement ID"] = df_af_Reeng_use["CM Placement ID"].astype(str)
df_DBM = df_DBM.dropna()
df_DBM["CM Placement ID"] = df_DBM["CM Placement ID"].astype(int)
df_DBM["Date"] = pd.to_datetime(df_DBM["Date"])
df_af_android_use["Date"] = pd.to_datetime(df_af_android_use["Date"])
df_af_IOS_use["Date"] = pd.to_datetime(df_af_IOS_use["Date"])
df_af_Reeng_use["Date"] = pd.to_datetime(df_af_Reeng_use["Date"])
df_DBM["CM Placement ID"] = df_DBM["CM Placement ID"].apply(str) 
df_DBM_android = df_DBM[df_DBM["Insertion Order"].str.contains("Android") == True]
df_DBM_IOS = df_DBM[df_DBM["Insertion Order"].str.contains("iOS") == True]
df_DBM_Reeng1 = df_DBM[df_DBM["Insertion Order"].str.contains('New user') == True]
df_af_android_use["CM Placement ID"] = df_af_android_use["CM Placement ID"].astype(str)

def new_df_android():
    new_df_android = pd.merge(df_DBM_android, df_af_android_use, left_on = ['CM Placement ID', 'Date'], right_on = ['CM Placement ID', 'Date']
                         , how = 'outer')
    new_df_android = new_df_android.groupby(["Date"]).sum()
    new_df_android["Impressions"] = df_DBM_android.groupby(["Date"]).sum()['Impressions']
    new_df_android["Clicks"] = df_DBM_android.groupby(["Date"]).sum()['Clicks']
    new_df_android["CTR"] = (new_df_android["Clicks"]/new_df_android["Impressions"])*100
    new_df_android["Revenue (Adv Currency)"] = df_DBM_android.groupby(["Date"]).sum()['Revenue (Adv Currency)']
    new_df_android["CPC"] = new_df_android["Revenue (Adv Currency)"]/new_df_android["Clicks"]
    new_df_android["CPI"] = new_df_android["Revenue (Adv Currency)"]/new_df_android["Installs"]
    new_df_android["CPR"] = new_df_android["Revenue (Adv Currency)"]/new_df_android["Registrations"]
    new_df_android["CPA"] = new_df_android["Revenue (Adv Currency)"]/new_df_android["Activations"]
    new_df_android["CPI"] = new_df_android.CPI.round(0)
    new_df_android["CPR"] = new_df_android.CPR.round(0)
    new_df_android["CPA"] = new_df_android.CPA.round(0)
    new_df_android["CPC"] = new_df_android.CPC.round(0)
    new_df_android["CTR"] = new_df_android.CTR.round(2)
    new_df_android["Revenue (Adv Currency)"] = new_df_android["Revenue (Adv Currency)"].round(0)
    return new_df_android

def new_df_IOS():
    df_af_IOS_use["CM Placement ID"] = df_af_IOS_use["CM Placement ID"].apply(str)
    new_df_IOS = pd.merge(df_DBM_IOS, df_af_IOS_use, left_on = ["CM Placement ID", "Date"], right_on = ["CM Placement ID", "Date"]
                         , how = 'outer')
    new_df_IOS = new_df_IOS.groupby(["Date"]).sum()
    new_df_IOS["Impressions"] = df_DBM_IOS.groupby(["Date"]).sum()['Impressions']
    new_df_IOS["Clicks"] = df_DBM_IOS.groupby(["Date"]).sum()['Clicks']
    new_df_IOS["Revenue (Adv Currency)"] = df_DBM_IOS.groupby(["Date"]).sum()['Revenue (Adv Currency)']
    new_df_IOS["CTR"] = (new_df_IOS["Clicks"]/new_df_IOS["Impressions"])*100
    new_df_IOS["CPC"] = new_df_IOS["Revenue (Adv Currency)"]/new_df_IOS["Clicks"]
    new_df_IOS["CPI"] = new_df_IOS["Revenue (Adv Currency)"]/new_df_IOS["Installs"]
    new_df_IOS["CPR"] = new_df_IOS["Revenue (Adv Currency)"]/new_df_IOS["Registrations"]
    new_df_IOS["CPA"] = new_df_IOS["Revenue (Adv Currency)"]/new_df_IOS["Activations"]
    new_df_IOS["CPI"] = new_df_IOS.CPI.round(0)
    new_df_IOS["CPR"] = new_df_IOS.CPR.round(0)
    new_df_IOS["CPA"] = new_df_IOS.CPA.round(0)
    new_df_IOS["CPC"] = new_df_IOS.CPC.round(0)
    new_df_IOS["CTR"] = new_df_IOS.CTR.round(2)
    new_df_IOS["Revenue (Adv Currency)"] = new_df_IOS["Revenue (Adv Currency)"].astype(int)
    return new_df_IOS

def new_df_Reeng():
    df_af_Reeng_use["CM Placement ID"] = df_af_Reeng_use["CM Placement ID"].apply(str)
    df_af_Reeng_use["CM Placement ID"] = df_af_Reeng_use["CM Placement ID"].apply(str)
    new_df_Reeng = pd.merge(df_DBM_Reeng1, df_af_Reeng_use, left_on = ["CM Placement ID", "Date"], right_on = ["CM Placement ID", "Date"]
                         , how = 'outer')
    new_df_Reeng = new_df_Reeng.dropna()
    new_df_Reeng = new_df_Reeng.groupby(['Date']).sum()
    new_df_Reeng["Impressions"] = df_DBM_Reeng1.groupby(["Date"]).sum()['Impressions']
    new_df_Reeng["Clicks"] = df_DBM_Reeng1.groupby(["Date"]).sum()['Clicks']
    new_df_Reeng["Revenue (Adv Currency)"] = df_DBM_Reeng1.groupby(["Date"]).sum()['Revenue (Adv Currency)']
    new_df_Reeng["CTR"] = (new_df_Reeng["Clicks"]/new_df_Reeng["Impressions"])*100
    new_df_Reeng["CPC"] = new_df_Reeng["Revenue (Adv Currency)"]/new_df_Reeng["Clicks"]
    new_df_Reeng["CPR"] = new_df_Reeng["Revenue (Adv Currency)"]/new_df_Reeng["Registrations"]
    new_df_Reeng["CPA"] = new_df_Reeng["Revenue (Adv Currency)"]/new_df_Reeng["Activations"]
    new_df_Reeng["CPR"] = new_df_Reeng.CPR.round(0)
    new_df_Reeng["CPA"] = new_df_Reeng.CPA.round(0)
    new_df_Reeng["CPC"] = new_df_Reeng.CPC.round(0)
    new_df_Reeng["CTR"] = new_df_Reeng.CTR.round(2)
    new_df_Reeng["Revenue (Adv Currency)"] = new_df_Reeng["Revenue (Adv Currency)"].astype(int)
    return new_df_Reeng

def df_Total():
    df_Total = pd.DataFrame()
    df_Total["Impressions"] = df_DBM.groupby(["Date"]).sum()["Impressions"]
    df_Total["Clicks"] = df_DBM.groupby(["Date"]).sum()["Clicks"]
    df_Total["Revenue (Adv Currency)"] = df_DBM.groupby(["Date"]).sum()['Revenue (Adv Currency)']
    df_Total["Installs"] = new_df_android()["Installs"] + new_df_IOS()["Installs"]
    df_Total["Registrations"] = new_df_android()["Registrations"] + new_df_IOS()["Registrations"] 
    df_Total["Activations"] = new_df_android()["Activations"] + new_df_IOS()["Activations"] 
    df_Total["CTR"] = (df_Total["Clicks"]/df_Total["Impressions"])*100
    df_Total["CPC"] = df_Total["Revenue (Adv Currency)"]/df_Total["Clicks"]
    df_Total["CPI"] = df_Total["Revenue (Adv Currency)"]/df_Total["Installs"]
    df_Total["CPR"] = df_Total["Revenue (Adv Currency)"]/df_Total["Registrations"]
    df_Total["CPA"] = df_Total["Revenue (Adv Currency)"]/df_Total["Activations"]
    df_Total["CPI"] = df_Total.CPI.round(0)
    df_Total["CPR"] = df_Total.CPR.round(0)
    df_Total["CPA"] = df_Total.CPA.round(0)
    df_Total["CPC"] = df_Total.CPC.round(0)
    df_Total["CTR"] = df_Total.CTR.round(2)
    return df_Total




def df_col_android():
    df_col_android = pd.merge(df_DBM_android, df_af_android_use, left_on = ['CM Placement ID', 'Date'], right_on = ['CM Placement ID', 'Date'], how = 'left')
    df_col_android = df_col_android.groupby(["Date", "Insertion Order"]).sum()
    del df_col_android["Line Item ID"]
    df_col_android["CTR"] = (df_col_android["Clicks"]/df_col_android["Impressions"])*100
    df_col_android["CPC"] = df_col_android["Revenue (Adv Currency)"]/df_col_android["Clicks"]
    df_col_android["CPI"] = df_col_android["Revenue (Adv Currency)"]/df_col_android["Installs"]
    df_col_android["CPR"] = df_col_android["Revenue (Adv Currency)"]/df_col_android["Registrations"]
    df_col_android["CPA"] = df_col_android["Revenue (Adv Currency)"]/df_col_android["Activations"]
    df_col_android["CPI"] = df_col_android.CPI.round(0)
    df_col_android["CPR"] = df_col_android.CPR.round(0)
    df_col_android["CPA"] = df_col_android.CPA.round(0)
    df_col_android["CPC"] = df_col_android.CPC.round(0)
    df_col_android["CTR"] = df_col_android.CTR.round(2)
    return df_col_android

def df_col_IOS():
    
    df_col_IOS = pd.merge(df_DBM_IOS, df_af_IOS_use, left_on = ['CM Placement ID', 'Date'], right_on = ['CM Placement ID', 'Date']
                         , how = 'outer')
    df_col_IOS = df_col_IOS.groupby(["Date","Insertion Order"]).sum()
    del df_col_IOS["Line Item ID"]
    df_col_IOS["CTR"] = (df_col_IOS["Clicks"]/df_col_IOS["Impressions"])*100
    df_col_IOS["CPC"] = df_col_IOS["Revenue (Adv Currency)"]/df_col_IOS["Clicks"]
    df_col_IOS["CPI"] = df_col_IOS["Revenue (Adv Currency)"]/df_col_IOS["Installs"]
    df_col_IOS["CPR"] = df_col_IOS["Revenue (Adv Currency)"]/df_col_IOS["Registrations"]
    df_col_IOS["CPA"] = df_col_IOS["Revenue (Adv Currency)"]/df_col_IOS["Activations"]
    df_col_IOS["CTR"] = df_col_IOS.CTR.round(2)
    return df_col_IOS

def df_col_Reeng():
    df_col_Reeng = pd.merge(df_DBM_Reeng1, df_af_Reeng_use, left_on = ['CM Placement ID', 'Date'], right_on = ['CM Placement ID', 'Date'], how = 'left')

    df_col_Reeng = df_col_Reeng.groupby(['Date','Insertion Order']).sum()
    

    df_col_Reeng["CTR"] = (df_col_Reeng["Clicks"]/df_col_Reeng["Impressions"])*100
    df_col_Reeng["CPC"] = df_col_Reeng["Revenue (Adv Currency)"]/df_col_Reeng["Clicks"]
    df_col_Reeng["CPR"] = df_col_Reeng["Revenue (Adv Currency)"]/df_col_Reeng["Registrations"]
    df_col_Reeng["CPA"] = df_col_Reeng["Revenue (Adv Currency)"]/df_col_Reeng["Activations"]
    return df_col_Reeng


def city_total():
    ftotal_android = pd.merge(df_DBM_android, df_af_android_use, left_on = ['CM Placement ID', 'Date'], right_on = ['CM Placement ID', 'Date']
                         , how = 'outer')
    ftotal_IOS = pd.merge(df_DBM_IOS, df_af_IOS_use, left_on = ["CM Placement ID", "Date"], right_on = ["CM Placement ID", "Date"]
                         , how = 'outer')
    ftotal_Reeng = pd.merge(df_DBM_Reeng1, df_af_Reeng_use, left_on = ["CM Placement ID", "Date"], right_on = ["CM Placement ID", "Date"]
                         , how = 'outer')

    df_total_af = pd.concat([ftotal_android, ftotal_IOS, ftotal_Reeng])
    df_total_af = df_total_af.groupby(["Date", "Insertion Order"]).sum()

    ftotal_android = pd.merge(df_DBM_android, df_af_android_use, left_on = ['CM Placement ID', 'Date'], right_on = ['CM Placement ID', 'Date']
                         , how = 'outer')
    ftotal_IOS = pd.merge(df_DBM_IOS, df_af_IOS_use, left_on = ["CM Placement ID", "Date"], right_on = ["CM Placement ID", "Date"]
                         , how = 'outer')
    ftotal_Reeng = pd.merge(df_DBM_Reeng1, df_af_Reeng_use, left_on = ["CM Placement ID", "Date"], right_on = ["CM Placement ID", "Date"]
                         , how = 'outer')

    df_total_af = pd.concat([ftotal_android, ftotal_IOS, ftotal_Reeng])
    df_total_af = df_total_af.groupby(["Date", "Insertion Order"]).sum()

    city_total = pd.DataFrame(index = df_total_af.index)
    city_total["Impressions"] = df_total_af['Impressions']
    city_total["Clicks"] = df_total_af['Clicks']
    city_total["CTR"] = ((city_total["Clicks"]/city_total["Impressions"])*100).round(2)
    city_total["Revenue"] = df_total_af['Revenue (Adv Currency)'].round(0)
    city_total["CPC"] = (city_total["Revenue"]/city_total["Clicks"]).round(0)
    city_total["Installs"] = df_total_af["Installs"]
    city_total["CPI"] = (city_total["Revenue"]/city_total["Installs"]).round(0)
    city_total["Registrations"] = df_total_af["Registrations"]
    city_total["CPR"] = (city_total["Revenue"]/city_total["Registrations"]).round(0)
    city_total["Activations"] = df_total_af["Activations"]
    city_total["CPA"] = (city_total["Revenue"]/city_total["Activations"]).round(0)
    city_total["CPM"] = ((city_total["Revenue"]/city_total["Impressions"])*1000).round(0)
    city_total["Activation Rate"] = ((city_total["Activations"]/city_total["Registrations"])*100).round(0)
    city_total["Click to Install"] = ((city_total["Installs"]/city_total["Clicks"])*100).round(2)
    return city_total

def df_indiranagar():
    lis_indira = []
    for i in range(len(df_DBM["Creative"].str.contains("Indiranagar"))):
        if df_DBM["Creative"].str.contains("Indiranagar")[i] == True:
            lis_indira.append(i)

    df_DBM_indira = df_DBM.loc[lis_indira]
    df_indiranagar = pd.merge(df_DBM_indira, df_af_android_use, left_on = ['CM Placement ID', 'Date'], right_on = ['CM Placement ID', 'Date'])
    df_indiranagar = df_indiranagar.groupby(["Date", "Line Item", "Creative"]).sum()
    df_indiranagar["CTR"] = (df_indiranagar["Clicks"]/df_indiranagar["Impressions"])*100
    df_indiranagar["CPM"] = ((df_indiranagar["Revenue (Adv Currency)"]/df_indiranagar["Impressions"])*100).round(1)
    df_indiranagar["CPC"] = (df_indiranagar["Revenue (Adv Currency)"]/df_indiranagar["Clicks"]).round(0)
    df_indiranagar["CPI"] = (df_indiranagar["Revenue (Adv Currency)"]/df_indiranagar["Installs"]).round(0)
    df_indiranagar["CPR"] = (df_indiranagar["Revenue (Adv Currency)"]/df_indiranagar["Registrations"]).round(0)
    df_indiranagar["CPA"] = (df_indiranagar["Revenue (Adv Currency)"]/df_indiranagar["Activations"]).round(0)
    df_indiranagar["CTR"] = df_indiranagar.CTR.round(2) 
    return df_indiranagar

def ind():
    df_ind_total = df_indiranagar().reset_index()
    df_ind_tot_yes = df_ind_total[df_ind_total["Date"] == yesterday]
    cpm = ((df_ind_tot_yes["Revenue (Adv Currency)"].sum()/df_ind_tot_yes["Impressions"].sum())*1000).round(1)
    ctr = ((df_ind_tot_yes["Clicks"].sum()/df_ind_tot_yes["Impressions"].sum())*100).round(2)
    cpi = (df_ind_tot_yes["Revenue (Adv Currency)"].sum()/df_ind_tot_yes["Installs"].sum()).round(0)
    cpr = (df_ind_tot_yes["Revenue (Adv Currency)"].sum()/df_ind_tot_yes["Registrations"].sum()).round(0)
    cpa = (df_ind_tot_yes["Revenue (Adv Currency)"].sum()/df_ind_tot_yes["Activations"].sum()).round(0)
    act_rate = ((df_ind_tot_yes["Activations"].sum()/df_ind_tot_yes["Registrations"].sum())*100).round(0)
    row2 = [df_ind_tot_yes["Impressions"].sum().round(0), df_ind_tot_yes["Revenue (Adv Currency)"].sum().round(0),cpm,df_ind_tot_yes["Clicks"].sum(),
           ctr,(df_ind_tot_yes["Revenue (Adv Currency)"].sum()/df_ind_tot_yes["Clicks"].sum()).round(2),df_ind_tot_yes.Installs.sum(),
          cpi, df_ind_tot_yes["Registrations"].sum(),cpr,df_ind_tot_yes["Activations"].sum(),cpa,act_rate]

    ind_cols = ["Impressions", "Spends","CPM", "Clicks","CTR","CPC","Installs","CPI","Registrations","CPR","Activations","CPA","Activation Rate"]
    ind = pd.DataFrame(columns = ["Impressions", "Spends","CPM", "Clicks","CTR","CPC","Installs","CPI","Registrations","CPR","Activations","CPA","Activation Rate"], data = [row2])
    return ind

def creative_android():
    creative_android = pd.merge(df_DBM_android, df_af_android_use, left_on = ["CM Placement ID", "Date"], right_on = ["CM Placement ID", "Date"]
             ,how = 'outer').groupby(['Insertion Order', 'Line Item', "Creative"]).sum()
    creative_android["CTR"] = (creative_android["Clicks"]/creative_android["Impressions"])*100
    creative_android["CPC"] = creative_android["Revenue (Adv Currency)"]/creative_android["Clicks"]
    creative_android["CPI"] = creative_android["Revenue (Adv Currency)"]/creative_android["Installs"]
    creative_android["CPR"] = creative_android["Revenue (Adv Currency)"]/creative_android["Registrations"]
    creative_android["CPA"] = creative_android["Revenue (Adv Currency)"]/creative_android["Activations"]
    creative_android["CPI"] = creative_android.CPI.round(0)
    creative_android["CPR"] = creative_android.CPR.round(0)
    creative_android["CPA"] = creative_android.CPA.round(0)
    creative_android["CPC"] = creative_android.CPC.round(0)
    creative_android["CTR"] = creative_android.CTR.round(2)
    creative_android["Revenue (Adv Currency)"] = creative_android['Revenue (Adv Currency)'].round(0)
    return creative_android

def creative_IOS():
    creative_IOS = pd.merge(df_DBM_IOS, df_af_IOS_use, left_on = ["CM Placement ID", "Date"], right_on = ["CM Placement ID", "Date"]
             ,how = 'outer').groupby(['Insertion Order', 'Line Item', "Creative"]).sum()

    creative_IOS["CTR"] = (creative_IOS["Clicks"]/creative_IOS["Impressions"])*100
    creative_IOS["CPC"] = creative_IOS["Revenue (Adv Currency)"]/creative_IOS["Clicks"]
    creative_IOS["CPI"] = creative_IOS["Revenue (Adv Currency)"]/creative_IOS["Installs"]
    creative_IOS["CPR"] = creative_IOS["Revenue (Adv Currency)"]/creative_IOS["Registrations"]
    creative_IOS["CPA"] = creative_IOS["Revenue (Adv Currency)"]/creative_IOS["Activations"]
    creative_IOS["CPI"] = creative_IOS.CPI.round(0)
    creative_IOS["CPR"] = creative_IOS.CPR.round(0)
    creative_IOS["CPA"] = creative_IOS.CPA.round(0)
    creative_IOS["CPC"] = creative_IOS.CPC.round(0)
    creative_IOS["CTR"] = creative_IOS.CTR.round(2)
    creative_IOS["Revenue (Adv Currency)"] = creative_IOS['Revenue (Adv Currency)'].round(0)
    return creative_IOS

def creative_Reeng():
    creative_Reeng = pd.merge(df_DBM_Reeng1, df_af_Reeng_use, left_on = ["CM Placement ID", "Date"], right_on = ["CM Placement ID", "Date"]
             ,how = 'outer').groupby(['Insertion Order', 'Line Item', "Creative"]).sum()

    creative_Reeng["CTR"] = (creative_Reeng["Clicks"]/creative_Reeng["Impressions"])*100
    creative_Reeng["CPC"] = creative_Reeng["Revenue (Adv Currency)"]/creative_Reeng["Clicks"]
    creative_Reeng["CPR"] = creative_Reeng["Revenue (Adv Currency)"]/creative_Reeng["Registrations"]
    creative_Reeng["CPA"] = creative_Reeng["Revenue (Adv Currency)"]/creative_Reeng["Activations"]
    creative_Reeng["CPR"] = creative_Reeng.CPR.round(0)
    creative_Reeng["CPA"] = creative_Reeng.CPA.round(0)
    creative_Reeng["CPC"] = creative_Reeng.CPC.round(0)
    creative_Reeng["CTR"] = creative_Reeng.CTR.round(2)
    creative_Reeng["Revenue (Adv Currency)"] = creative_Reeng['Revenue (Adv Currency)'].round(0)
    return creative_Reeng
def df_fin_g():
    ftotal_android = pd.merge(df_DBM_android, df_af_android_use, left_on = ['CM Placement ID', 'Date'], right_on = ['CM Placement ID', 'Date']
                     , how = 'outer')
    ftotal_IOS = pd.merge(df_DBM_IOS, df_af_IOS_use, left_on = ["CM Placement ID", "Date"], right_on = ["CM Placement ID", "Date"]
                     , how = 'outer')
    ftotal_Reeng = pd.merge(df_DBM_Reeng1, df_af_Reeng_use, left_on = ["CM Placement ID", "Date"], right_on = ["CM Placement ID", "Date"]
                     , how = 'outer')
    df_total_af = pd.concat([ftotal_android, ftotal_IOS, ftotal_Reeng])
    df_total_af = df_total_af.groupby(["Date", "Insertion Order"]).sum()

    df_fin_af = pd.concat([ftotal_android, ftotal_IOS, ftotal_Reeng])
    df_DBM_others = df_DBM[(df_DBM["Insertion Order"].str.contains("Android") == False) & (df_DBM["Insertion Order"].str.contains("iOS") == False) & (df_DBM["Insertion Order"].str.contains("New user") == False)]
    df_fin = pd.merge(df_DBM_others, df_fin_af, left_on = ["CM Placement ID", "Date"], right_on = ["CM Placement ID", "Date"]
            , how = 'outer')
    df_fin_g = df_fin.groupby(["Date", "Insertion Order_x","Line Item_x"]).sum()
    df_fin_g["Impressions_x"] == df_fin_g["Impressions_x"]/2
    df_fin_g["Clicks_x"] == df_fin_g["Clicks_x"]/2
    df_fin_g["Revenue (Adv Currency)_x"] = df_fin_g["Revenue (Adv Currency)_x"]/2
    del df_fin_g["Impressions_y"]
    del df_fin_g["Clicks_y"]
    del df_fin_g["Line Item ID_y"]
    del df_fin_g["Revenue (Adv Currency)_y"]
    df_fin_g["CTR"] = (df_fin_g["Clicks_x"]/df_fin_g["Impressions_x"])*100
    df_fin_g["CPC"] = df_fin_g["Revenue (Adv Currency)_x"]/df_fin_g["Clicks_x"]
    df_fin_g["CPR"] = df_fin_g["Revenue (Adv Currency)_x"]/df_fin_g["Registrations"]
    df_fin_g["CPA"] = df_fin_g["Revenue (Adv Currency)_x"]/df_fin_g["Activations"]
    df_fin_g["CPR"] = df_fin_g.CPR.round(0)
    df_fin_g["CPA"] = df_fin_g.CPA.round(0)
    df_fin_g["CPC"] = df_fin_g.CPC.round(0)
    df_fin_g["CTR"] = df_fin_g.CTR.round(2)
    df_fin_g["Revenue (Adv Currency)_x"] = df_fin_g['Revenue (Adv Currency)_x'].round(0)
    return df_fin_g

download_name = read_last_file()[-1].split('_')[0]
today = datetime.today()
d3 = today.strftime("%m-%d-%y")
d3 = d3[:5]
name = download_name + "_" + "Tracker_" + d3 + ".xlsx"
writer = pd.ExcelWriter(name)
new_df_android().to_excel(writer, 'Android')
new_df_IOS().to_excel(writer, 'IOS')
#new_df_Reeng.to_excel(writer, 'Reengagement')

df_col_android().to_excel(writer, 'Android_City')
df_col_IOS().to_excel(writer, 'IOS_City')
#df_col_Reeng().to_excel(writer, 'Reengagement_City')
df_Total().to_excel(writer, "Total tracking")
city_total().to_excel(writer, "City Total")
df_indiranagar().to_excel(writer, "Indiranagar")
ind().to_excel(writer, "Indiranagar Tracker Total")
creative_android().to_excel(writer, "Android_Creative")
creative_IOS().to_excel(writer, 'IOS_Creative')
#creative_Reeng.to_excel(writer, 'Reeng_Creative')
df_fin_g().to_excel(writer, 'unlisted IOs')
writer.save()

def my_speech(message):
    engine = pyttsx3.init()
    engine.setProperty('rate',150)
    engine.say('{}'.format(message))
    engine.runAndWait()

act = str(df_Total()['Activations'][-1])
cpa1 = str(df_Total()['CPA'][-1])
spends = str(df_Total()["Revenue (Adv Currency)"][-1].astype(int))

#Reeng_act = new_df_Reeng()["Activations"][-1]
android_act = new_df_android()["Activations"][-1]
IOS_act = new_df_IOS()["Activations"][-1]
lis_act = [android_act, IOS_act]


message = "Hello Satan. Yesterday, You spent " + spends + " and got "  + act + " activations at a cost of " + cpa1 + " rupees per activation. "  

print(message)
my_speech(message)