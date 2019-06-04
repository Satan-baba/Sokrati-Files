import pandas as pd
import os
import numpy as np
from datetime import datetime, timedelta
from stat import S_ISREG, ST_CTIME, ST_MODE
import os, sys, time
import pyttsx3

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
df_DBM_Reeng = df_DBM_Reeng.dropna()
del df_DBM['Advertiser Currency']
del df_DBM_Reeng["Advertiser Currency"]
df_af_android_use = df_af_android[["Date","Campaign (c)","Installs","new_user_registration (Unique users)","appsflyer_new_order_created (Unique users)"]]
df_af_IOS_use = df_af_IOS[["Date","Campaign (c)","Installs","new_user_registration (Unique users)","appsflyer_new_order_created (Unique users)"]]
df_af_Reeng_use = df_af_Reeng[["Date","Campaign (c)","new_user_registration (Unique users)","appsflyer_new_order_created (Unique users)"]]
df_af_IOS_use.columns = ["Date","CM Placement ID", "Installs", "Registrations", "Activations" ] 
df_af_android_use.columns = ["Date","CM Placement ID", "Installs", "Registrations", "Activations" ] 
df_af_Reeng_use.columns = ["Date","CM Placement ID", "Registrations", "Activations" ]
df_DBM = df_DBM.dropna()
df_DBM["CM Placement ID"] = df_DBM["CM Placement ID"].astype(int)
df_DBM["Date"] = pd.to_datetime(df_DBM["Date"])
df_af_android_use["Date"] = pd.to_datetime(df_af_android_use["Date"])
df_af_IOS_use["Date"] = pd.to_datetime(df_af_IOS_use["Date"])
df_af_Reeng_use["Date"] = pd.to_datetime(df_af_Reeng_use["Date"])
df_DBM["CM Placement ID"] = df_DBM["CM Placement ID"].apply(str) 
new_df_android  = pd.merge(df_DBM, df_af_android_use, left_on = ['CM Placement ID', 'Date'], right_on = ['CM Placement ID', 'Date'])
new_df_android = new_df_android.groupby(["Date"]).sum()

lis_android = []
for i in range(len(df_DBM["Insertion Order"].str.contains("Android"))):
    if df_DBM["Insertion Order"].str.contains("Android")[i] == True:
        lis_android.append(i)
        
        
df_android_revenue = df_DBM.iloc[lis_android].groupby(["Date"]).sum()
new_df_android["Impressions"] = df_android_revenue["Impressions"]
new_df_android["Clicks"] = df_android_revenue["Clicks"]
new_df_android["Revenue (Adv Currency)"] = df_android_revenue["Revenue (Adv Currency)"]
new_df_android["CTR"] = (new_df_android["Clicks"]/new_df_android["Impressions"])*100
new_df_android["CPC"] = new_df_android["Revenue (Adv Currency)"]/new_df_android["Clicks"]
new_df_android["CPI"] = new_df_android["Revenue (Adv Currency)"]/new_df_android["Installs"]
new_df_android["CPR"] = new_df_android["Revenue (Adv Currency)"]/new_df_android["Registrations"]
new_df_android["CPA"] = new_df_android["Revenue (Adv Currency)"]/new_df_android["Activations"]
new_df_android["CPI"] = new_df_android.CPI.round(0)
new_df_android["CPR"] = new_df_android.CPR.round(0)
new_df_android["CPA"] = new_df_android.CPA.round(0)
new_df_android["CPC"] = new_df_android.CPC.round(0)
new_df_android["CTR"] = new_df_android.CTR.round(2)
new_df_android["Revenue (Adv Currency)"] = new_df_android["Revenue (Adv Currency)"].astype(int)
df_af_IOS_use["CM Placement ID"] = df_af_IOS_use["CM Placement ID"].apply(str)
new_df_IOS  = pd.merge(df_DBM, df_af_IOS_use, left_on = ['CM Placement ID', 'Date'], right_on = ['CM Placement ID', 'Date'])
new_df_IOS = new_df_IOS.groupby(["Date"]).sum()

lis_IOS = []
for i in range(len(df_DBM["Insertion Order"].str.contains("iOS"))):
    if df_DBM["Insertion Order"].str.contains("iOS")[i] == True:
        lis_IOS.append(i)
        
        
df_IOS_revenue = df_DBM.iloc[lis_IOS].groupby(["Date"]).sum()
new_df_IOS["Impressions"] = df_IOS_revenue["Impressions"]
new_df_IOS["Clicks"] = df_IOS_revenue["Clicks"].sum()
new_df_IOS["Revenue (Adv Currency)"] = df_IOS_revenue["Revenue (Adv Currency)"].sum()
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
df_af_Reeng_use["CM Placement ID"] = df_af_Reeng_use["CM Placement ID"].apply(str)
df_DBM_Reeng["Date"] = pd.to_datetime(df_DBM_Reeng["Date"])
new_df_Reeng  = pd.merge(df_DBM_Reeng, df_af_Reeng_use, left_on = ['CM Placement ID', 'Date'], right_on = ['CM Placement ID', 'Date'])
new_df_Reeng = new_df_Reeng.groupby(["Date"]).sum()
df_Reeng_revenue = df_DBM_Reeng.groupby(["Date"]).sum()
new_df_Reeng["Impressions"] = df_Reeng_revenue["Impressions"]
new_df_Reeng["Clicks"] = df_Reeng_revenue["Clicks"]
new_df_Reeng["Revenue (Adv Currency)"] = df_Reeng_revenue["Revenue (Adv Currency)"]
new_df_Reeng["CTR"] = (new_df_Reeng["Clicks"]/new_df_Reeng["Impressions"])*100
new_df_Reeng["CPC"] = new_df_Reeng["Revenue (Adv Currency)"]/new_df_Reeng["Clicks"]
new_df_Reeng["CPR"] = new_df_Reeng["Revenue (Adv Currency)"]/new_df_Reeng["Registrations"]
new_df_Reeng["CPA"] = new_df_Reeng["Revenue (Adv Currency)"]/new_df_Reeng["Activations"]
new_df_Reeng["CPR"] = new_df_Reeng.CPR.round(0)
new_df_Reeng["CPA"] = new_df_Reeng.CPA.round(0)
new_df_Reeng["CPC"] = new_df_Reeng.CPC.round(0)
new_df_Reeng["CTR"] = new_df_Reeng.CTR.round(2)
new_df_Reeng["Revenue (Adv Currency)"] = new_df_Reeng["Revenue (Adv Currency)"].astype(int)
df_Total = pd.DataFrame()
df_DBM_revenue = df_DBM.groupby(["Date"]).sum()

df_Total["Impressions"] = df_DBM_revenue["Impressions"]
df_Total["Clicks"] = df_DBM_revenue["Clicks"]
df_Total["Revenue (Adv Currency)"] = df_DBM_revenue["Revenue (Adv Currency)"]

df_Total["Installs"] = new_df_android["Installs"] + new_df_IOS["Installs"]
df_Total["Registrations"] = new_df_android["Registrations"] + new_df_IOS["Registrations"] + new_df_Reeng["Registrations"]
df_Total["Activations"] = new_df_android["Activations"] + new_df_IOS["Activations"] + new_df_Reeng["Activations"]
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
df_col_android["CPI"] = df_col_android.CPI.round(0)
df_col_android["CPR"] = df_col_android.CPR.round(0)
df_col_android["CPA"] = df_col_android.CPA.round(0)
df_col_android["CPC"] = df_col_android.CPC.round(0)
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


cities = ["Bangalore", "Pune", "Gurgaon"]
cities_ser = pd.Series(cities)
yesterday = datetime.now() - timedelta(days=1)
yesterday = yesterday.strftime('%Y-%m-%d')
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

df_af_android_use
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

df_ind_total = df_indiranagar.reset_index()
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
df_indiranagar.to_excel(writer, "Indiranagar")
ind.to_excel(writer, "Indiranagar Tracker Total")
writer.save()

def my_speech(message):
    engine = pyttsx3.init()
    engine.setProperty('rate',150)
    engine.say('{}'.format(message))
    engine.runAndWait()

act = str(df_Total['Activations'][-1])
cpa1 = str(df_Total['CPA'][-1])
spends = str(df_Total["Revenue (Adv Currency)"][-1].astype(int))

Reeng_act = new_df_Reeng["Activations"][-1]
android_act = new_df_android["Activations"][-1]
IOS_act = new_df_IOS["Activations"][-1]
lis_act = [Reeng_act, android_act, IOS_act]

least_act = 100000000
for i in lis_act:
    if i < least_act:
        least_act = i
        
if least_act == Reeng_act:
    bad_per = "Re-engagement was the worst performing segment for yesterday with only " + str(least_act) + " activations."
elif least_act == IOS_act:
    bad_per = "IOS was the worst performing segment for yesterday with only " + str(least_act) + " activations."
else:
    bad_per = "android was the worst performing segment for yesterday with only" + str(least_act) + " activations."
    
    
message = "Hello Satan. Yesterday, You spent " + spends + " and got "  + act + " activations at a cost of " + cpa1 + " rupees per activation. " + bad_per  

print(message)
my_speech(message)