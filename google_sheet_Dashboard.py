# -*- coding: utf-8 -*-
"""
Created on Wed Feb 10 15:26:34 2021

@author: dusa9002
"""
%reset -f
#pip install google_spreadsheet
#pip install google-auth-oauthlib
#pip install -t lib google-api-python-client
#pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib
#pip install gspread-pandas
#pip install gspread-dataframe
import numpy as np
import pandas as pd
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow,Flow
from google.auth.transport.requests import Request
import os
import pickle

#################################################### USER INPUTS #####################################################

os.chdir('C:/Users/dusa9002/Documents/Dashboard/')

### Provide Cycle name eg: W678
wave = "W8910"

### Provide NUE tool output containing the column "UNIVERSE"
df = pd.read_csv("NUE_OUTPUT_for_dashboard.csv", encoding= 'latin-1')

### Provide SUMMARY FILE (SF) path
path = "C:/Users/dusa9002/Documents/Dashboard/Egypt_NUE Summary_for_DASHBOARD.xlsx"

### Reading SF into pandas
summary = pd.read_excel(path, sheet_name = "Egypt_NUE Summary_(SF)")
mapping = pd.read_excel(path , sheet_name = "MAP")



### Open this section if DF is to be called from Google Sheets #################################################
################################ Call Google Sheet #################################################################
#SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
## here enter the id of your google sheet
#SAMPLE_SPREADSHEET_ID_input = '1YLg7D9_KJimrqisLiXo9SBMqq-I-RwzVDjzCkEAj8Jc'
#SAMPLE_RANGE_NAME = 'A1:FZ1000'
#
#def main():
#    global values_input, service
#    creds = None
#    if os.path.exists('token.pickle'):
#        with open('token.pickle', 'rb') as token:
#            creds = pickle.load(token)
#    if not creds or not creds.valid:
#        if creds and creds.expired and creds.refresh_token:
#            creds.refresh(Request())
#        else:
#            flow = InstalledAppFlow.from_client_secrets_file(
#                'client_secret.json', SCOPES) # here enter the name of your downloaded JSON file
#            creds = flow.run_local_server(port=0)
#        with open('token.pickle', 'wb') as token:
#            pickle.dump(creds, token)
#
#    service = build('sheets', 'v4', credentials=creds)
#
#    # Call the Sheets API
#    sheet = service.spreadsheets()
#    result_input = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID_input,
#                                range=SAMPLE_RANGE_NAME).execute()
#    values_input = result_input.get('values', [])
#
#    if not values_input and not values_expansion:
#        print('No data found.')
#
#main()
#
#df=pd.DataFrame(values_input[:], columns=values_input[0])
################################################# DF TRANSFORMATIONS ##############################################
##################################################### DF Calling ############################################

### Rounding up the universe from NUE output
df['UNIVERSE'] = round(df['UNIVERSE'],0)
#Splitting PSU_Stratum to get Urbanity
## IF URBANITY already present, remove this
df[['Urbanity', 'NN']] = df.PSU_STRATUM.apply(lambda x: pd.Series(str(x).split("-", 1 )))
del df['NN']

### Splitting ampping file for shorter DFs and for merging purpose
mbd_map = mapping.iloc[:,0:2]
urb_map = mapping.iloc[:,2:4]
out_map = mapping.iloc[:,4:6]
index_map = mapping.iloc[:,6:8]

index_map['Columns'] = index_map['Columns'].str.strip()         #Removing whitespaces from Column names
index_map.dropna(axis=0, how='all', inplace=True)               #Removing rows with all NA values
index_map = index_map.append(pd.DataFrame(np.array([['Bev_'+ wave, 'BEV'],['Tob_'+ wave, 'TOB'],[wave + "_UE_all", 'FMCG']]),
                         columns=['Columns', 'Index']), ignore_index=True)     ## Appending column name of tool output to column list

### Splitting column names according to index for filtering in the dashboard
fmcg_col = index_map[index_map['Index'] == 'FMCG']['Columns']
bev_col = index_map[index_map['Index'] == 'BEV']['Columns']
tob_col = index_map[index_map['Index'] == 'TOB']['Columns']

### Adjusting keywords for Reg, Urb, Shop_type by mapping from input file to form Cells identical to Summary file (SF)
df1 = df.copy()

df1.rename({"GEO_CODE":"STD MBD_ToolOutput"}, axis=1, inplace=True)
df1.rename({"Urbanity":"Urbanity_ToolOutput"}, axis=1, inplace=True)
df1.rename({"OUTTYPENM":"Oulet Type Name_ToolOutput"}, axis=1, inplace=True)

### Mapping with NUE Summary file Cell names
df1 = pd.merge(df1, mbd_map, how = 'left', on = "STD MBD_ToolOutput")
df1 = pd.merge(df1, urb_map, how = 'left', on = "Urbanity_ToolOutput")
df1 = pd.merge(df1, out_map, how = 'left', on = "Oulet Type Name_ToolOutput")

### Forming Cellnames wrt Summary File (SF)
df1['CELLNAME'] = df1['STD MBD _SF'] + " " + df1['Urbanity_SF'] + " " + df1["Oulet Type Name_SF"]

### Splitting DF into different indices for handler information
df_short = df1[['PSU', 'CELLNAME', 'OUTLETTYPE', 'Governerate', 'UNIVERSE', 'BEV_HAND', 'CIG_HAND', 'WAVE']]
df_bev = df_short[df_short['BEV_HAND'] == 1]            ## NEEDS AUTOMATION FOR OTHER INDICES
df_tob = df_short[df_short['CIG_HAND'] == 1]

### Grouping UNIVERSE acc. to CELLNAME
pivot1 = df_short.groupby('CELLNAME')['UNIVERSE'].sum().reset_index()
pivot_bev = df_bev.groupby('CELLNAME')['UNIVERSE'].sum().reset_index()
pivot_tob = df_tob.groupby('CELLNAME')['UNIVERSE'].sum().reset_index()

### Naming current bev_hand and tob_hand columns
pivot_bev.rename(columns = {'UNIVERSE':'Bev_'+ wave}, inplace=True)
pivot_tob.rename(columns = {'UNIVERSE':'Tob_'+ wave}, inplace=True)

### Merging current NUE data with summary file 
summary.rename({'Std_Cell': 'CELLNAME'},axis=1, inplace=True)
summary2 = pd.merge(summary, pivot1, how = 'left', on = 'CELLNAME')
summary2.rename({'UNIVERSE': wave + "_UE_all"},axis=1, inplace=True)
summary2 = pd.merge(summary2, pivot_bev, how = 'left', on = 'CELLNAME')
summary2 = pd.merge(summary2, pivot_tob, how = 'left', on = 'CELLNAME')

### Writing externally to check the summary for mistakes
#summary2.to_csv("checkforcell.csv")

### Removing whitespaces from column names
summary2 = summary2.rename(columns=lambda x: x.strip())

### Restructuring data for smooth execution in Google DataStudio by melting and keeping universe info in a single column
melt_columns = index_map['Columns'].to_list()
melted = pd.melt(summary2, id_vars= ['CELLNAME', 'STD_MBD', 'Urbanity', 'Oulet_Type_Name', 'CHAIN', 'FMCG_Channel',
                                     'Beverage_Channel', 'Tobacco_Channel', 'FMCG_reported', 'BEV_reported',
                                     'Tob_reported','Food_reported', 'Drug_reported', 'Liquor_reported', 'Unilever_reported'], 
                 value_vars= melt_columns)
melted = melted.dropna(axis=1, how= 'all')      ### Removing other indices info not present in country

### Adding Index information for dashboard handler filters
melted.rename(columns = {'variable': 'Columns', 'value':'Universe'}, inplace= True)
melted = pd.merge(melted, index_map, how='left', on= 'Columns')

### Removing Cells with no Universe value
melted1 = melted[melted['Universe'] > 0]
final_df = melted1.copy()
final_df = final_df.fillna(0)



### DIFFERENCES columns (Abs. and Rel diff. of NUE and CBA counts and other stuff)
summary2 = summary2.fillna(0)
cols_pass = fmcg_col.append([bev_col, tob_col, pd.Series(['CBA_PREV_COUNTS', 'CBA_CURR_YR'])])
diff_df = summary2.groupby(['STD_MBD', 'Urbanity'])[cols_pass].sum().reset_index()

## Percentage differences
#CBA Difference
diff_df['% CBA Growth'] = diff_df['CBA_CURR_YR']/ diff_df['CBA_PREV_COUNTS'] - 1

# Absolute Differences
abs_diff = pd.Series()
abs_index = pd.Series()


for i in range(0, len(fmcg_col)-1):
    diff_df[fmcg_col.iloc[i] + ' vs ' +  fmcg_col.iloc[i+1]] = diff_df[fmcg_col.iloc[i]] - diff_df[fmcg_col.iloc[i+1]]
    abs_diff = abs_diff.append(pd.Series(fmcg_col.iloc[i] + ' vs ' +  fmcg_col.iloc[i+1]))
    abs_index = abs_index.append(pd.Series('FMCG'))
    
for i in range(0, len(bev_col)-1):
    diff_df[bev_col.iloc[i] + ' vs ' +  bev_col.iloc[i+1]] = diff_df[bev_col.iloc[i]] - diff_df[bev_col.iloc[i+1]]
    abs_diff = abs_diff.append(pd.Series(bev_col.iloc[i] + ' vs ' +  bev_col.iloc[i+1]))
    abs_index = abs_index.append(pd.Series('BEV'))
    
for i in range(0, len(tob_col)-1):
    diff_df[tob_col.iloc[i] + ' vs ' +  tob_col.iloc[i+1]] = diff_df[tob_col.iloc[i]] - diff_df[tob_col.iloc[i+1]]
    abs_diff = abs_diff.append(pd.Series(tob_col.iloc[i] + ' vs ' +  tob_col.iloc[i+1]))
    abs_index = abs_index.append(pd.Series('TOB'))

abs_diff = pd.concat([abs_diff, abs_index], axis=1)
abs_diff.rename(columns = {0:'AD', 1:'AD_index'}, inplace = True) 
    
# Relative Differences
rel_diff = pd.Series()
index = pd.Series()

for i in range(0, len(fmcg_col)-1):
    diff_df['% '+fmcg_col.iloc[i] + ' vs ' +  fmcg_col.iloc[i+1]] = ((diff_df[fmcg_col.iloc[i]]/diff_df[fmcg_col.iloc[i+1]]) -1)*100
    rel_diff = rel_diff.append(pd.Series('% ' + fmcg_col.iloc[i] + ' vs ' +  fmcg_col.iloc[i+1]))
    index = index.append(pd.Series('FMCG'))

for i in range(0, len(bev_col)-1):
    diff_df['% '+bev_col.iloc[i] + ' vs ' +  bev_col.iloc[i+1]] = ((diff_df[bev_col.iloc[i]] / diff_df[bev_col.iloc[i+1]]) - 1)*100
    rel_diff = rel_diff.append(pd.Series('% '+bev_col.iloc[i] + ' vs ' +  bev_col.iloc[i+1]))
    index = index.append(pd.Series('BEV'))
    
for i in range(0, len(tob_col)-1):
    diff_df['% '+ tob_col.iloc[i] + ' vs ' +  tob_col.iloc[i+1]] = ((diff_df[tob_col.iloc[i]] / diff_df[tob_col.iloc[i+1]]) - 1)*100
    rel_diff = rel_diff.append(pd.Series('% '+tob_col.iloc[i] + ' vs ' +  tob_col.iloc[i+1]))
    index = index.append(pd.Series('TOB'))

rel_diff = pd.concat([rel_diff, index], axis=1)
rel_diff.rename(columns = {0:'RD', 1:'RD_index'}, inplace = True) 

## UNIVERSE contributors
uni_contri = pd.Series()
for i in range(0, len(fmcg_col)):
    diff_df[fmcg_col.iloc[i] + '_UniContri'] = (diff_df[fmcg_col.iloc[i]]/diff_df[fmcg_col.iloc[i]].sum())*100
    uni_contri = uni_contri.append(pd.Series(fmcg_col.iloc[i] + '_UniContri'))

col_df = pd.concat([abs_diff, rel_diff], axis=1)
col_df['temp'] = 1

### Melting into G-Data Studio format

ad_melt = pd.melt(diff_df, id_vars = ['STD_MBD', 'Urbanity'], value_vars = abs_diff['AD'])
ad_melt.rename(columns={'variable':'AD'}, inplace=True)


rd_melt = pd.melt(diff_df, id_vars = ['STD_MBD', 'Urbanity'], value_vars = rel_diff['RD'])
rd_melt.rename(columns={'variable':'RD'}, inplace=True)

contri_melt = pd.melt(diff_df, id_vars = ['STD_MBD', 'Urbanity'], value_vars = uni_contri)
contri_melt.rename(columns = lambda x: x + '_contri', inplace=True)


### Preparing df for merging two melted df
final_diff_df = diff_df.iloc[:, 0:2]
final_diff_df['temp'] = 1
final_diff_df = pd.merge(final_diff_df, col_df, on = 'temp', how = 'left')
final_diff_df.drop(columns={'temp'}, inplace=True)

final_diff_df = pd.merge(final_diff_df, ad_melt, on= ['STD_MBD', 'Urbanity', 'AD'], how= 'left')
final_diff_df.rename(columns={'value': 'Absolute Difference value'}, inplace = True)

final_diff_df = pd.merge(final_diff_df, rd_melt, on= ['STD_MBD', 'Urbanity', 'RD'], how= 'left')
final_diff_df.rename(columns={'value': 'Relative Difference value'}, inplace = True)

final_diff_df1 = final_diff_df
final_diff_df1 = final_diff_df1.replace([np.inf, -np.inf], np.nan)
final_diff_df1 = final_diff_df1.fillna(0)

contri_final = contri_melt
contri_final = contri_final.replace([np.inf, -np.inf], np.nan)
contri_final = contri_final.fillna("")

diff_df = diff_df.replace([np.inf, -np.inf], np.nan)
diff_df = diff_df.fillna("")

### Adding column headers for G-Sheet by duplicating them in the first row
final_df = final_df.columns.to_frame().T.append(final_df, ignore_index=True)
contri_final = contri_final.columns.to_frame().T.append(contri_final, ignore_index=True)
final_diff_df1 = final_diff_df1.columns.to_frame().T.append(final_diff_df1, ignore_index=True)
diff_df = diff_df.columns.to_frame().T.append(diff_df, ignore_index=True)


##################################### WRITING INTO G-SHEET #################################################
#change this by your sheet ID
SAMPLE_SPREADSHEET_ID_input = '1YLg7D9_KJimrqisLiXo9SBMqq-I-RwzVDjzCkEAj8Jc'

#change the range if needed
SAMPLE_RANGE_NAME = 'A1:FZ50000'
DIFF_RANGE = 'Sheet2!A1:FZ50000'
CONTRI_RANGE = 'Sheet3!A1:FZ50000'
TABLE_RANGE = 'Sheet4!A1:FZ50000'

def Create_Service(client_secret_file, api_service_name, api_version, *scopes):
    global service
    SCOPES = [scope for scope in scopes[0]]
    #print(SCOPES)
    
    cred = None

    if os.path.exists('token_write.pickle'):
        with open('token_write.pickle', 'rb') as token:
            cred = pickle.load(token)

    if not cred or not cred.valid:
        if cred and cred.expired and cred.refresh_token:
            cred.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(client_secret_file, SCOPES)
            cred = flow.run_local_server()

        with open('token_write.pickle', 'wb') as token:
            pickle.dump(cred, token)

    try:
        service = build(api_service_name, api_version, credentials=cred)
        print(api_service_name, 'service created successfully')
        #return service
    except Exception as e:
        print(e)
        #return None
        
# change 'my_json_file.json' by your downloaded JSON file.
Create_Service('client_secret.json', 'sheets', 'v4',['https://www.googleapis.com/auth/spreadsheets'])
    
def Export_Data_To_Sheets():
    body = {}
    resultClear = service.spreadsheets( ).values( ).clear( spreadsheetId=SAMPLE_SPREADSHEET_ID_input, range=SAMPLE_RANGE_NAME, 
                                      body=body ).execute( )
    response_date = service.spreadsheets().values().update(
        spreadsheetId=SAMPLE_SPREADSHEET_ID_input,
        valueInputOption='RAW',
        range=SAMPLE_RANGE_NAME,
        body=dict(
            majorDimension='ROWS',
            values=final_df.values.tolist())
    ).execute()
    print('Sheet successfully Updated')

Export_Data_To_Sheets()

def Export_Data_To_Sheets2():
    body = {}
    resultClear = service.spreadsheets( ).values( ).clear( spreadsheetId=SAMPLE_SPREADSHEET_ID_input, range=DIFF_RANGE, 
                                      body=body ).execute( )
    response_date = service.spreadsheets().values().update(
        spreadsheetId=SAMPLE_SPREADSHEET_ID_input,
        valueInputOption='RAW',
        range=DIFF_RANGE,
        body=dict(
            majorDimension='ROWS',
            values=final_diff_df1.values.tolist())
    ).execute()
    print('Sheet successfully Updated')

Export_Data_To_Sheets2()

def Export_Data_To_Sheets3():
    body = {}
    resultClear = service.spreadsheets( ).values( ).clear( spreadsheetId=SAMPLE_SPREADSHEET_ID_input, range=CONTRI_RANGE, 
                                      body=body ).execute( )
    response_date = service.spreadsheets().values().update(
        spreadsheetId=SAMPLE_SPREADSHEET_ID_input,
        valueInputOption='RAW',
        range=CONTRI_RANGE,
        body=dict(
            majorDimension='ROWS',
            values=contri_final.values.tolist())
    ).execute()
    print('Sheet successfully Updated')

Export_Data_To_Sheets3()

def Export_Data_To_Sheets4():
    body = {}
    resultClear = service.spreadsheets( ).values( ).clear( spreadsheetId=SAMPLE_SPREADSHEET_ID_input, range=TABLE_RANGE, 
                                      body=body ).execute( )
    response_date = service.spreadsheets().values().update(
        spreadsheetId=SAMPLE_SPREADSHEET_ID_input,
        valueInputOption='RAW',
        range=TABLE_RANGE,
        body=dict(
            majorDimension='ROWS',
            values=diff_df.values.tolist())
    ).execute()
    print('Sheet successfully Updated')

Export_Data_To_Sheets4()

############################################# THE END #######################################################################