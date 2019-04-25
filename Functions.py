# -*- coding: utf-8 -*-
#!/usr/bin/python
"""
@create  : 20190206 (yyyyMMdd)
@author  : Omegal Gangapersad
@Purpose : Container script which buckets all the functions used frequently. It contains
            - LogScript, general log function to keep track of progress of a script
            - StrfTimeDelta, this creates a string of a time elapsed (TimeDelta) in HH:MM:SS format
@instruct: Under the declare sections adjust: 
            - the importdirectory to the user's selected directory - always
            - the BBBEE, PRICE, BVPS, MV, YLD or SEC - if user altered source filenames
@update  : Date (yyyyMMdd)
               20190215
               - Added logs
               20190424
               - Added Google drive shareable link download               
""" 

def LogScript(ScriptName, Time, LogComment):
    import datetime
    tmpTimeStamp = Time.strftime('%d-%b-%Y %H:%M:%S') 
    print_string = "{} | {} | {}" #{} placeholders
    tmpMessage = print_string.format(tmpTimeStamp, ScriptName, LogComment)
    print(print_string.format(tmpTimeStamp, ScriptName, LogComment))  
    LogFile(tmpMessage)
    
def StrfTimeDelta(TimeDelta):
    TotSeconds = int(TimeDelta.total_seconds()) # Convert TimeDelta to total seconds
    Hours = TotSeconds // 3600 
    RemainingSeconds = TotSeconds - (Hours * 3600) # remaining seconds
    Minutes = RemainingSeconds // 60
    Seconds = RemainingSeconds - (Minutes * 60)
    Result = str('{:02}:{:02}:{:02}'.format(int(Hours), int(Minutes), int(Seconds)))
    return Result
    #source: https://stackoverflow.com/questions/538666/python-format-timedelta-to-string
    
def LogFile(Message):
    import logging
    import os
    for handler in logging.root.handlers[:]: #makes sure empty cache
        logging.root.removeHandler(handler)
    logging.basicConfig(format = '%(message)s',
                        datefmt = '%m/%d/%Y %I:%M:%S %p',
                        filename = os.path.dirname(os.path.realpath(__file__)) + '\\PythonLog.txt',
                        level=logging.INFO)
    logging.info(Message)
    #source: https://stackoverflow.com/questions/35898160/logging-module-not-writing-to-file?rq=1

def StagingDates(InputDataFrame):
    #STOPPED HERE
    from datetime import datetime
    StagingDateTime = [date.to_datetime() for date in InputDataFrame]
    StagingYear = [date.year for date in InputDataFrame]
    StagingMonth = [date.month for date in InputDataFrame]
    StagingDay = [date.day for date in InputDataFrame]
    #Source: https://stackoverflow.com/questions/25852044/converting-pandas-tslib-timestamp-to-datetime-python


def StackDFDS(InputDataFrameName,ValueName,tmpStagingDSAvailableSECIDDataFrame):
    import pandas as pd
    import numpy as np
    y = InputDataFrameName
    y = y.set_index([0])
    z = y.stack()
    z = pd.DataFrame(z)
    z['tmpSECID'] = z.index.get_level_values(1) #sec, must do - 1 to get mapping to index
    z['tmpSECID'] = np.array(z['tmpSECID'])-1
    z['tmpYear'] = z.index.get_level_values(0) 
    z.columns = [ValueName,'DSAvailableSECID','Year']
    z = pd.merge(z, tmpStagingDSAvailableSECIDDataFrame, on='DSAvailableSECID', how='left')
    del z['DSAvailableSECID']
    return z

def download_file_from_google_drive(id, destination):    
    import requests
    URL = "https://docs.google.com/uc?export=download"
    session = requests.Session()
    response = session.get(URL, params = { 'id' : id }, stream = True)
    token = get_confirm_token(response)
    if token:
        params = { 'id' : id, 'confirm' : token }
        response = session.get(URL, params = params, stream = True)
    save_response_content(response, destination)   
    #source: https://stackoverflow.com/questions/38511444/python-download-files-from-google-drive-using-url

def get_confirm_token(response):
    for key, value in response.cookies.items():
        if key.startswith('download_warning'):
            return value
    return None
    #source: https://stackoverflow.com/questions/38511444/python-download-files-from-google-drive-using-url    

def save_response_content(response, destination):
    CHUNK_SIZE = 32768
    with open(destination, "wb") as f:
        for chunk in response.iter_content(CHUNK_SIZE):
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
    #source: https://stackoverflow.com/questions/38511444/python-download-files-from-google-drive-using-url
                
def RankingPerGroup(tmpColumns, tmpGroupOnColumnsInt, tmpSourceDataFrame): #enabled for list based grouping
    import pandas as pd
    import numpy as np    
    tmpGrouping = tmpColumnGroup
    tmpLoopGroup = ColumnGroup.iloc[0] 
"""
#start function   
#libs
import pandas as pd
import numpy as np    

#INPUT
tmpColumns = ['Year','BBBEE_Rank','LogPriceReturn','BVPS','MV']   #input
tmpGroupOnColumnsInt = [0] 
tmpSourceDataFrame = FinalData.copy() #input

#CALCULATE
#check shape tmpGroupOnColumnsInt
if np.array(tmpGroupOnColumnsInt).shape[0] == 1:
    print('Sup')
    tmpGroupOnColumnsName = pd.DataFrame(tmpColumns).iloc[tmpGroupOnColumnsInt,0]    
else:
    print('Suuuup')
    tmpGroupOnColumnsName = tmpColumns[tmpGroupOnColumnsInt] #figure out how to make this work with only one value        
tmpColumns = pd.Series(tmpColumns)
tmpGroupOnColumnsName = pd.Series(tmpGroupOnColumnsName)
tmpBoolGroup =  tmpColumns.isin(tmpGroupOnColumnsName)
tmpColumnsToBeGrouped = tmpColumns[tmpBoolGroup == False]

#create output structure
tmpOutputColumns = list(tmpGroupOnColumnsName)
for indexInt, item in enumerate(tmpColumnsToBeGrouped):
    itemOutput = item + '_Rank'
    tmpOutputColumns.append(itemOutput)    
tmpOutputDF = pd.DataFrame(np.nan, index=[0], columns= tmpOutputColumns) #create an empty dataframe to initiate

#Loop through tmpLoopGroup1
tmpLoopGroup1Name = str(tmpGroupOnColumnsName[0]) #define loop group 1 column name
TmpSeriesGroup1UniqueValues= pd.DataFrame(tmpSourceDataFrame[tmpLoopGroup1Name].unique()) #get unique values loop group 1 to loop through

# actual start tmploopGroup1
Loop = 0 #remove this, just to make it work for now
tmpLoopGroup1Item = TmpSeriesGroup1UniqueValues.iloc[Loop,0]
TmpDF1 = tmpSourceDataFrame[tmpSourceDataFrame[str(tmpLoopGroup1Name)]==tmpLoopGroup1Item][tmpColumns] # get dataframe where condition -- remember this way and retrospect it in stagingdata etc





    tmpYear = UniqueYears['Year'][0]
    df = FinalData.loc[FinalData['Year'] == int(tmpYear), ['Year','BBBEE_Rank','LogPriceReturn','BVPS','MV',]]
    df['LogPriceReturn_Rank'] = df['LogPriceReturn'].rank() 
    df['BVPS_Rank'] = df['BVPS'].rank() 
    df['MV_Rank'] = df['MV'].rank() 
    df2 = df[['Year','LogPriceReturn_Rank','BBBEE_Rank','BVPS_Rank','MV_Rank']].copy()
"""
