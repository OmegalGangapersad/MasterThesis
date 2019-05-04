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
               20190215:
                   - Added logs
               20190424:
                   - Added Google drive shareable link download                                  
               20190428:
                   - Adjusted Dates function to incorporate BBBEEdummy and ReturnYearEndDummy and simplify month end calculation
                   - Created new simpler stacking function StackDFDS_simple
                   - Removed old StackDFDS
                   - Replaced SECID with FirmID
                   - Added code in comment to create sector dummy matrix
               20190501:
                   - Created outlier function
               20190502:
                   - Created DescribeExNan function
                   - Created DescribeExNan for multiple years function called DescribeMultipleYearsFactor
               20190504:                
                   - Created OLSStandardizeXCol to standardize x columns for regressions
                   - Abstracted regression for 5 years into function Regression5YearOutput
                   - Created Abstracted scatterplots into PriceLogScatterplots
                   
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

def DatesDF(InputDataFrame):
    from datetime import datetime
    import numpy as np
    import pandas as pd
    tmpDateTime = [date.to_datetime() for date in InputDataFrame]
    tmpYear = [date.year for date in InputDataFrame]
    tmpMonth = [date.month for date in InputDataFrame]
    tmpDay = [date.day for date in InputDataFrame]
    tmpDateID = np.array(InputDataFrame.index.get_level_values(0))
    MonthEndDummy = np.array(pd.DataFrame(tmpMonth) - pd.DataFrame(tmpMonth).shift(1))
    MonthEndDummy[0] = 1
    MonthEndDummy[MonthEndDummy!=0] = 1   
    MonthEndDummy = np.array(MonthEndDummy)[:,0] 
    BBBEEReleaseDateDummy = np.zeros(shape=InputDataFrame.shape[0])
    ReturnYearEndDummy = np.zeros(shape=InputDataFrame.shape[0])
    DatesDF = pd.DataFrame({
                        'DateID':tmpDateID,            
                        'DateTime':tmpDateTime,
                        'Year':tmpYear,
                        'Month':tmpMonth,
                        'Day':tmpDay,
                        'MonthEndDummy':MonthEndDummy,
                        'BBBEEReleaseDateDummy':BBBEEReleaseDateDummy,
                        'ReturnYearEndDummy':ReturnYearEndDummy
                    })
    return DatesDF
    #source: https://stackoverflow.com/questions/25852044/converting-pandas-tslib-timestamp-to-datetime-python

def StackDFDS_simple(InputDataFrameName,ValueName):
    import pandas as pd
    OutputDF = pd.DataFrame(InputDataFrameName.stack())
    OutputDF['FirmID'] = OutputDF.index.get_level_values(1)
    OutputDF['DateID'] = OutputDF.index.get_level_values(0)
    OutputDF.columns = [ValueName,'FirmID','DateID']
    OutputDF = OutputDF.reset_index(drop=True)
    return OutputDF

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

def CapOutliers(InputDataFrame, InputColumn):
    import seaborn as sns
    import numpy as np
    import os    
    MainDir = os.path.dirname(os.path.realpath(__file__)) #working directory
    ExportDir = MainDir + '\\Output\\Descriptives\\' 
    
    tmpDF = InputDataFrame[['Year','FirmID',InputColumn]]
    tmpDF = tmpDF.dropna()
    tmpDF = tmpDF.drop_duplicates()
    tmpPreOutlierOverview = tmpDF.describe()
    tmpPreOutlierOverview.to_excel(ExportDir + InputColumn + 'PreOutlierOverview.xlsx', sheet_name='Input') 
    tmpPreOutlierPlot = sns.distplot(np.array(tmpDF[InputColumn]))  # getDistribution of InputColumn
    tmpPreOutlierPlot = tmpPreOutlierPlot.get_figure() #save distribution of InputColumn
    tmpPreOutlierPlot.savefig(ExportDir + InputColumn + '_PreOutlierDistribution.png')
    minValue = tmpPreOutlierOverview[InputColumn]['mean'] - (2*tmpPreOutlierOverview[InputColumn]['std']) #handle outlier minimum
    maxValue = tmpPreOutlierOverview[InputColumn]['mean'] + (2*tmpPreOutlierOverview[InputColumn]['std']) #handle outlier max
    tmpDF.loc[(tmpDF[InputColumn]<=minValue),InputColumn] = minValue
    tmpDF.loc[(tmpDF[InputColumn]>=maxValue),InputColumn] = maxValue
    tmpPostOutlierPlot = sns.distplot(np.array(tmpDF[InputColumn]))  # getDistribution of InputColumn
    tmpPostOutlierPlot = tmpPostOutlierPlot.get_figure() #save distribution of InputColumn
    tmpPostOutlierPlot.savefig(ExportDir + InputColumn + '_PostOutlierDistribution.png')
    tmpPostOutlierOverview = tmpDF.describe()
    tmpPostOutlierOverview.to_excel(ExportDir + InputColumn + 'PostOutlierOverview.xlsx', sheet_name='Input') 
   
    return tmpDF

def DescribeExNaN(InputDataFrame,InputColumn,InputMainDataFrame):
    import pandas as pd
    tmpDF = pd.DataFrame(InputDataFrame[InputColumn].dropna())
    tmpDescribe = pd.DataFrame(tmpDF.describe())
    tmpOutput = pd.merge(InputMainDataFrame,tmpDescribe,how='left',left_index=True, right_index=True)
    
    return tmpOutput

def DescribeMultipleYearsFactor(InputDataFrame,InputColumnPrefix,InputYears,InputYearsColumn,InputMainDataFrame):
    for ii in range(InputYears.shape[0]):
        tmpColumnName = str(InputColumnPrefix+str(InputYears[InputYearsColumn][ii]))
        InputMainDataFrame = DescribeExNaN(InputDataFrame,tmpColumnName,InputMainDataFrame)
    return InputMainDataFrame

def OLSStandardizeXCol(InputDataFrame):
    import pandas as pd
    tmpXColumns  = pd.DataFrame(InputDataFrame.columns)
    for jj in range(tmpXColumns.shape[0]):
        tmpColumnName = str(tmpXColumns[0][jj])
        if tmpColumnName.find('DSDUMMY_') == 0:
            tmpXColumns[0][jj] = tmpColumnName[8:] #remove DSDUMMY for sector
        if tmpColumnName.find('BP') == 0:
            tmpXColumns[0][jj] = 'BP'
        if tmpColumnName.find('SIZE') == 0:
            tmpXColumns[0][jj] = 'SIZE'
        if tmpColumnName.find('MarketPremium') == 0:
            tmpXColumns[0][jj] = 'MarketPremium'
        if tmpColumnName.find('RiskFreeReturn') == 0:
                tmpXColumns[0][jj] = 'RiskFreeReturn'
        if tmpColumnName.find('BBBEE_Rank') == 0:
                tmpXColumns[0][jj] = 'BBBEE_Rank'
    InputDataFrame.columns = tmpXColumns[0]
    return InputDataFrame
    
def Regression5YearOutput(InpDict, ExpDir):
    import pandas as pd  
    from statsmodels.iolib.summary2 import summary_col      
    tmpKeys = pd.DataFrame(list(InpDict.keys()))
    tmpModelName = tmpKeys[0][0][:-1] #get name from first key - assumes all key are the same 
    results_table = summary_col(results=[InpDict[str(tmpModelName + '5')],
                                         InpDict[str(tmpModelName + '4')],
                                         InpDict[str(tmpModelName + '3')],
                                         InpDict[str(tmpModelName + '2')],
                                         InpDict[str(tmpModelName + '1')]],
                                float_format='%0.2f',
                                stars = True,
                                model_names=['1','2','3','4','5'],
                                info_dict={'N':lambda x: "{0:d}".format(int(x.nobs)),'R2':lambda x: "{:.2f}".format(x.rsquared)})
    results_table.add_title(str('OLS Regressions - ' +  tmpModelName))    
    with open(ExpDir + 'OLS_Summary_' + tmpModelName + '.txt', 'w') as fh: #Output
        fh.write(results_table.as_text())

def PriceLogScatterplots(inpXColumns,inpYColumn,inpDataFrame,ExpDir):
    import pandas as pd
    import matplotlib.pyplot as plt    
    for xx in range(pd.DataFrame(inpXColumns).shape[0]):
        tmpXColumn = inpXColumns[xx]
        fig = plt.figure()
        plt.rc('font', family='serif')
        plt.scatter(inpDataFrame[tmpXColumn],inpDataFrame[inpYColumn],color='k')
        plt.xlabel(tmpXColumn)
        plt.ylabel(inpYColumn)
        plt.show()
        fig.savefig(ExpDir + 'ScatterPlot_'+ inpYColumn +  '_' + tmpXColumn + '.png')    
   


"""
def RankingPerGroup(tmpColumns, tmpGroupOnColumnsInt, tmpSourceDataFrame): #enabled for list based grouping
    import pandas as pd
    import numpy as np    
    tmpGrouping = tmpColumnGroup
    tmpLoopGroup = ColumnGroup.iloc[0] 

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
    
CODE TO CREATE DUMMY VARIABLE SECTORS

for ii in range(tmpSECTORID.shape[0]): #create a dummy variable name for all DS_SECTORS
    tmpColumnName = str('DSDUMMY_' + str(tmpSECTORID['DS_SECTORNAME'][ii]))
    tmpSECTORDummyMat = pd.DataFrame(np.zeros(shape=(StagingSECTOR.shape[0],1)))
    tmpSECTORDummyMat[StagingSECTOR['DS_SECTORNAME'] == str(tmpSECTORID['DS_SECTORNAME'][ii])] = 1
    StagingSECTOR[str(tmpColumnName)] = tmpSECTORDummyMat

#UNCOMMENT BELOW TO ADD ICB SECTOR
tmpSECTORID_ICB =  StagingSECTOR[['ICB_SECTORNAME']]   
tmpSECTORID_ICB = tmpSECTORID_ICB.drop_duplicates()
tmpSECTORID_ICB = tmpSECTORID_ICB.reset_index(drop=True)
for ii in range(tmpSECTORID_ICB.shape[0]): #create a dummy variable name for all DS_SECTORS
    tmpColumnName = str('ICBDUMMY_' + str(tmpSECTORID_ICB['ICB_SECTORNAME'][ii]))
    tmpSECTORDummyMat = pd.DataFrame(np.zeros(shape=(StagingSECTOR.shape[0],1)))
    tmpSECTORDummyMat[StagingSECTOR['ICB_SECTORNAME'] == str(tmpSECTORID_ICB['ICB_SECTORNAME'][ii])] = 1
    StagingSECTOR[str(tmpColumnName)] = tmpSECTORDummyMat



#CREATE STAGINGSECTOR DS
tmpSECTORMat = StagingFirmID.drop(['Raw_FirmID','RIC','ISIN','CompanyName','DS_SECTORNAME','ICB_SECTORNAME','ICB_SECTORID'], axis=1) #create this tmpMat to join to StagingDS
StagingSECTORDS = pd.DataFrame({
                        'DateID':np.array(StackPrice['DateID']),
                        'FirmID':np.array(StackPrice['FirmID'])                        
                        })
StagingSECTORDS =pd.merge(StagingSECTORDS,tmpSECTORMat,on='FirmID',how='left')

#CREATE STAGINGSECTOR ICB
tmpSECTORMat = StagingFirmID.drop(['Raw_FirmID','RIC','ISIN','CompanyName','DS_SECTORNAME','ICB_SECTORNAME','DS_SECTORID'], axis=1) #create this tmpMat to join to StagingDS
StagingSECTORDS = pd.DataFrame({
                        'DateID':np.array(StackPrice['DateID']),
                        'FirmID':np.array(StackPrice['FirmID'])                        
                        })
StagingSECTORDS =pd.merge(StagingSECTORDS,tmpSECTORMat,on='FirmID',how='left')


StagingDS = pd.merge(StagingDS,tmpSECTORMat,on='FirmID',how='left')
    
"""
