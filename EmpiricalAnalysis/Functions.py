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
                   - Cleaned code
               20190505: 
                   - Created SimpleLineChart function
               20190512: 
                   - Created Bootstrap function
                   - Created BootstrapLineChart
               20190514: 
                   - Added output to LaTeX in Regression5YearOutput function
""" 
import numpy as np
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

def CapOutliers(InputDataFrame, InputColumn, InputExportDir):
    import seaborn as sns
    import numpy as np
    ExportDir = InputExportDir #MainDir + '\\Output\\Descriptives\\' 
    
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
    
def Regression5YearOutput(InpDict, ExpDir,InpTitlePrefix):
    import pandas as pd  
    from statsmodels.iolib.summary2 import summary_col      
    tmpKeys = pd.DataFrame(list(InpDict.keys()))
    tmpModelName = tmpKeys[0][0][:-1] #get name from first key - assumes all key are the same 
    results_table = summary_col(results=[InpDict[str(tmpModelName + '1')],
                                         InpDict[str(tmpModelName + '2')],
                                         InpDict[str(tmpModelName + '3')],
                                         InpDict[str(tmpModelName + '4')],
                                         InpDict[str(tmpModelName + '5')]],
                                float_format='%0.4f',
                                stars = True,
                                model_names=['1','2','3','4','5'],
                                info_dict={'N':lambda x: "{0:d}".format(int(x.nobs)),'R2':lambda x: "{:.2f}".format(x.rsquared)})
    results_table.add_title(str('OLS Regressions - ' +  tmpModelName))    
    with open(ExpDir + InpTitlePrefix + tmpModelName + '.txt', 'w') as fh: #Output text
        fh.write(results_table.as_text())
    with open(ExpDir + InpTitlePrefix + tmpModelName + '.tex', 'w') as f: #Output latex
        f.write(results_table.as_latex())


def SimpleLineChart(inpXColumn,inpYColumn,inpDataFrame,ExpDir,XLabel,YLabel):    
    import matplotlib.pyplot as plt
    if XLabel == '':
        XLabel = inpXColumn
    else:
        XLabel = XLabel        
    if YLabel == '':
        YLabel = inpYColumn
    else:
        YLabel = YLabel
        
    fig = plt.figure()
    plt.rc('font', family='serif')
    plt.plot(inpDataFrame[inpYColumn],inpDataFrame[inpXColumn],color='k')
    plt.xlabel(YLabel)
    plt.ylabel(XLabel)
    plt.show()
    fig.savefig(ExpDir + 'Plot_'+ inpYColumn +  '_' + inpXColumn + '.png')   

def BootstrapLineChart(inpDataFrame,ExpDir,inpTitle):    
    # this function assumes that the first and second column are the confidence intervals and 3rd and 4th the BBBEE columns    
    import matplotlib.pyplot as plt        
    inpXColumn = 'Year'
    XLabel = inpXColumn
    YLabel = 'Cumulative Share price return'
    tmpYColumns = inpDataFrame.columns
    tmpYColumns = tmpYColumns.drop(['Year'])
    Markers = [',',',',',',',']
    LineStyles = ['-','-','-','--']
    Colors = ['grey','grey','k','k']
    fig = plt.figure()
    plt.rc('font', family='serif')
    for ii in range(len(tmpYColumns)):
        inpYColumn = str(tmpYColumns[ii])        
        plt.plot(inpDataFrame[inpXColumn],inpDataFrame[inpYColumn], color=Colors[ii],linestyle=LineStyles[ii],marker=Markers[ii])
        plt.ylabel(YLabel)    
    plt.xlabel(XLabel)
    plt.legend(prop={'size': 8})
    plt.show()
    fig.savefig(ExpDir + inpTitle + '.png')    
    
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
        #plt.show()
        fig.savefig(ExpDir + 'ScatterPlot_'+ inpYColumn +  '_' + tmpXColumn + '.png') 

def bootstrap(dataset, confidence=0.95, iterations=10000,
              sample_size=1.0, statistic=np.mean):
    """
    Bootstrap the confidence intervals for a given sample of a population
    and a statistic.
    Args:
        dataset: A list of values, each a sample from an unknown population
        confidence: The confidence value (a float between 0 and 1.0)
        iterations: The number of iterations of resampling to perform
        sample_size: The sample size for each of the resampled (0 to 1.0
                     for 0 to 100% of the original data size)
        statistic: The statistic to use. This must be a function that accepts
                   a list of values and returns a single value.
    Returns:
        Returns the upper and lower values of the confidence interval.
    """
    import numpy as np
    from sklearn.utils import resample
    
    stats = list()
    n_size = int(len(dataset) * sample_size)

    for _ in range(iterations):
        # Sample (with replacement) from the given dataset
        sample = resample(dataset, n_samples=n_size)
        # Calculate user-defined statistic and store it
        stat = statistic(sample)
        stats.append(stat)

    # Sort the array of per-sample statistics and cut off ends
    ostats = sorted(stats)
    lval = np.percentile(ostats, ((1 - confidence) / 2) * 100)
    uval = np.percentile(ostats, (confidence + ((1 - confidence) / 2)) * 100)

    return (lval, uval)    
    # source: https://github.com/mvanga/pybootstrap/blob/master/pybootstrap/__init__.py     
