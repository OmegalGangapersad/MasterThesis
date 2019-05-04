# -*- coding: utf-8 -*-
#!/usr/bin/python
"""
@create  :  20190215 (yyyyMMdd)
@author  :  Omegal Gangapersad
@Purpose :  Clean RawData table to produce a single clean table with data which can be used to perform analysis on. This is realised by walking through these steps
				1. Identify in Datastream data where columns are Error - index
				2. Identify these index in SEC
				3. Identify these index in BEE
				4. Create StagingData table
				5. Output StagingData to csv
@instruct:  Do not remove line 1 and 2. Code in this script on line 2 is used to be able to import other scripts as modules. 
@update  :  Date (yyyyMMdd)
            20190215:               
                - Created script
                - Played with plots
            20190429:   
                - Rewrote reading StagingData
                - Created segment to create working dataframe
                - Created descriptives of count observations - total and per sector
            20190430:
                - Created annual return fir, riskfree, market
                - Created dataset ready for (regression) analysis
                - Identified a method to cap outliers
            20190501:
                - Finished outliers - and export thereof - next add multiple years. Found positive (low R2 Annual LogReturn and BBBEE score!)
                - Rerank BBBEE to adjust for missing data
            20190502:
                - Cleaned code
                - Started methodology for return on different time frame
            20190503:
                - Added Riskfree return over different time frames
                - Started on factor returns
                - Abstracted Factor and Year
                - Created dummy top bottom 3iles BP, SIZE
                - Finished return on different time frame
                - Started on descriptives and scatterplot/regression/correlationmatrix per year
"""

##START SCRIPT
import os
import Functions
import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
tmpScriptName = os.path.basename(__file__)
tmpStartTimeScript = datetime.datetime.now()
tmpScriptName = os.path.basename(__file__)
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Start Script')
MainDir = os.path.dirname(os.path.realpath(__file__)) #working directory
ExportDir = MainDir + '\\Output\\Descriptives\\' 

##READ STAGING DATA
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Start STEP1: Read StagingData') 
try:
    InputFilenameBBBEE = 'BBBEE.csv'
    InputFilenameDS = 'DS.csv'
    InputFilenameFirm = 'Firm.csv'
    InputFilenameDates = 'Dates.csv'  
    tmpMainDir = os.path.dirname(os.path.realpath(__file__)) #working directory
    tmpImportDirectory = tmpMainDir + '\\Input\\StagingData\\' 
    StagingBBBEE = pd.read_csv(tmpImportDirectory + InputFilenameBBBEE)
    StagingDS = pd.read_csv(tmpImportDirectory + InputFilenameDS)
    StagingFirm = pd.read_csv(tmpImportDirectory + InputFilenameFirm)
    StagingDates = pd.read_csv(tmpImportDirectory + InputFilenameDates)
    del InputFilenameBBBEE, InputFilenameDS, InputFilenameFirm, InputFilenameDates, tmpMainDir, tmpImportDirectory
except:
    import StagingData


Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Start STEP2: Create working dataset called df') 
# DEFINE DF CRITERIA - CAN BE LOOPED OVER
BBBEEMonth = 4 #define month where BBBEE is released - April is value from van der Merwe paper
BBBEELag = 4 #define months Lag from BBBEE release Month - you can adjust this value to check when market reacts
MonthYearEnd = BBBEEMonth + BBBEELag #define month for year end to calculate

# Create Dataset0 rawest data, grouped by year
StagingDates.loc[(StagingDates['Month'] == BBBEEMonth) & (StagingDates['MonthEndDummy'] == 1),'BBBEEReleaseDateDummy'] = StagingDates.loc[(StagingDates['Month'] == BBBEEMonth) & (StagingDates['MonthEndDummy'] == 1),'Year'] #put year in BBBEEdummy to identify from where BBBEE scores run
StagingDates.loc[(StagingDates['Month'] == MonthYearEnd) & (StagingDates['MonthEndDummy'] == 1),'ReturnYearEndDummy'] = StagingDates.loc[(StagingDates['Month'] == MonthYearEnd) & (StagingDates['MonthEndDummy'] == 1),'Year'] #put year in BBBEEdummy to identify from where Return year end run
tmpYear = StagingDates[['DateID','ReturnYearEndDummy']]
tmpYear.columns = ['DateID','Year'] 
StagingDS = pd.merge(StagingDS,pd.DataFrame(tmpYear),on='DateID',how='left') #create column with ReturnYearEndDummy to merge with StagingBBBEE
Dataset0 = StagingDS.loc[StagingDS['Year']!=0] #this will return more rows than StagingBBBEE because StagingBBBEE only capture sec year combination for which there is a BBBEE score Dataset0 captures the sec even when there is no BBBEE score
Dataset0 = pd.merge(Dataset0,StagingBBBEE,on=['Year','FirmID'], how='left')

ObsVariableYear = Dataset0.groupby('Year').count() #Check number of firms with BBBEE score/without BBBEE score per year
ObsSectorYearCount = Dataset0.pivot_table(['BBBEE_Rank','Price'], index='Year', columns='DS_SECTORID', aggfunc='count') #Check number of firms per sector per year - compare with JSE All share Index - Price as proxy for all observations
ObsVariableYear.to_excel(ExportDir + 'ObsVariableYear.xlsx', sheet_name='Input')
ObsSectorYearCount.to_excel(ExportDir + 'ObsSectorYearCount.xlsx', sheet_name='Input') #work in excel to create %percentages


# Create Dataset1 which has returns, market premium and riskfree rate starting from BBBEEStartYear
BBBEEStartYear = 2004
Dataset1 = Dataset0.loc[(Dataset0['Year']>=(BBBEEStartYear-1))]
PriceLogReturnMatrix = Dataset1.pivot_table('PriceLogReturnCum', index='Year', columns='FirmID')
tmpYear = pd.DataFrame(PriceLogReturnMatrix.index.get_level_values(0))
tmpYear['YearIndex'] = tmpYear.index.get_level_values(0)
#Create industry dummy variable

# factor returns
FactorDecile = pd.DataFrame(['SIZE','BP'])
FactorDecile.columns = ['Factors']
for ii in range(FactorDecile.shape[0]):    
    inpFactor = FactorDecile['Factors'][ii]
    FactorDF = pd.DataFrame(columns=['FirmID','Year',str(inpFactor+'_Decile')])
    for jj in range(tmpYear.shape[0]):
        inpYear = tmpYear['Year'][jj]
        tmpFactorDF = Dataset1.loc[(Dataset1['Year']==inpYear),['FirmID','Year',inpFactor]]
        tmpFactorDF[str(inpFactor+'_Decile')] = pd.qcut(tmpFactorDF[inpFactor],10,labels=False)
        tmpFactorDF = tmpFactorDF[['FirmID','Year',str(inpFactor+'_Decile')]]
        FactorDF = pd.concat([FactorDF, tmpFactorDF])    
        del inpYear, tmpFactorDF
    Dataset1 = pd.merge(Dataset1,FactorDF,on=['FirmID','Year'],how='left')
    del inpFactor, FactorDF

del ii,jj    
BPMatrix = Dataset1.pivot_table('BP_Decile', index='Year', columns='FirmID')
BPTopDummy = ([BPMatrix <=2])
BPTopDummy = BPTopDummy[0]
BPBottomDummy = ([BPMatrix >=7])
BPBottomDummy = BPBottomDummy[0]
SIZEMatrix = Dataset1.pivot_table('SIZE_Decile', index='Year', columns='FirmID')
SIZETopDummy = ([SIZEMatrix <=2])
SIZETopDummy = SIZETopDummy[0]
SIZEBottomDummy = ([SIZEMatrix >=7])
SIZEBottomDummy = SIZEBottomDummy[0]

MarketReturn = tmpYear[['Year','YearIndex']]
MarketPremium = tmpYear[['Year','YearIndex']]
BPIndex = tmpYear[['Year','YearIndex']]
SIZEIndex = tmpYear[['Year','YearIndex']]
RiskFreeProxy = 'SA10YR'
RiskFreeReturn = Dataset1[['Year',RiskFreeProxy]]
RiskFreeReturn = RiskFreeReturn.drop_duplicates()
RiskFreeReturn[RiskFreeProxy] = RiskFreeReturn[RiskFreeProxy]/100
RiskFreeReturn = RiskFreeReturn.reset_index(drop=True)
RiskFreeReturn['RFR_Compound'] = (1+RiskFreeReturn[RiskFreeProxy]).cumprod() -1
PriceLogReturn = Dataset1[['Year','FirmID']]

InpReturnHorizonYears = pd.DataFrame([1,2,3,4,5])
InpReturnHorizonYears.columns = ['ReturnHorizonYears']

for ii in range(InpReturnHorizonYears.shape[0]): #Create returns, BPIndex, SIZEIndex, RiskFreeReturn and MarketReturn
    tmpReturnHorizonYears = InpReturnHorizonYears['ReturnHorizonYears'][ii]
    print(tmpReturnHorizonYears)
    tmpDF = PriceLogReturnMatrix
    tmpDF = tmpDF.fillna(0) #replace nan with 0 to be able to calculate yearly return
    if tmpReturnHorizonYears > 1:
        tmpDF1 = pd.DataFrame(np.array((1+ tmpDF.shift(-tmpReturnHorizonYears))/(1+tmpDF)-1)) #calculate forward looking return, to calculate for BBBEE score in year 0 what was return from year 0 to year tmpReturnHorizonYears    
        RiskFreeReturn['RiskFreeReturn_YR'+str(tmpReturnHorizonYears)] = pd.DataFrame(np.array(((1+ RiskFreeReturn['RFR_Compound'].shift(-(tmpReturnHorizonYears-1)))/(1+RiskFreeReturn['RFR_Compound']))-1)) #calculate compound riskfree return over tmpReturnHorizonYears
        RiskFreeReturn['RiskFreeReturn_YR'+str(tmpReturnHorizonYears)] = pd.DataFrame(np.array((1+RiskFreeReturn[RiskFreeProxy])*(1+RiskFreeReturn['RiskFreeReturn_YR'+str(tmpReturnHorizonYears)])-1)) #add return on the year to have the riskfreereturn for tmpReturnHorizonYears
    else:    
        tmpDF1 = pd.DataFrame(np.array((1+tmpDF)/(1+ tmpDF.shift(tmpReturnHorizonYears))-1))
        RiskFreeReturn['RiskFreeReturn_YR'+str(tmpReturnHorizonYears)] = pd.DataFrame(np.array((1+RiskFreeReturn['RFR_Compound'])/(1+ RiskFreeReturn['RFR_Compound'].shift(tmpReturnHorizonYears))-1))      
    tmpDF2 = Functions.StackDFDS_simple(tmpDF1,str('PriceLogReturn_YR'+str(tmpReturnHorizonYears)))
    tmpDF2.columns = [str('PriceLogReturn_YR'+str(tmpReturnHorizonYears)),'FirmID','YearIndex']
    tmpDF2 = pd.merge(tmpDF2,tmpYear,on='YearIndex',how='left')
    tmpDF2 = tmpDF2[[str('PriceLogReturn_YR'+str(tmpReturnHorizonYears)),'FirmID','Year']]
    PriceLogReturn = pd.merge(PriceLogReturn,tmpDF2,on=['Year','FirmID'],how='left')    
    MarketReturn['MarketReturn_YR'+str(tmpReturnHorizonYears)] = np.nanmean(np.array(tmpDF1), axis=1) #calculate market return over time horizon    
    MarketPremium['MarketPremium_YR'+str(tmpReturnHorizonYears)] = MarketReturn['MarketReturn_YR'+str(tmpReturnHorizonYears)] - RiskFreeReturn['RiskFreeReturn_YR'+str(tmpReturnHorizonYears)]
    tmpBPTop = pd.DataFrame(np.array(BPTopDummy)*np.array(tmpDF1))
    tmpBPTop = pd.DataFrame(np.nanmean(tmpBPTop, axis=1))
    tmpBPBottom = pd.DataFrame(np.array(BPBottomDummy)*np.array(tmpDF1))
    tmpBPBottom = pd.DataFrame(np.nanmean(tmpBPBottom, axis=1))    
    BPIndex['BPIndex_YR'+str(tmpReturnHorizonYears)] = tmpBPTop - tmpBPBottom
    tmpSIZETop = pd.DataFrame(np.array(SIZETopDummy)*np.array(tmpDF1))
    tmpSIZETop = pd.DataFrame(np.nanmean(tmpSIZETop, axis=1))
    tmpSIZEBottom = pd.DataFrame(np.array(SIZEBottomDummy)*np.array(tmpDF1))
    tmpSIZEBottom = pd.DataFrame(np.nanmean(tmpSIZEBottom, axis=1))
    SIZEIndex['SIZEIndex_YR'+str(tmpReturnHorizonYears)] = tmpSIZETop - tmpSIZEBottom
    del tmpDF, tmpDF1, tmpDF2, tmpBPTop, tmpBPBottom, tmpSIZETop, tmpSIZEBottom

del ii
#Adjust BBBEE Rank data
YearBBBEECount= Dataset1[['BBBEE_Rank','Year']].groupby('Year').count()
YearBBBEECount.loc[(YearBBBEECount['BBBEE_Rank']==0)] = float('nan')
MinBBBEECount = round(YearBBBEECount['BBBEE_Rank'].min(),-1) # find lowest number of observations BBBEE Rank rounded on nearest 10
tmpCleanRank = Dataset1.groupby('Year')['BBBEE_Rank'].rank(ascending=True) #rerank BBBEE based on year
Dataset1['BBBEE_Rank_Clean'] = tmpCleanRank
Dataset1.loc[(Dataset1['BBBEE_Rank_Clean'] > MinBBBEECount),'BBBEE_Rank_Clean'] = float('nan')
del tmpCleanRank

#Add industry dummy
tmpSECTORID = StagingFirm[['DS_SECTORNAME','DS_SECTORID']].drop_duplicates()
tmpSECTORID = tmpSECTORID.reset_index(drop=True)
SectorDummy = Dataset1[['Year','FirmID','DS_SECTORID']]
for ii in range(tmpSECTORID.shape[0]): #create a dummy variable name for all DS_SECTORS
    tmpColumnName = str('DSDUMMY_' + str(tmpSECTORID['DS_SECTORNAME'][ii]))
    SectorDummy[str(tmpColumnName)] = np.zeros(shape=(SectorDummy.shape[0],1))
    SectorDummy.loc[(SectorDummy['DS_SECTORID'] == tmpSECTORID['DS_SECTORID'][ii]),str(tmpColumnName)] = 1

del tmpSECTORID

SectorDummy = SectorDummy.drop(['DS_SECTORID'], axis=1)

#Finalize Variables
Dataset1 = Dataset1.loc[(Dataset1['Year']>=BBBEEStartYear)] #finalize Dataset1
SectorDummy = SectorDummy.loc[(SectorDummy['Year']>=BBBEEStartYear)] #finalize SectorDummy
BPIndex = BPIndex.loc[(BPIndex['Year']>=BBBEEStartYear)] #finalize BPIndex
SIZEIndex = SIZEIndex.loc[(SIZEIndex['Year']>=BBBEEStartYear)] #finalize BPIndex
MarketPremium = MarketPremium.loc[(MarketPremium['Year']>=BBBEEStartYear)] #finalize MarketPremium
MarketReturn = MarketReturn.loc[(MarketReturn['Year']>=BBBEEStartYear)] #finalize MarketReturn
RiskFreeReturn = RiskFreeReturn.loc[(RiskFreeReturn['Year']>=BBBEEStartYear)] #finalize RiskFreeReturn
PriceLogReturn = PriceLogReturn.loc[(PriceLogReturn['Year']>=BBBEEStartYear)] 

#Run Descriptives over different time horizons - describe
Describe1 = pd.DataFrame(Dataset1['FirmID'].describe())
Describe1 = Functions.DescribeExNaN(Dataset1,'BP',Describe1)
Describe1 = Functions.DescribeExNaN(Dataset1,'SIZE',Describe1)
Describe1 = pd.DataFrame(Functions.DescribeMultipleYearsFactor(BPIndex,'BPIndex_YR',InpReturnHorizonYears,'ReturnHorizonYears',Describe1))
Describe1 = pd.DataFrame(Functions.DescribeMultipleYearsFactor(SIZEIndex,'SIZEIndex_YR',InpReturnHorizonYears,'ReturnHorizonYears',Describe1))
Describe1 = pd.DataFrame(Functions.DescribeMultipleYearsFactor(MarketPremium,'MarketPremium_YR',InpReturnHorizonYears,'ReturnHorizonYears',Describe1))
Describe1 = pd.DataFrame(Functions.DescribeMultipleYearsFactor(MarketReturn,'MarketReturn_YR',InpReturnHorizonYears,'ReturnHorizonYears',Describe1))
Describe1 = pd.DataFrame(Functions.DescribeMultipleYearsFactor(RiskFreeReturn,'RiskFreeReturn_YR',InpReturnHorizonYears,'ReturnHorizonYears',Describe1))
Describe1 = pd.DataFrame(Functions.DescribeMultipleYearsFactor(PriceLogReturn,'PriceLogReturn_YR',InpReturnHorizonYears,'ReturnHorizonYears',Describe1))
Describe1.to_excel(ExportDir + 'Descriptives.xlsx', sheet_name='Input')

#Run Descriptives over different time horizons - describe, scatterplots, correlation matrix
InpReturnHorizonYears = pd.DataFrame([1,2,3,4,5])
InputYearsColumn = 'ReturnHorizonYears'
InputYears = InpReturnHorizonYears
OutputSet = Dataset1[['Year','FirmID','BP','SIZE','BBBEE_Rank_Clean']]
for ii in range(InputYears.shape[0]):
    tmpOutput = pd.merge(Dataset1[['Year','FirmID','BP','SIZE','BBBEE_Rank_Clean']],BPIndex[['Year',str('BPIndex_YR'+ str(InputYears[InputYearsColumn][ii]))]],on='Year',how='left')
    tmpOutput = pd.merge(tmpOutput,SIZEIndex[['Year',str('SIZEIndex_YR'+ str(InputYears[InputYearsColumn][ii]))]],on='Year',how='left')
    tmpOutput = pd.merge(tmpOutput,MarketPremium[['Year',str('MarketPremium_YR'+ str(InputYears[InputYearsColumn][ii]))]],on='Year',how='left')
    tmpOutput = pd.merge(tmpOutput,RiskFreeReturn[['Year',str('RiskFreeReturn_YR'+ str(InputYears[InputYearsColumn][ii]))]],on='Year',how='left')
    tmpOutput = pd.merge(tmpOutput,PriceLogReturn[['Year','FirmID',str('PriceLogReturn_YR'+ str(InputYears[InputYearsColumn][ii]))]],on=['Year','FirmID'],how='left')
    #describe scatterplots, correlation matrix
    
    #Run Regression over different time horizons - simple bp, size, bpIndex, sizeIndex and 
    



#Adjust for outliers
 




"""


    Inpdef DescribeMultipleYearsFactor(InputDataFrame,InputColumnPrefix,InputYears,InputYearsColumn,InputMainDataFrame):utDataFrame = BPIndex
    InputColumnPrefix = 'BPIndex_YR'
    InputYears = InpReturnHorizonYears
    InputYearsColumn = 'ReturnHorizonYears'
    InputMainDataFrame = Describe1

InputDataFrame = BPIndex
InputColumnPrefix = 'BPIndex_YR'
InputYears = InpReturnHorizonYears
InputMainDataFrame = Describe1
for ii in range(InputYears.shape[0]):
    print(InputYears['ReturnHorizonYears'][ii])
    tmpColumnName = str(InputColumnPrefix+str(InputYears['ReturnHorizonYears'][ii]))
    Describe1 = Functions.DescribeExNaN(InputDataFrame,tmpColumnName,Describe1)
    




a = z.count('BP')
x = np.array(np.char.find(z, 'BP') != -1)
Describe1 = Functions.DescribeExNaN(BPIndex,'BPIndex_YR2',Describe1)
[s for s in list if sub in s]


if any(item in z for item in 'BP'):
    print(item)
    


InputDataFrame = BPIndex
InputColumn = 'BPIndex_YR2'
InputMainDataFrame = Describe1
tmpDF = pd.DataFrame(InputDataFrame[InputColumn].dropna())
tmpDescribe = pd.DataFrame(tmpDF.describe())
tmpOutput = pd.merge(InputMainDataFrame,tmpDescribe,how='left',left_index=True, right_index=True)

pd.merge(Describe1,Functions.DescribeExNaN(BPIndex,'BPIndex_YR2'),how='left',left_index=True, right_index=True)

tmpDF = pd.DataFrame(BPIndex['BPIndex_YR2'].dropna())
tmpDescribe = pd.DataFrame(tmpDF.describe())


Dataset1 = Dataset1.loc[(Dataset1['Year']>-BBBEEStartYear)] #finalize Dataset1

#STOPPED HERE
RiskFreeReturn['RiskFreeReturn_YR'+str(tmpReturnHorizonYears)] = pd.DataFrame(np.array((1+RiskFreeReturn['RFR_Compound'])/(1+ RiskFreeReturn['RFR_Compound'].shift(tmpReturnHorizonYears))-1))


tmpMarketReturn['MarketReturn_YR'+str(tmpReturnHorizonYears)] = np.nanmean(np.array(tmpDF1), axis=1)


y = y.fillna(0) #replace nan with 0 to be able to calculate yearly return

YearlyLogReturn = pd.DataFrame(np.array((1+y)/(1+ y.shift(1))-1)) #calculate yearly return
MarketLogReturn = np.nanmean(np.array(YearlyLogReturn), axis=1) #market return is the average return of all observations
YearlyLogReturn[YearlyLogReturn == 0] = float('nan')
FirmLogReturn = pd.DataFrame(YearlyLogReturn.stack())
FirmLogReturn.columns = ['FirmLogReturn']
FirmLogReturn['Dataset0Index'] = FirmLogReturn.index.get_level_values(0)
FirmLogReturn['FirmID'] = FirmLogReturn.index.get_level_values(1)
y = y.reset_index() #get year as column
y['Dataset0Index'] = y.index.get_level_values(0) #get row id as column
tmpYear = y[['Year','Dataset0Index']] # established mapping year and row id

FirmLogReturn = pd.merge(FirmLogReturn,tmpYear,on='Dataset0Index',how='left')
RiskFreeReturn = Dataset0[['Year','SA10YR']]
RiskFreeReturn = RiskFreeReturn.drop_duplicates()
RiskFreeReturn['SA10YR'] = RiskFreeReturn['SA10YR']/100
RiskFreeReturn.columns = ['Year','RiskFreeReturn']
MarketPremium = MarketLogReturn - RiskFreeReturn['RiskFreeReturn']
MarketPremium = pd.DataFrame(MarketPremium)
MarketPremium = MarketPremium.reset_index(drop=True)
MarketPremium['Year'] = tmpYear['Year']
MarketPremium.columns = ['MarketPremium','Year']
MarketLogReturn = pd.DataFrame(MarketLogReturn)
MarketLogReturn = MarketLogReturn.reset_index(drop=True)
MarketLogReturn['Year'] = tmpYear['Year']
MarketLogReturn.columns = ['MarketLogReturn','Year']


Dataset1 = Dataset0[['Year','FirmID','FirmLogReturn','BP','SIZE','BBBEE_Rank']]
Dataset1 = pd.merge(Dataset1,FirmLogReturn,on=['Year','FirmID'],how='left')
Dataset1 = pd.merge(Dataset1, MarketPremium, on='Year',how='left')
Dataset1 = pd.merge(Dataset1, RiskFreeReturn, on='Year',how='left')
Dataset1 = Dataset1.loc[(Dataset1['Year']>=BBBEEStartYear)]
Dataset1.to_excel(ExportDir + 'Dataset.xlsx', sheet_name='Input') 

# Create Dataset2 which has BBBEE rank adjusted data
Dataset2 = Dataset1 
ObsSectorYearCount2 = Dataset2.groupby('Year').count()
MinBBBEECount = round(ObsSectorYearCount2['BBBEE_Rank'].min(),-1) # find lowest number of observations BBBEE Rank rounded on nearest 10
tmpDataset2Years = Dataset2['Year'].unique()
tmpCleanRank = Dataset2.groupby('Year')['BBBEE_Rank'].rank(ascending=True) #rerank BBBEE based on year
Dataset2['BBBEE_Rank'] = tmpCleanRank
Dataset2.loc[(Dataset2['BBBEE_Rank'] > MinBBBEECount),'BBBEE_Rank'] = float('nan')
ObsVariableYearCount3 = Dataset2.groupby('Year').count()
Dataset2.to_excel(ExportDir + 'Dataset2.xlsx', sheet_name='Input') 
OverviewIncNaN = Dataset2.describe(include = [np.number]) #pre outlier overview

# Create Dataset3 which has outlier adjusted data
Dataset3 = Dataset2[['Year','FirmID','BBBEE_Rank','RiskFreeReturn','MarketPremium']]
Dataset3 = Dataset3.reset_index(drop=True)
tmpColumns = ['FirmLogReturn','BP','SIZE']

for ii in range(len(tmpColumns)):
    inpColumn = str(tmpColumns[ii])
    tmpDF1 = Functions.CapOutliers(Dataset2,inpColumn)
    Dataset3 = pd.merge(Dataset3,tmpDF1,on=['Year','FirmID'],how='left')
    
Dataset3.to_excel(ExportDir + 'Dataset3.xlsx', sheet_name='Input') 

#Rerun Factor Deciles


##NOTES FROM HERE BELOW

    InputDataFrame = Dataset2
    InputColumn = inpColumn
    
    
    tmpDF = InputDataFrame[['Year','FirmID',InputColumn]]
    tmpDF = tmpDF.dropna()
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
   
    
    tmpDF1 = tmpDF
    tmpDF1 = tmpDF1.reset_index(drop=True)
    
    tmpDF1 = Functions.CapOutliers(Dataset2,inpColumn)
    Dataset3 = pd.merge(Dataset3,tmpDF1,on=['Year','FirmID'],how='left')

    tmpDF2 = tmpDF1.drop_duplicates()
    Dataset3 = pd.merge(Dataset3,tmpDF2,on=['Year','FirmID'],how='left')
    
    
#cap outliers - nans
import seaborn as sns
import pandas as pd
import os
InputColumn = 'FirmLogReturn'
InputDataframe = Dataset2
MainDir = os.path.dirname(os.path.realpath(__file__)) #working directory
ExportDir = MainDir + '\\Output\\Descriptives\\' 

tmpDF = InputDataframe[['Year','FirmID',InputColumn]]
tmpDF = tmpDF.dropna()
tmpPreOutlierOverview = tmpDF.describe()
tmpPreOutlierPlot = sns.distplot(np.array(tmpDF[InputColumn]))  # getDistribution of InputColumn
tmpPreOutlierPlot = tmpPreOutlierPlot.get_figure() #save distribution of InputColumn
tmpPreOutlierPlot.savefig(ExportDir + InputColumn + '_PreOutlierDistribution.png')
minValue = tmpPreOutlierOverview[InputColumn]['mean'] - (2*tmpPreOutlierOverview[InputColumn]['std']) #handle outlier minimum
maxValue = tmpPreOutlierOverview[InputColumn]['mean'] + (2*tmpPreOutlierOverview[InputColumn]['std']) #handle outlier max
tmpDF.loc[(tmpDF[InputColumn]<=minValue)] = minValue
tmpDF.loc[(tmpDF[InputColumn]>=maxValue)] = maxValue
    


tmpFirmReturn = Dataset[['Year','FirmID','FirmLogReturn']] 
tmpFirmReturn = tmpFirmReturn.dropna()
OverviewFirmReturn = tmpFirmReturn.describe()
Distribution_FirmLogReturn = sns.distplot(np.array(tmpFirmReturn['FirmLogReturn']))  # getDistribution of FirmReturn
Distribution_FirmLogReturn = Distribution_FirmLogReturn.get_figure() #save distribution of FirmReturn
Distribution_FirmLogReturn.savefig(ExportDir + 'Distribution_FirmLogReturn.png')             #save distribution of FirmReturn



tmpBP = Dataset[['Year','FirmID','BP']] #add new tmpBP to new dataset
tmpBP = tmpBP.dropna()
OverviewBP = tmpBP.describe()
Distribution_BP = sns.distplot(np.array(tmpBP['BP']))  # getDistribution of FirmReturn
Distribution_BP = Distribution_BP.get_figure() #save distribution of FirmReturn
Distribution_BP.savefig(ExportDir + 'Distribution_BP.png')             #save distribution of FirmReturn
minBP = OverviewBP['BP']['mean'] - (2*OverviewBP['BP']['std']) #handle outlier minimum
maxBP = OverviewBP['BP']['mean'] + (2*OverviewBP['BP']['std']) #handle outlier max
tmpBP.loc[(tmpBP['BP']<=minBP)] = minBP
tmpBP.loc[(tmpBP['BP']>=maxBP)] = maxBP
OverviewBP2 = tmpBP.describe()
minFirmReturn = OverviewFirmReturn['FirmLogReturn']['25%'] #handle outlier minimum
maxFirmReturn = OverviewFirmReturn['FirmLogReturn']['75%'] #handle outlier minimum

#NEXT STEPS: CREATE A FUNCTION FOR CREATING DESCRIBES AND HANDLE OUTLIERS




#notes
df2[cols] = df2[cols].replace({'0':np.nan, 0:np.nan}
z = pd.DataFrame(df.columns)
p = df.loc[(df['FirmID'] == 5)]
np.mean(a, axis=0)
x = np.array(YearlyLogReturn)
z = 

x = YearlyLogReturn.groupby('Year').mean()
x = pd.DataFrame(YearlyLogReturn.mean())
x = df.describe()

1. Define BBBEE lag - DONE

2. Create yearly dataset
in stagingdates where monthend and month are condition fill year then BBBEE left join based on that to DS
calculate annual return ds based on this - DONE

3. Check number of firms with BBBEE score/without BBBEE score per year
x = ObsV.describe()

4. Check number of firms per sector per year - compare with JSE All share Index




#StagingDates['BBBEEReleaseDateDummy'] = StagingDates['BBBEEReleaseDateDummy'].replace(to_replace=0, method='ffill') #fill BBBEEReleaseDateDummy with value till next year end - completing year id for each date
#StagingDates['ReturnYearEndDummy'] = StagingDates['ReturnYearEndDummy'].replace(to_replace=0, method='ffill') #fill YearEnd with value till next year end - completing year id for each date

df = pd.merge(StagingBBBEE,StagingDS,on=['Year','FirmID'], how='left')
StagingBBBEE = pd.merge(StagingBBBEE,StagingDS,on=['Year','FirmID'], how='left')



StagingBBBEE = pd.merge(RawBBBEE,tmpFIRMBBBEE,on='CompanyName', how='left')

     







    
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Initiate Staging Data')
import StagingData
StagingData = StagingData.StagingData
tmpKeyData = pd.DataFrame(StagingData[['Year','LogPriceReturn', 'BBBEE_Rank','BVPS','MV']]) #Add yields eventually
FinalData = tmpKeyData.dropna() #removed all nan - this is the cleanest set of data

#DESCRIPTIVES PER YEAR
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Compute Descriptives per factor table aggregate')
DescriptiveLogPriceReturnTotalSample = FinalData['LogPriceReturn'].describe()
DescriptiveBEERankTotalSample = FinalData['BBBEE_Rank'].describe()
DescriptiveBVPSTotalSample = FinalData['BVPS'].describe()
DescriptiveMVTotalSample = FinalData['MV'].describe() 

#DESCRIPTIVES PER YEAR
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Compute Descriptives per factor table per year')
df = FinalData[['Year','LogPriceReturn']]
DescriptiveLogPriceReturnYear = df.groupby('Year').describe()
df = FinalData[['Year','BBBEE_Rank']]
DescriptiveBBBEERankYear = df.groupby('Year').describe()
df = FinalData[['Year','BVPS']]
DescriptiveBVPSYear = df.groupby('Year').describe()
df = FinalData[['Year','MV']]
DescriptiveBVPSYear = df.groupby('Year').describe()

#Create ranks LogPriceReturn, BVPS and MV


UniqueYears = pd.DataFrame(FinalData['Year'].unique())#get unique years to get ranks per year
UniqueYears.columns = ['Year']



RankData = pd.DataFrame(np.nan, index=[0], columns=['Year','LogPriceReturn_Rank','BBBEE_Rank','BVPS_Rank','MV_Rank']) #create an empty dataframe 

#call function that loops through grouping and to be ranked columns

tmpYear = UniqueYears['Year'][0]
df = FinalData.loc[FinalData['Year'] == int(tmpYear), ['Year','BBBEE_Rank','LogPriceReturn','BVPS','MV',]]
df['LogPriceReturn_Rank'] = df['LogPriceReturn'].rank() 
df['BVPS_Rank'] = df['BVPS'].rank() 
df['MV_Rank'] = df['MV'].rank() 
df2 = df[['Year','LogPriceReturn_Rank','BBBEE_Rank','BVPS_Rank','MV_Rank']].copy()

RankData = RankData.append(df2)




#SCATTERPLOTS
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Run Scatterplots')
ScatterPlotContainer = plt.figure()
ScatterPlot1 = ScatterPlotContainer.add_subplot(2,2,1)
ScatterPlot2 = ScatterPlotContainer.add_subplot(2,2,2)
ScatterPlot3 = ScatterPlotContainer.add_subplot(2,2,3)
                                                
ScatterPlot1.scatter('BBBEE_Rank','LogPriceReturn',data=FinalData)
ScatterPlot2.scatter('MV','LogPriceReturn',data=FinalData)
ScatterPlot3.scatter('BVPS','LogPriceReturn',data=FinalData)

#OUTPUT
MainDir = os.path.dirname(os.path.realpath(__file__))
ExportDir = MainDir + '\\Output\\Descriptives\\' 
FinalData.to_csv(ExportDir + 'FinalData.csv', encoding='utf-8', index=False)
plt.savefig(ExportDir + 'ScatterPlotPriceReturnVsFactors.pdf')
"""
