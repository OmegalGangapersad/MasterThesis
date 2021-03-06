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
                - Finished descriptives (describe) and regression per year
                - Cleaned code
            20190504:
                - Finished regressions methodology and non outlier adjusted models
                - Flipped BBBEE Rank - so that higher BBBEE rank is better BBBEE. This improves intuition in regression and scatterplot
                - Abstracted regression for 5 years into function Regression5YearOutput
                - Created Abstracted scatterplots into PriceLogScatterplots
                - Started with outlier adjusted analysis
                - Finished outlier adjusted analysis
                - Cleaned code
            20190505:
                - Added comparison to JSE sector
            20190506:
                - Remove #remove all rows with no BBBEE value
                - Removed duplicates from regression analysis tmpOutput dataset
            20190508:
                - Changed Simple to MF (Merwe Ferreira model)
                - Added EP factor to MF
                - Removed Marketpremium and Riskfree rate from MF
                - Adjusted Size index to small minus big, instead of big minus small
            20190512:
                - Changed Export directory to allow for different lags
                - Removed values exactly zero, error as result of true false dummies
                - Started bootstrap inspired by Mehta and Ward
                - Finished bootstrap 
            20190515:
                - Rewrote bootstrap to account for all sectors
            20190526:
                - Renamed variables 
            20190528:
                - Renamed variables (ratio)
                - Reactivated bootstrap and installed nested regression
            20190603:
                - Cleaned code
                - Started regressions per sector Dataset1
                - Finished regressions per sector Dataset1
            20190604:
                - Latest statmodels requires !pip install git+https://github.com/statsmodels/statsmodels.git@master for export to latex to work
            20190608:
                - Added persistency checks
"""

##START SCRIPT
import os
import Functions
import datetime
import numpy as np
import pandas as pd
from numpy import matlib as mb
import matplotlib.pyplot as plt
import seaborn as sns
import statsmodels.api as sm
from statsmodels.iolib.summary2 import summary_col
tmpScriptName = os.path.basename(__file__)
tmpStartTimeScript = datetime.datetime.now()
tmpScriptName = os.path.basename(__file__)
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Start Script')

# DEFINE DF CRITERIA - CAN BE LOOPED OVER
BBBEEMonth = 4 #define month where BBBEE is released - April is value from van der Merwe and Ferreira paper
BBBEELag = 4 #define months Lag from BBBEE release Month - you can adjust this value to check when market reacts - Merwe indicates 4 months
MonthYearEnd = BBBEEMonth + BBBEELag #define month for year end to calculate
BBBEEStartYear = 2004

MainDir = os.path.dirname(os.path.realpath(__file__)) #working directory
ExportDir = MainDir + '\\Output\\LagMonths_' + str(BBBEELag) + '\\'
##CHECK FOR EXISTENCE OUTPUT FOLDER   
try:    
    try:
        os.mkdir(MainDir + '\\Output\\')
        os.mkdir(ExportDir)
    except:
        os.mkdir(ExportDir)            
    Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Created Input StagingData folder') 
except:
    Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Input StagingData folder exists')         


##READ STAGING DATA
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Start STEP1: Read StagingData') 
try:
    InputFilenameBBBEE = 'BBBEE.csv'
    InputFilenameDS = 'DS.csv'
    InputFilenameFirm = 'Firm.csv'
    InputFilenameDates = 'Dates.csv'  
    InputFilenameJSE = 'JSE.csv'      
    tmpMainDir = os.path.dirname(os.path.realpath(__file__)) #working directory
    tmpImportDirectory = tmpMainDir + '\\Input\\StagingData\\' 
    StagingBBBEE = pd.read_csv(tmpImportDirectory + InputFilenameBBBEE)
    StagingDS = pd.read_csv(tmpImportDirectory + InputFilenameDS)
    StagingFirm = pd.read_csv(tmpImportDirectory + InputFilenameFirm)
    StagingDates = pd.read_csv(tmpImportDirectory + InputFilenameDates)
    StagingJSE = pd.read_csv(tmpImportDirectory + InputFilenameJSE)
    del InputFilenameBBBEE, InputFilenameDS, InputFilenameFirm, InputFilenameDates, tmpMainDir, tmpImportDirectory
except:
    import StagingData
Functions.LogScript(tmpScriptName,datetime.datetime.now(),'Start STEP2: Create working dataset called df') 

# Create Dataset0 rawest data, grouped by year
StagingDates.loc[(StagingDates['Month'] == BBBEEMonth) & (StagingDates['MonthEndDummy'] == 1),'BBBEEReleaseDateDummy'] = StagingDates.loc[(StagingDates['Month'] == BBBEEMonth) & (StagingDates['MonthEndDummy'] == 1),'Year'] #put year in BBBEEdummy to identify from where BBBEE scores run
StagingDates.loc[(StagingDates['Month'] == MonthYearEnd) & (StagingDates['MonthEndDummy'] == 1),'ReturnYearEndDummy'] = StagingDates.loc[(StagingDates['Month'] == MonthYearEnd) & (StagingDates['MonthEndDummy'] == 1),'Year'] #put year in BBBEEdummy to identify from where Return year end run
tmpYear = StagingDates[['DateID','ReturnYearEndDummy']]
tmpYear.columns = ['DateID','Year'] 
StagingDS = pd.merge(StagingDS,pd.DataFrame(tmpYear),on='DateID',how='left') #create column with ReturnYearEndDummy to merge with StagingBBBEE
Dataset0 = StagingDS.loc[StagingDS['Year']!=0] #this will return more rows than StagingBBBEE because StagingBBBEE only capture sec year combination for which there is a BBBEE score Dataset0 captures the sec even when there is no BBBEE score
Dataset0 = pd.merge(Dataset0,StagingBBBEE,on=['Year','FirmID'], how='left')
Dataset0.rename(columns={'BM':'BMRatio', 'SIZE':'SIZERatio', 'EP':'EPRatio'}, inplace=True)

ObsVariableYear = Dataset0.groupby('Year').count() #Check number of firms with BBBEE score/without BBBEE score per year
ObsSectorYearCount = Dataset0.pivot_table(['BBBEE_Rank','Price'], index='Year', columns='DS_SECTORID', aggfunc='count') #Check number of firms per sector per year - compare with JSE All share Index - Price as proxy for all observations

try:
    ObsVariableYear.to_excel(ExportDir + 'ObsVariableYear.xlsx', sheet_name='Input')
except:     
    os.mkdir(ExportDir) # only create sub directory
        
ObsVariableYear.to_excel(ExportDir + 'ObsVariableYear.xlsx', sheet_name='Input')    
ObsSectorYearCount.to_excel(ExportDir + 'ObsSectorYearCount.xlsx', sheet_name='Input') #work in excel to create %percentages

# Create Dataset1 which has returns, market premium and riskfree rate starting from BBBEEStartYear
Dataset1 = Dataset0.loc[(Dataset0['Year']>=(BBBEEStartYear-1))]

SharePriceReturnMatrix = Dataset1.pivot_table('PriceLogReturnCum', index='Year', columns='FirmID')
tmpYear = pd.DataFrame(SharePriceReturnMatrix.index.get_level_values(0))
tmpYear['YearIndex'] = tmpYear.index.get_level_values(0)

# FF BM and SIZE top 30% Dummy
FactorDecile = pd.DataFrame(['SIZERatio','BMRatio'])
FactorDecile.columns = ['Factors']

for ii in range(FactorDecile.shape[0]):    
    inpFactor = FactorDecile['Factors'][ii]
    FactorDF = pd.DataFrame(columns=['FirmID','Year',str(inpFactor+'_Decile')])
    for jj in range(tmpYear.shape[0]):
        inpYear = int(tmpYear['Year'][jj])
        tmpFactorDF = Dataset1.loc[(Dataset1['Year']==inpYear),['FirmID','Year',inpFactor]]
        tmpFactorDF[str(inpFactor+'_Decile')] = pd.qcut(tmpFactorDF[inpFactor],10,labels=False)
        tmpFactorDF = tmpFactorDF.drop_duplicates()
        tmpFactorDF = tmpFactorDF[['FirmID','Year',str(inpFactor+'_Decile')]]
        tmpFactorDF = tmpFactorDF.astype(float)
        FactorDF = pd.concat([FactorDF, tmpFactorDF])    
        del inpYear, tmpFactorDF
    Dataset1 = pd.merge(Dataset1,FactorDF,on=['FirmID','Year'],how='left')
    del inpFactor, FactorDF

del ii,jj 

#Dataset1.loc[(Dataset1['FirmID']==14) & (Dataset1['Year']==2018),'BMRatio_Decile'] = 5
BMMatrix = Dataset1.pivot_table('BMRatio_Decile', index='Year', columns='FirmID', dropna=False)
BMTopDummy = ([BMMatrix <=2]) #FF use top and bottom 30% see http://mba.tuck.dartmouth.edu/pages/faculty/ken.french/Data_Library/det_port_form_sz.html
BMTopDummy = BMTopDummy[0]
BMBottomDummy = ([BMMatrix >=7])
BMBottomDummy = BMBottomDummy[0]
SIZEMatrix = Dataset1.pivot_table('SIZERatio_Decile', index='Year', columns='FirmID')
SIZETopDummy = ([SIZEMatrix <=2])
SIZETopDummy = SIZETopDummy[0]
SIZEBottomDummy = ([SIZEMatrix >=7])
SIZEBottomDummy = SIZEBottomDummy[0]

MarketReturn = tmpYear[['Year','YearIndex']]
MarketPremium = tmpYear[['Year','YearIndex']]
BMIndex = tmpYear[['Year','YearIndex']]
SIZEIndex = tmpYear[['Year','YearIndex']]
RiskFreeProxy = 'SA10YR'
RiskFreeReturn = Dataset1[['Year',RiskFreeProxy]]
RiskFreeReturn = RiskFreeReturn.drop_duplicates()
RiskFreeReturn[RiskFreeProxy] = RiskFreeReturn[RiskFreeProxy]/100
RiskFreeReturn = RiskFreeReturn.reset_index(drop=True)
RiskFreeReturn['RFR_Compound'] = (1+RiskFreeReturn[RiskFreeProxy]).cumprod() -1
SharePriceReturn = Dataset1[['Year','FirmID']]

InpReturnHorizonYears = pd.DataFrame([1,2,3,4,5])
InpReturnHorizonYears.columns = ['ReturnHorizonYears']

for ii in range(InpReturnHorizonYears.shape[0]): #Create returns, BMIndex, SIZEIndex, RiskFreeReturn and MarketReturn
    tmpReturnHorizonYears = InpReturnHorizonYears['ReturnHorizonYears'][ii]
    tmpDF = SharePriceReturnMatrix
    tmpDF = tmpDF.fillna(0) #replace nan with 0 to be able to calculate yearly return
    if tmpReturnHorizonYears > 1:
        tmpDF1 = pd.DataFrame(np.array((1+ tmpDF.shift(-tmpReturnHorizonYears))/(1+tmpDF)-1)) #calculate forward looking return, to calculate for BBBEE score in year 0 what was return from year 0 to year tmpReturnHorizonYears    
        RiskFreeReturn['RiskFreeReturn_YR'+str(tmpReturnHorizonYears)] = pd.DataFrame(np.array(((1+ RiskFreeReturn['RFR_Compound'].shift(-(tmpReturnHorizonYears-1)))/(1+RiskFreeReturn['RFR_Compound']))-1)) #calculate compound riskfree return over tmpReturnHorizonYears
        RiskFreeReturn['RiskFreeReturn_YR'+str(tmpReturnHorizonYears)] = pd.DataFrame(np.array((1+RiskFreeReturn[RiskFreeProxy])*(1+RiskFreeReturn['RiskFreeReturn_YR'+str(tmpReturnHorizonYears)])-1)) #add return on the year to have the riskfreereturn for tmpReturnHorizonYears
    else:    
        tmpDF1 = pd.DataFrame(np.array((1+tmpDF)/(1+ tmpDF.shift(tmpReturnHorizonYears))-1))
        RiskFreeReturn['RiskFreeReturn_YR'+str(tmpReturnHorizonYears)] = pd.DataFrame(np.array((1+RiskFreeReturn['RFR_Compound'])/(1+ RiskFreeReturn['RFR_Compound'].shift(tmpReturnHorizonYears))-1))      
    tmpDF2 = Functions.StackDFDS_simple(tmpDF1,str('SharePriceReturn_YR'+str(tmpReturnHorizonYears)))
    tmpDF2.columns = [str('SharePriceReturn_YR'+str(tmpReturnHorizonYears)),'FirmID','YearIndex']
    tmpDF2 = pd.merge(tmpDF2,tmpYear,on='YearIndex',how='left')
    tmpDF2 = tmpDF2[[str('SharePriceReturn_YR'+str(tmpReturnHorizonYears)),'FirmID','Year']]
    SharePriceReturn = pd.merge(SharePriceReturn,tmpDF2,on=['Year','FirmID'],how='left')    
    tmpDF1 = np.array(tmpDF1)
    tmpDF1[tmpDF1 == 0] = np.nan #remove values that are exactly 0, must be an error
    MarketReturn['MarketReturn_YR'+str(tmpReturnHorizonYears)] = np.nanmean(np.array(tmpDF1), axis=1) #calculate market return over time horizon    
    MarketPremium['MarketPremium_YR'+str(tmpReturnHorizonYears)] = MarketReturn['MarketReturn_YR'+str(tmpReturnHorizonYears)] - RiskFreeReturn['RiskFreeReturn_YR'+str(tmpReturnHorizonYears)]
    tmpBMTop = pd.DataFrame(np.array(BMTopDummy)*np.array(tmpDF1))
    tmpBMTop = pd.DataFrame(np.nanmean(tmpBMTop, axis=1))
    tmpBMBottom = pd.DataFrame(np.array(BMBottomDummy)*np.array(tmpDF1))
    tmpBMBottom = pd.DataFrame(np.nanmean(tmpBMBottom, axis=1))    
    BMIndex['BMIndex_YR'+str(tmpReturnHorizonYears)] = tmpBMTop - tmpBMBottom
    tmpSIZETop = pd.DataFrame(np.array(SIZETopDummy)*np.array(tmpDF1))
    tmpSIZETop = pd.DataFrame(np.nanmean(tmpSIZETop, axis=1))
    tmpSIZEBottom = pd.DataFrame(np.array(SIZEBottomDummy)*np.array(tmpDF1))
    tmpSIZEBottom = pd.DataFrame(np.nanmean(tmpSIZEBottom, axis=1))
    SIZEIndex['SIZEIndex_YR'+str(tmpReturnHorizonYears)] = tmpSIZEBottom - tmpSIZETop #Fama French Size is small minus big market cap
    del tmpDF, tmpDF1, tmpDF2, tmpBMTop, tmpBMBottom, tmpSIZETop, tmpSIZEBottom

del ii
#Adjust BBBEE Rank data
YearBBBEECount= Dataset1[['BBBEE_Rank','Year']].groupby('Year').count()
YearBBBEECount.loc[(YearBBBEECount['BBBEE_Rank']==0)] = float('nan')
tmpPlot = YearBBBEECount
tmpPlot = tmpPlot.reset_index()
Functions.SimpleLineChart('BBBEE_Rank','Year',tmpPlot,ExportDir,'Number of BBBEE ranked observations','') #plot observation of BBBEE through time
del tmpPlot
MinBBBEECount = round(YearBBBEECount['BBBEE_Rank'].min(),-1) # find lowest number of observations BBBEE Rank rounded on nearest 10
tmpCleanRank = Dataset1.groupby('Year')['BBBEE_Rank'].rank(ascending=True) #rerank BBBEE based on year 
Dataset1['BBBEE_Rank_Clean'] = tmpCleanRank
Dataset1.loc[(Dataset1['BBBEE_Rank_Clean'] > MinBBBEECount),'BBBEE_Rank_Clean'] = float('nan')
tmpCleanRank2 = Dataset1.groupby('Year')['BBBEE_Rank_Clean'].rank(ascending=False) #make BBBEE Rank higher the better - to improve interpretability in regressions
Dataset1['BBBEE_Rank_Clean'] = tmpCleanRank2
del tmpCleanRank, tmpCleanRank2

#Add industry dummy
tmpSECTORID = StagingFirm[['DS_SECTORNAME','DS_SECTORID']].drop_duplicates()
tmpSECTORID = tmpSECTORID.reset_index(drop=True)
SectorDummy = Dataset1[['Year','FirmID','DS_SECTORID']]
for ii in range(tmpSECTORID.shape[0]): #create a dummy variable name for all DS_SECTORS
    tmpColumnName = str('DSDUMMY_' + str(tmpSECTORID['DS_SECTORNAME'][ii]))
    SectorDummy[str(tmpColumnName)] = np.zeros(shape=(SectorDummy.shape[0],1))
    SectorDummy.loc[(SectorDummy['DS_SECTORID'] == tmpSECTORID['DS_SECTORID'][ii]),str(tmpColumnName)] = 1

#bootstrap method as inspired by Mehta and Ward (p94) for the entire dataset and just for Industrial sector    
tmpLoop = tmpSECTORID
tmpLoop = tmpLoop.append(pd.Series(['All', 0], index=tmpLoop.columns), ignore_index=True) #https://thispointer.com/python-pandas-how-to-add-rows-in-a-dataframe-using-dataframe-append-loc-iloc/
for ii in range(tmpLoop.shape[0]): #loop through sectors
    tmpDataset = Dataset1.loc[(Dataset1['Year']>=BBBEEStartYear),['Year','FirmID','BBBEE_Rank_Clean','DS_SECTORID']]
    tmpDataset = tmpDataset.reset_index(drop=True)
    tmpSharePriceReturn = SharePriceReturn.loc[(SharePriceReturn['Year']>=BBBEEStartYear),['Year','FirmID','SharePriceReturn_YR1']]
    tmpDataset = pd.merge(tmpDataset,tmpSharePriceReturn,on=['Year','FirmID'],how='left')
    del tmpSharePriceReturn
    tmpSectorName = tmpLoop['DS_SECTORNAME'].iloc[ii]
    tmpSectorID = tmpLoop['DS_SECTORID'].iloc[ii]
    if tmpSectorName == 'All':
        tmpTitle = 'Bootstrap_All'
    else:
        tmpDataset = tmpDataset.loc[(tmpDataset['DS_SECTORID'] == tmpSectorID)] 
        tmpDataset = tmpDataset.reset_index(drop=True)
        tmpCleanRank = tmpDataset.groupby('Year')['BBBEE_Rank_Clean'].rank(ascending=True) #rerank BBBEE based on year 
        tmpDataset['BBBEE_Rank_Clean'] = tmpCleanRank
        tmpTitle = 'Bootstrap_' + tmpSectorName
        del tmpCleanRank
    
    tmpYearsBootstrap = tmpDataset['Year'].drop_duplicates()
    tmpYearsBootstrap = tmpYearsBootstrap.reset_index(drop=True)
    tmpOutputBootstrap = pd.DataFrame(np.zeros(shape=(tmpYearsBootstrap.shape[0],5)))
    tmpOutputBootstrap.columns = ['Year','B-BBEE Top','B-BBEE Bottom','95% confidence','5% confidence']
    tmpOutputBootstrap['Year'] = tmpYearsBootstrap
    
    for jj in range(tmpOutputBootstrap.shape[0]): #loop through years
        tmpYearLoop = tmpOutputBootstrap['Year'].loc[jj]
        tmpDataset2 = tmpDataset.loc[(tmpDataset['Year'] == tmpYearLoop)] 
        tmpDataset2 = tmpDataset2.dropna()        
        tmpDatasetMaxRank = tmpDataset2['BBBEE_Rank_Clean'].max()
        if tmpDatasetMaxRank >= 2: # only calculate if there are two observations for a year
            tmpDatasetCutOff = round(np.percentile(tmpDataset2['BBBEE_Rank_Clean'], 30),0) #determine what 30% means in terms of rank
            tmpOutputBootstrap['B-BBEE Top'].loc[jj] = tmpDataset2.loc[(tmpDataset2['BBBEE_Rank_Clean']>=(tmpDatasetMaxRank - tmpDatasetCutOff)),'SharePriceReturn_YR1'].mean()
            tmpOutputBootstrap['B-BBEE Bottom'].loc[jj] = tmpDataset2.loc[(tmpDataset2['BBBEE_Rank_Clean']<=tmpDatasetCutOff),'SharePriceReturn_YR1'].mean()
            try:
                tmpLower, tmpUpper = Functions.bootstrap(tmpDataset2['SharePriceReturn_YR1'], confidence=0.95, iterations=10000, sample_size=1, statistic=np.median)           
                tmpOutputBootstrap['95% confidence'].loc[jj] = tmpUpper
                tmpOutputBootstrap['5% confidence'].loc[jj] = tmpLower
            except:
                tmpOutputBootstrap['95% confidence'].loc[jj] = 0
                tmpOutputBootstrap['5% confidence'].loc[jj] = 0
            del tmpYearLoop, tmpDataset2
            
    tmpOutputBootstrapCum = (1+tmpOutputBootstrap).cumprod() -1 # compound returns
    tmpOutputBootstrapCum['Year'] = tmpOutputBootstrap['Year']
    Functions.BootstrapLineChart(tmpOutputBootstrap,ExportDir,tmpTitle + '_Annual')
    Functions.BootstrapLineChart(tmpOutputBootstrapCum,ExportDir,tmpTitle + '_Cumulative')    
    del tmpYearsBootstrap, tmpOutputBootstrap, tmpOutputBootstrapCum, tmpTitle, tmpDataset

#compare with sector  bias with JSE
tmpSectorDataset1 = Dataset1[['FirmID','Year','DS_SECTORID','BBBEE_Rank_Clean']] # Dataset1
tmpSectorDataset1 = tmpSectorDataset1.dropna()
tmpSectorDatasetPivot = tmpSectorDataset1.pivot_table('FirmID', index='Year', columns='DS_SECTORID', aggfunc='count')
tmpSectorDatasetPivot.reset_index(drop=True)
tmpSectorDatasetPivot = tmpSectorDatasetPivot.fillna(0)
tmpYearSector = tmpSectorDataset1[['BBBEE_Rank_Clean','Year']].groupby('Year').count()
tmpYearSector = pd.DataFrame(mb.repmat(np.array(tmpYearSector), 1, tmpSectorDatasetPivot.shape[1]))
tmpSector1 = np.array(tmpSectorDatasetPivot) / np.array(tmpYearSector)

StagingJSE = pd.merge(StagingJSE,tmpSECTORID,on='DS_SECTORNAME',how='left') #calc JSE 
tmpJSE = StagingJSE[['DS_SECTORNAME','DS_SECTORID']].groupby('DS_SECTORID').count()
tmpJSETot = StagingJSE['DS_SECTORNAME'].count()
tmpJSE = tmpJSE/tmpJSETot
tmpJSE = np.transpose(tmpJSE)
tmpJSE2 = np.matlib.repmat(np.array(tmpJSE), tmpSectorDatasetPivot.shape[0],1)

tmpSectorYears = tmpSectorDatasetPivot.reset_index() #Calc diff
tmpSectorYears = tmpSectorYears[['Year']]
tmpJSEDataset1Diff = tmpSector1 - tmpJSE2 

tmpJSEDataset1Diff = pd.DataFrame(tmpJSEDataset1Diff) #prep for export
tmpJSEDataset1Diff['Year'] = tmpSectorYears
tmpJSEDataset1Diff = tmpJSEDataset1Diff.set_index('Year', drop=True)
tmpColumns = tmpSECTORID.set_index('DS_SECTORID',drop=True)
tmpColumns=tmpColumns.sort_index()
tmpColumns = list(tmpColumns['DS_SECTORNAME'])
tmpJSEDataset1Diff.columns = tmpColumns
tmpJSEDataset1Diff.to_excel(ExportDir + "RelativeDifference_JSE_Dataset.xlsx",sheet_name='Input')

del ii
SectorDummy = SectorDummy.drop(['DS_SECTORID'], axis=1)

#Finalize Variables
Dataset1 = Dataset1.loc[(Dataset1['Year']>=BBBEEStartYear)] #finalize Dataset1
SectorDummy = SectorDummy.loc[(SectorDummy['Year']>=BBBEEStartYear)] #finalize SectorDummy
BMIndex = BMIndex.loc[(BMIndex['Year']>=BBBEEStartYear)] #finalize BMIndex
SIZEIndex = SIZEIndex.loc[(SIZEIndex['Year']>=BBBEEStartYear)] #finalize SIZEIndex
MarketPremium = MarketPremium.loc[(MarketPremium['Year']>=BBBEEStartYear)] #finalize MarketPremium
MarketReturn = MarketReturn.loc[(MarketReturn['Year']>=BBBEEStartYear)] #finalize MarketReturn
RiskFreeReturn = RiskFreeReturn.loc[(RiskFreeReturn['Year']>=BBBEEStartYear)] #finalize RiskFreeReturn
SharePriceReturn = SharePriceReturn.loc[(SharePriceReturn['Year']>=BBBEEStartYear)] 

#Run Descriptives over different time horizons - describe
tmpBMRatio = Dataset1.loc[(np.isnan(np.array(Dataset1['BBBEE_Rank_Clean']))==False)]#remove all rows with no BBBEE value
tmpSIZERatio = Dataset1.loc[(np.isnan(np.array(Dataset1['BBBEE_Rank_Clean']))==False)]#remove all rows with no BBBEE value
tmpEPRatio = Dataset1.loc[(np.isnan(np.array(Dataset1['BBBEE_Rank_Clean']))==False)]#remove all rows with no BBBEE value
tmpSharePriceReturn = SharePriceReturn.loc[(np.isnan(np.array(Dataset1['BBBEE_Rank_Clean']))==False)]
Describe1 = pd.DataFrame(Dataset1['FirmID'].describe())
Describe1 = Functions.DescribeExNaN(tmpBMRatio,'BMRatio',Describe1)
Describe1 = Functions.DescribeExNaN(tmpSIZERatio,'SIZERatio',Describe1)
Describe1 = Functions.DescribeExNaN(tmpEPRatio,'EPRatio',Describe1)
Describe1 = pd.DataFrame(Functions.DescribeMultipleYearsFactor(BMIndex,'BMIndex_YR',InpReturnHorizonYears,'ReturnHorizonYears',Describe1)) #describes over all the InputYears for this factor
Describe1 = pd.DataFrame(Functions.DescribeMultipleYearsFactor(SIZEIndex,'SIZEIndex_YR',InpReturnHorizonYears,'ReturnHorizonYears',Describe1))
Describe1 = pd.DataFrame(Functions.DescribeMultipleYearsFactor(MarketPremium,'MarketPremium_YR',InpReturnHorizonYears,'ReturnHorizonYears',Describe1))
Describe1 = pd.DataFrame(Functions.DescribeMultipleYearsFactor(MarketReturn,'MarketReturn_YR',InpReturnHorizonYears,'ReturnHorizonYears',Describe1))
Describe1 = pd.DataFrame(Functions.DescribeMultipleYearsFactor(RiskFreeReturn,'RiskFreeReturn_YR',InpReturnHorizonYears,'ReturnHorizonYears',Describe1))
Describe1 = pd.DataFrame(Functions.DescribeMultipleYearsFactor(tmpSharePriceReturn,'SharePriceReturn_YR',InpReturnHorizonYears,'ReturnHorizonYears',Describe1))
Describe1.to_excel(ExportDir + 'Descriptives.xlsx', sheet_name='Input')
del tmpBMRatio, tmpSIZERatio, tmpEPRatio

#Run Descriptives over different time horizons - describe, scatterplots, correlation matrix
InpReturnHorizonYears = pd.DataFrame([1,2,3,4,5])
InputYears = InpReturnHorizonYears
OutputSet = Dataset1[['Year','FirmID','BMRatio','SIZERatio','EPRatio','BBBEE_Rank_Clean']]
OutputSet = OutputSet.dropna()
RegressionOutputMF = {}
RegressionOutputMF20042007 = {}
RegressionOutputMF20072013 = {}
RegressionOutputMF20132018 = {}
RegressionOutputFF = {}
RegressionOutputFF20042007 = {}
RegressionOutputFF20072013 = {}
RegressionOutputFF20132018 = {}
RegressionOutputNested = {}

for ii in range(InputYears.shape[0]): #see https://lectures.quantecon.org/py/ols.html
    tmpOutputAll = pd.merge(OutputSet[['Year','FirmID','BMRatio','SIZERatio','EPRatio','BBBEE_Rank_Clean']],BMIndex[['Year',str('BMIndex_YR'+ str(InputYears[0][ii]))]],on='Year',how='left')
    tmpOutputAll['BBBEE_Rank'] = tmpOutputAll['BBBEE_Rank_Clean']
    tmpOutputAll = tmpOutputAll.drop('BBBEE_Rank_Clean', axis=1)
    tmpOutputAll = pd.merge(tmpOutputAll,SIZEIndex[['Year',str('SIZEIndex_YR'+ str(InputYears[0][ii]))]],on='Year',how='left')
    tmpOutputAll = pd.merge(tmpOutputAll,MarketPremium[['Year',str('MarketPremium_YR'+ str(InputYears[0][ii]))]],on='Year',how='left')
    tmpOutputAll = pd.merge(tmpOutputAll,RiskFreeReturn[['Year',str('RiskFreeReturn_YR'+ str(InputYears[0][ii]))]],on='Year',how='left')
    tmpOutputAll = pd.merge(tmpOutputAll,SharePriceReturn[['Year','FirmID',str('SharePriceReturn_YR'+ str(InputYears[0][ii]))]],on=['Year','FirmID'],how='left')
    tmpOutputAll = pd.merge(tmpOutputAll,SectorDummy,on=['Year','FirmID'],how='left')
    tmpOutputAll = tmpOutputAll.drop_duplicates()
    for xx in range(4):                
        if xx == 0:
             tmpOutput = tmpOutputAll
             #correlation matrix
             tmpCorrelationMatrix = pd.DataFrame(tmpOutput.corr())
             tmpCorrelationMatrix.to_excel(ExportDir + 'CorrelationMatrix_YR' + str(InputYears[0][ii])+ '.xlsx', sheet_name='Input')       
             #create scatterplots 
             tmpXColumns = ['BMRatio','SIZERatio','EPRatio','BBBEE_Rank',str('BMIndex_YR'+ str(InputYears[0][ii])),str('SIZEIndex_YR'+ str(InputYears[0][ii])),str('MarketPremium_YR'+ str(InputYears[0][ii])),str('RiskFreeReturn_YR'+ str(InputYears[0][ii]))]
             tmpYColumns = str('SharePriceReturn_YR'+ str(InputYears[0][ii]))    
             Functions.PriceLogScatterplots(tmpXColumns,tmpYColumns,tmpOutput,ExportDir)    
             del tmpXColumns,tmpYColumns,tmpCorrelationMatrix
        elif xx == 1:
             tmpOutput = tmpOutputAll.loc[(tmpOutputAll['Year']<2007)]   
        elif xx == 2:
             tmpOutput = tmpOutputAll.loc[(tmpOutputAll['Year']>=2007) & (tmpOutputAll['Year']<2013)]   
        elif xx == 3:
             tmpOutput = tmpOutputAll.loc[(tmpOutputAll['Year']>=2013)]   
             
        #Define Y and X and standardize X column names
        tmpY = tmpOutput[[str('SharePriceReturn_YR'+ str(InputYears[0][ii]))]]  
        
        tmpX1 = tmpOutput.drop(['Year','FirmID',str('BMIndex_YR'+ str(InputYears[0][ii])),str('SIZEIndex_YR'+ str(InputYears[0][ii])),str('SharePriceReturn_YR'+ str(InputYears[0][ii])),str('MarketPremium_YR'+ str(InputYears[0][ii])),str('RiskFreeReturn_YR'+ str(InputYears[0][ii]))], axis=1)
        tmpX1 = Functions.OLSStandardizeXCol(tmpX1)   
        tmpX1 = sm.add_constant(tmpX1)
        tmpX2 = tmpOutput.drop(['Year','FirmID','BMRatio','SIZERatio','EPRatio',str('SharePriceReturn_YR'+ str(InputYears[0][ii]))], axis=1)
        tmpX2 = Functions.OLSStandardizeXCol(tmpX2) 
        tmpX3 = tmpOutput.drop(['Year','FirmID',str('SharePriceReturn_YR'+ str(InputYears[0][ii]))], axis=1)
        tmpX3 = sm.add_constant(tmpX3)
        tmpX3 = Functions.OLSStandardizeXCol(tmpX3) 
        
        #Run Regression over different time horizons - simple bp, size, bpIndex, sizeIndex and     
        tmpOLSMF = sm.OLS(tmpY,tmpX1, missing='drop') # MF = Merwe and Ferreira model
        tmpOLSMFResults = tmpOLSMF.fit()
        tmpKey = str('MF' + str(InputYears[0][ii]))   # https://stackoverflow.com/questions/5036700/how-can-you-dynamically-create-variables-via-a-while-loop 
        tmpValue = tmpOLSMFResults            
        if xx == 0:
             RegressionOutputMF[tmpKey] = tmpValue    
        elif xx == 1:
             RegressionOutputMF20042007[tmpKey] = tmpValue    
        elif xx == 2:
             RegressionOutputMF20072013[tmpKey] = tmpValue    
        elif xx == 3:     
            RegressionOutputMF20132018[tmpKey] = tmpValue                
        del tmpKey, tmpValue            
       
        tmpOLSFF = sm.OLS(tmpY,tmpX2, missing='drop') # MF = Fama and French
        tmpOLSFFResults = tmpOLSFF.fit()  
        tmpKey1 = str('FF' + str(InputYears[0][ii]))   # https://stackoverflow.com/questions/5036700/how-can-you-dynamically-create-variables-via-a-while-loop 
        tmpValue1 = tmpOLSFFResults   
        if xx == 0:
             RegressionOutputFF[tmpKey1] = tmpValue1    
        elif xx == 1:
             RegressionOutputFF20042007[tmpKey1] = tmpValue1    
        elif xx == 2:
             RegressionOutputFF20072013[tmpKey1] = tmpValue1    
        elif xx == 3:     
            RegressionOutputFF20132018[tmpKey1] = tmpValue1                
        del tmpKey1, tmpValue1
        
        tmpOLSNested = sm.OLS(tmpY,tmpX3, missing='drop') # MF = Fama and French
        tmpOLSNestedResults = tmpOLSNested.fit()  
        tmpKey1 = str('Nested' + str(InputYears[0][ii]))   # https://stackoverflow.com/questions/5036700/how-can-you-dynamically-create-variables-via-a-while-loop 
        tmpValue1 = tmpOLSNestedResults   
        if xx == 0:
             RegressionOutputNested[tmpKey1] = tmpValue1 
        del tmpKey1, tmpValue1                       
        del tmpOutput,tmpX1,tmpX2,tmpY,tmpOLSMF,tmpOLSMFResults,tmpOLSFF,tmpOLSFFResults
    del tmpOutputAll

del ii, xx
Functions.Regression5YearOutput(RegressionOutputMF,ExportDir,'OLS_Summary_' ) # Output Regression results Merwe and Ferreira
Functions.Regression5YearOutput(RegressionOutputMF20042007,ExportDir,'OLS_Summary_2004_2007_' ) # Output Regression results Merwe and Ferreira
Functions.Regression5YearOutput(RegressionOutputMF20072013,ExportDir,'OLS_Summary_2007_2013_' ) # Output Regression results Merwe and Ferreira
Functions.Regression5YearOutput(RegressionOutputMF20132018,ExportDir,'OLS_Summary_2013_2018_' ) # Output Regression results Merwe and Ferreira

Functions.Regression5YearOutput(RegressionOutputFF,ExportDir,'OLS_Summary_' ) # Output Regression results Fama French
Functions.Regression5YearOutput(RegressionOutputFF20042007,ExportDir,'OLS_Summary_2004_2007_' ) # Output Regression results Merwe and Ferreira
Functions.Regression5YearOutput(RegressionOutputFF20072013,ExportDir,'OLS_Summary_2007_2013_' ) # Output Regression results Merwe and Ferreira
Functions.Regression5YearOutput(RegressionOutputFF20132018,ExportDir,'OLS_Summary_2013_2018_' ) # Output Regression results Merwe and Ferreira

Functions.Regression5YearOutput(RegressionOutputNested,ExportDir,'OLS_Summary_' ) # Output Regression results Fama French

#regression per sector
RegressionSectorFF = dict.fromkeys(list(tmpSECTORID['DS_SECTORNAME']))
RegressionSectorMF = dict.fromkeys(list(tmpSECTORID['DS_SECTORNAME']))
tmpSectorLoop = 0
for tmpSectorLoop in range(tmpSECTORID.shape[0]):
    tmpSectorID = tmpSECTORID['DS_SECTORID'].iloc[tmpSectorLoop]
    tmpSectorName = tmpSECTORID['DS_SECTORNAME'].iloc[tmpSectorLoop]
    RegressionSectorMF[tmpSectorName] ={}
    RegressionSectorFF[tmpSectorName] ={}
    OutputSetSector = Dataset1.loc[(Dataset1['DS_SECTORID'] == tmpSectorID),['Year','FirmID','BMRatio','SIZERatio','EPRatio','BBBEE_Rank_Clean']]
    for ii in range(InputYears.shape[0]): #see https://lectures.quantecon.org/py/ols.html
        tmpOutputAll = pd.merge(OutputSetSector[['Year','FirmID','BMRatio','SIZERatio','EPRatio','BBBEE_Rank_Clean']],BMIndex[['Year',str('BMIndex_YR'+ str(InputYears[0][ii]))]],on='Year',how='left')
        tmpOutputAll['BBBEE_Rank'] = tmpOutputAll['BBBEE_Rank_Clean']
        tmpOutputAll = tmpOutputAll.drop('BBBEE_Rank_Clean', axis=1)
        tmpOutputAll = pd.merge(tmpOutputAll,SIZEIndex[['Year',str('SIZEIndex_YR'+ str(InputYears[0][ii]))]],on='Year',how='left')
        tmpOutputAll = pd.merge(tmpOutputAll,MarketPremium[['Year',str('MarketPremium_YR'+ str(InputYears[0][ii]))]],on='Year',how='left')
        tmpOutputAll = pd.merge(tmpOutputAll,RiskFreeReturn[['Year',str('RiskFreeReturn_YR'+ str(InputYears[0][ii]))]],on='Year',how='left')
        tmpOutputAll = pd.merge(tmpOutputAll,SharePriceReturn[['Year','FirmID',str('SharePriceReturn_YR'+ str(InputYears[0][ii]))]],on=['Year','FirmID'],how='left')
        tmpOutputAll = pd.merge(tmpOutputAll,SectorDummy,on=['Year','FirmID'],how='left')
        tmpOutputAll = tmpOutputAll.drop_duplicates()
        tmpOutput = tmpOutputAll
        tmpY = tmpOutput[[str('SharePriceReturn_YR'+ str(InputYears[0][ii]))]]          
        tmpX1 = tmpOutput.drop(['Year','FirmID',str('BMIndex_YR'+ str(InputYears[0][ii])),str('SIZEIndex_YR'+ str(InputYears[0][ii])),str('SharePriceReturn_YR'+ str(InputYears[0][ii])),str('MarketPremium_YR'+ str(InputYears[0][ii])),str('RiskFreeReturn_YR'+ str(InputYears[0][ii]))], axis=1)
        tmpX1 = Functions.OLSStandardizeXCol(tmpX1)   
        tmpX1 = sm.add_constant(tmpX1)
        tmpX2 = tmpOutput.drop(['Year','FirmID','BMRatio','SIZERatio','EPRatio',str('SharePriceReturn_YR'+ str(InputYears[0][ii]))], axis=1)
        tmpX2 = Functions.OLSStandardizeXCol(tmpX2) 
        
        #Run Regression over different time horizons - simple bp, size, bpIndex, sizeIndex and     
        tmpOLSMF = sm.OLS(tmpY,tmpX1, missing='drop') # MF = Merwe and Ferreira model
        tmpOLSMFResults = tmpOLSMF.fit()
        tmpKey = str('MF' + str(InputYears[0][ii]))   # https://stackoverflow.com/questions/5036700/how-can-you-dynamically-create-variables-via-a-while-loop 
        tmpValue = tmpOLSMFResults          
        RegressionSectorMF[tmpSectorName][tmpKey] = tmpValue
        del tmpKey, tmpValue     
            
        tmpOLSFF = sm.OLS(tmpY,tmpX2, missing='drop') # MF = Fama and French
        tmpOLSFFResults = tmpOLSFF.fit()  
        tmpKey1 = str('FF' + str(InputYears[0][ii]))   # https://stackoverflow.com/questions/5036700/how-can-you-dynamically-create-variables-via-a-while-loop 
        tmpValue1 = tmpOLSFFResults   
        RegressionSectorFF[tmpSectorName][tmpKey1] = tmpValue1
        del tmpKey1, tmpValue1                
        
        del tmpOutputAll,tmpOutput,tmpX1,tmpX2,tmpY,tmpOLSMF,tmpOLSMFResults,tmpOLSFF,tmpOLSFFResults
    
    Functions.Regression5YearOutput(RegressionSectorMF[tmpSectorName],ExportDir,'OLS_Summary_' + tmpSectorName + '_')
    Functions.Regression5YearOutput(RegressionSectorFF[tmpSectorName],ExportDir,'OLS_Summary_' + tmpSectorName + '_')            
    del ii, tmpSectorID, tmpSectorName, OutputSetSector
del tmpSectorLoop

#Check BBBEE persistency
tmpDataset = Dataset1.loc[(Dataset1['Year']>=BBBEEStartYear)]
tmpAll = Functions.PersistencyDF(tmpDataset,5)
tmpAll.to_excel(ExportDir + 'BBBEEPersistency_All.xlsx', sheet_name='Input')
del tmpDataset,tmpAll

for LoopIndex, LoopSectorID in enumerate(tmpSECTORID['DS_SECTORID']):
    tmpSectorName = tmpSECTORID['DS_SECTORNAME'].iloc[LoopIndex]
    print(tmpSectorName)
    tmpDataset = Dataset1.loc[(Dataset1['Year']>=BBBEEStartYear)&(Dataset1['DS_SECTORID']==LoopSectorID)]
    tmpSector = Functions.PersistencyDF(tmpDataset,5)
    tmpSector.to_excel(ExportDir + 'BBBEEPersistency_' + tmpSectorName + '.xlsx', sheet_name='Input')
del tmpDataset,tmpSector,LoopIndex, LoopSectorID

#Adjust for outliers - Dataset2 - RiskFreeReturn is not adjusted 
Dataset2 = Dataset1 #Already contains clean BBBEE
tmpYear2 = tmpYear.loc[(tmpYear['Year']>=BBBEEStartYear)] #ensure to only take years for which BBBBEE data is available
tmpYear2 = tmpYear2.reset_index(drop=True)
InpReturnHorizonYears = pd.DataFrame([1,2,3,4,5])
InpReturnHorizonYears.columns = ['ReturnHorizonYears']

#Adjust BM, SIZE
Dataset2 = Dataset2.reset_index(drop=True)
tmpColumns = ['BMRatio','SIZERatio']
for ii in range(len(tmpColumns)): 
    inpColumn = str(tmpColumns[ii])
    tmpDF1 = Functions.CapOutliers(Dataset2,inpColumn, ExportDir)
    Dataset2 = Dataset2.drop([inpColumn], axis=1)
    Dataset2 = pd.merge(Dataset2,tmpDF1,on=['Year','FirmID'],how='left')
del tmpColumns, ii

#Adjust SharePriceReturns for outliers - Create SharePriceReturn2
SharePriceReturn2 = SharePriceReturn[['Year','FirmID']]
tmpColumns = pd.DataFrame(SharePriceReturn.columns)
tmpColumns = tmpColumns.loc[(tmpColumns[0] != 'Year')]
tmpColumns = tmpColumns.loc[(tmpColumns[0] != 'FirmID')]
tmpColumns = list(tmpColumns[0])
for ii in range(len(tmpColumns)): 
    inpColumn = str(tmpColumns[ii])
    tmpDF1 = Functions.CapOutliers(SharePriceReturn,inpColumn, ExportDir)
    SharePriceReturn2 = pd.merge(SharePriceReturn2,tmpDF1,on=['Year','FirmID'],how='left')
del tmpColumns, ii

#Rerun Deciles
Dataset2 = Dataset2.drop(['SIZERatio_Decile','BMRatio_Decile'], axis=1)

#Rerun factor returns
FactorDecile2 = pd.DataFrame(['SIZERatio','BMRatio'])
FactorDecile2.columns = ['Factors']
for ii in range(FactorDecile2.shape[0]):    
    inpFactor2 = FactorDecile2['Factors'][ii]
    FactorDF2 = pd.DataFrame(columns=['FirmID','Year',str(inpFactor2+'_Decile')])
    for jj in range(tmpYear2.shape[0]):
        inpYear = tmpYear2['Year'][jj]
        tmpFactorDF2 = Dataset2.loc[(Dataset2['Year']==inpYear),['FirmID','Year',inpFactor2]]
        tmpFactorDF2 = tmpFactorDF2.drop_duplicates()
        tmpFactorDF2[str(inpFactor2+'_Decile')] = pd.qcut(tmpFactorDF2[inpFactor2],10,labels=False)
        tmpFactorDF2 = tmpFactorDF2[['FirmID','Year',str(inpFactor2+'_Decile')]]
        tmpFactorDF2 = tmpFactorDF2.astype(float)
        FactorDF2 = pd.concat([FactorDF2, tmpFactorDF2])    
        del inpYear, tmpFactorDF2
    Dataset2 = pd.merge(Dataset2,FactorDF2,on=['FirmID','Year'],how='left')
    del inpFactor2, FactorDF2

del ii,jj 
BMMatrix2 = Dataset2.pivot_table('BMRatio_Decile', index='Year', columns='FirmID', dropna=False)
BMTopDummy2 = ([BMMatrix2 <=2])
BMTopDummy2 = BMTopDummy2[0]
BMBottomDummy2 = ([BMMatrix2 >=7])
BMBottomDummy2 = BMBottomDummy2[0]
SIZEMatrix2 = Dataset2.pivot_table('SIZERatio_Decile', index='Year', columns='FirmID' , dropna=False)
SIZETopDummy2 = ([SIZEMatrix2 <=2])
SIZETopDummy2 = SIZETopDummy2[0]
SIZEBottomDummy2 = ([SIZEMatrix2 >=7])
SIZEBottomDummy2 = SIZEBottomDummy2[0]

#Rerun MarketReturn, MarketPremium, BMIndex, SIZEIndex
MarketReturn2 = tmpYear2[['Year','YearIndex']]
MarketPremium2 = tmpYear2[['Year','YearIndex']]
BMIndex2 = tmpYear2[['Year','YearIndex']]
SIZEIndex2 = tmpYear2[['Year','YearIndex']]
BMTopDummy2 = BMTopDummy[BMTopDummy.index>=BBBEEStartYear] #source: https://stackoverflow.com/questions/24088795/python-pandas-select-index-where-index-is-larger-than-x
BMBottomDummy2 = BMBottomDummy[BMBottomDummy.index>=BBBEEStartYear] 

for ii in range(InpReturnHorizonYears.shape[0]):
    tmpReturnHorizonYears = InpReturnHorizonYears['ReturnHorizonYears'][ii]
    tmpSharePriceReturnColumn = str('SharePriceReturn_YR' + str(tmpReturnHorizonYears))    
    tmpDF = SharePriceReturn2.pivot_table(tmpSharePriceReturnColumn, index='Year', columns='FirmID', dropna=False)    
    MarketReturn2['MarketReturn_YR'+str(tmpReturnHorizonYears)] = np.nanmean(np.array(tmpDF), axis=1) #calculate market return over time horizon    
    MarketPremium2['MarketPremium_YR'+str(tmpReturnHorizonYears)] = MarketReturn2['MarketReturn_YR'+str(tmpReturnHorizonYears)] - RiskFreeReturn['RiskFreeReturn_YR'+str(tmpReturnHorizonYears)]
    tmpBMTop = pd.DataFrame(np.array(BMTopDummy2)*np.array(tmpDF))
    tmpBMTop = pd.DataFrame(np.nanmean(tmpBMTop, axis=1))
    tmpBMBottom = pd.DataFrame(np.array(BMBottomDummy2)*np.array(tmpDF))
    tmpBMBottom = pd.DataFrame(np.nanmean(tmpBMBottom, axis=1))    
    BMIndex2['BMIndex_YR'+str(tmpReturnHorizonYears)] = tmpBMTop - tmpBMBottom
    tmpSIZETop = pd.DataFrame(np.array(SIZETopDummy2)*np.array(tmpDF))
    tmpSIZETop = pd.DataFrame(np.nanmean(tmpSIZETop, axis=1))
    tmpSIZEBottom = pd.DataFrame(np.array(SIZEBottomDummy2)*np.array(tmpDF))
    tmpSIZEBottom = pd.DataFrame(np.nanmean(tmpSIZEBottom, axis=1))
    SIZEIndex2['SIZEIndex_YR'+str(tmpReturnHorizonYears)] = tmpSIZEBottom - tmpSIZETop #Fama French Size is small minus big market cap
    del tmpDF, tmpBMTop, tmpBMBottom, tmpSIZETop, tmpSIZEBottom

#Run Descriptives over different time horizons - describe
tmpBMRatio = Dataset2.loc[(np.isnan(np.array(Dataset1['BBBEE_Rank_Clean']))==False)]#remove all rows with no BBBEE value
tmpSIZERatio = Dataset2.loc[(np.isnan(np.array(Dataset1['BBBEE_Rank_Clean']))==False)]#remove all rows with no BBBEE value
tmpEPRatio = Dataset2.loc[(np.isnan(np.array(Dataset1['BBBEE_Rank_Clean']))==False)]#remove all rows with no BBBEE value
tmpSharePriceReturn = SharePriceReturn2.loc[(np.isnan(np.array(Dataset1['BBBEE_Rank_Clean']))==False)]
Describe2 = pd.DataFrame(Dataset2['FirmID'].describe())
Describe2 = Functions.DescribeExNaN(tmpBMRatio,'BMRatio',Describe2)
Describe2 = Functions.DescribeExNaN(tmpSIZERatio,'SIZERatio',Describe2)
Describe2 = Functions.DescribeExNaN(tmpEPRatio,'EPRatio',Describe2)
Describe2 = pd.DataFrame(Functions.DescribeMultipleYearsFactor(BMIndex2,'BMIndex_YR',InpReturnHorizonYears,'ReturnHorizonYears',Describe2)) #describes over all the InputYears for this factor
Describe2 = pd.DataFrame(Functions.DescribeMultipleYearsFactor(SIZEIndex2,'SIZEIndex_YR',InpReturnHorizonYears,'ReturnHorizonYears',Describe2))
Describe2 = pd.DataFrame(Functions.DescribeMultipleYearsFactor(MarketPremium2,'MarketPremium_YR',InpReturnHorizonYears,'ReturnHorizonYears',Describe2))
Describe2 = pd.DataFrame(Functions.DescribeMultipleYearsFactor(MarketReturn2,'MarketReturn_YR',InpReturnHorizonYears,'ReturnHorizonYears',Describe2))
Describe2 = pd.DataFrame(Functions.DescribeMultipleYearsFactor(RiskFreeReturn,'RiskFreeReturn_YR',InpReturnHorizonYears,'ReturnHorizonYears',Describe2))
Describe2 = pd.DataFrame(Functions.DescribeMultipleYearsFactor(tmpSharePriceReturn,'SharePriceReturn_YR',InpReturnHorizonYears,'ReturnHorizonYears',Describe2))
Describe2.to_excel(ExportDir + 'Descriptives_OutlierAdjusted.xlsx', sheet_name='Input')
del tmpBMRatio, tmpSIZERatio, tmpEPRatio, tmpSharePriceReturn

#Run Descriptives over different time horizons - describe, scatterplots, correlation matrix
InpReturnHorizonYears = pd.DataFrame([1,2,3,4,5])
InputYears = InpReturnHorizonYears
OutputSet2 = Dataset2[['Year','FirmID','BMRatio','SIZERatio','EPRatio','BBBEE_Rank_Clean']]
OutputSet2 = OutputSet2.dropna()
RegressionOutputMF2 = {}
RegressionOutputFF2 = {}

for ii in range(InputYears.shape[0]): #see https://lectures.quantecon.org/py/ols.html
    tmpOutput = pd.merge(OutputSet2[['Year','FirmID','BMRatio','SIZERatio','EPRatio','BBBEE_Rank_Clean']],BMIndex2[['Year',str('BMIndex_YR'+ str(InputYears[0][ii]))]],on='Year',how='left')
    tmpOutput['BBBEE_Rank'] = tmpOutput['BBBEE_Rank_Clean']
    tmpOutput = tmpOutput.drop('BBBEE_Rank_Clean', axis=1)
    tmpOutput = pd.merge(tmpOutput,SIZEIndex2[['Year',str('SIZEIndex_YR'+ str(InputYears[0][ii]))]],on='Year',how='left')
    tmpOutput = pd.merge(tmpOutput,MarketPremium2[['Year',str('MarketPremium_YR'+ str(InputYears[0][ii]))]],on='Year',how='left')
    tmpOutput = pd.merge(tmpOutput,RiskFreeReturn[['Year',str('RiskFreeReturn_YR'+ str(InputYears[0][ii]))]],on='Year',how='left')
    tmpOutput = pd.merge(tmpOutput,SharePriceReturn2[['Year','FirmID',str('SharePriceReturn_YR'+ str(InputYears[0][ii]))]],on=['Year','FirmID'],how='left')
    tmpOutput = pd.merge(tmpOutput,SectorDummy,on=['Year','FirmID'],how='left')
    tmpOutput = tmpOutput.drop_duplicates()
    
    #correlation matrix
    tmpCorrelationMatrix = pd.DataFrame(tmpOutput.corr())
    tmpCorrelationMatrix.to_excel(ExportDir + 'CorrelationMatrix_OutlierAdjusted_YR' + str(InputYears[0][ii])+ '.xlsx', sheet_name='Input')
   
    #create scatterplots 
    tmpXColumns = ['BMRatio','SIZERatio','EPRatio','BBBEE_Rank',str('BMIndex_YR'+ str(InputYears[0][ii])),str('SIZEIndex_YR'+ str(InputYears[0][ii])),str('MarketPremium_YR'+ str(InputYears[0][ii])),str('RiskFreeReturn_YR'+ str(InputYears[0][ii]))]
    tmpYColumn = str('SharePriceReturn_YR'+ str(InputYears[0][ii]))    
    Functions.PriceLogScatterplots(tmpXColumns,tmpYColumn,tmpOutput,ExportDir)    
    del tmpXColumns,tmpYColumn
                   
    #Define Y and X and standardize X column names
    tmpY = tmpOutput[[str('SharePriceReturn_YR'+ str(InputYears[0][ii]))]]  
    tmpX1 = tmpOutput.drop(['Year','FirmID',str('BMIndex_YR'+ str(InputYears[0][ii])),str('SIZEIndex_YR'+ str(InputYears[0][ii])),str('SharePriceReturn_YR'+ str(InputYears[0][ii])),str('MarketPremium_YR'+ str(InputYears[0][ii])),str('RiskFreeReturn_YR'+ str(InputYears[0][ii]))], axis=1)    
    tmpX1 = Functions.OLSStandardizeXCol(tmpX1)   
    tmpX1 = sm.add_constant(tmpX1)
    tmpX2 = tmpOutput.drop(['Year','FirmID','BMRatio','SIZERatio','EPRatio',str('SharePriceReturn_YR'+ str(InputYears[0][ii]))], axis=1)
    tmpX2 = Functions.OLSStandardizeXCol(tmpX2)    
    
    #Run Regression over different time horizons - simple bpRatio, sizeRatio, bpIndex, sizeIndex and     
    tmpOLSMF = sm.OLS(tmpY,tmpX1, missing='drop') # MF = Merwe and Ferreira model
    tmpOLSMFResults = tmpOLSMF.fit()
    tmpKey = str('MF' + str(InputYears[0][ii]))   # https://stackoverflow.com/questions/5036700/how-can-you-dynamically-create-variables-via-a-while-loop 
    tmpValue = tmpOLSMFResults    
    RegressionOutputMF2[tmpKey] = tmpValue    
    del tmpKey, tmpValue
        
    tmpOLSFF = sm.OLS(tmpY,tmpX2, missing='drop') # MF = Fama and French
    tmpOLSFFResults = tmpOLSFF.fit()  
    tmpKey1 = str('FF' + str(InputYears[0][ii]))   # https://stackoverflow.com/questions/5036700/how-can-you-dynamically-create-variables-via-a-while-loop 
    tmpValue1 = tmpOLSFFResults    
    RegressionOutputFF2[tmpKey1] = tmpValue1  
    del tmpKey1, tmpValue1
            
    del tmpOutput,tmpCorrelationMatrix,tmpX1,tmpX2,tmpY,tmpOLSMF,tmpOLSMFResults,tmpOLSFF,tmpOLSFFResults
    
Functions.Regression5YearOutput(RegressionOutputMF2,ExportDir,'OLS_Summary_OutlierAdjusted' ) # Output Regression results Merwe and Ferreira
Functions.Regression5YearOutput(RegressionOutputFF2,ExportDir,'OLS_Summary_OutlierAdjusted' ) # Output Regression results Fama French