# first, get list of stocks

# then (get stock data + analyse it) using multithread and store results at one place.

import pandas as pd
import requests
from nsepython import *
import json
import time
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep

finPCR = []

def getStockList():
    stockList =nsefetch("https://www.nseindia.com/api/master-quote")
    print(stockList)
    return stockList


def getStockOptionChainData(stock):
    try:
        data = nse_optionchain_scrapper(stock)
    except:
        print("********")
        print(stock)    
    return data

def modPCR(finalJson):
    putContractSumMod = 0
    callContractSumMod = 0
    putContractSumReg = 0
    callContractSumReg = 0
    for element in finalJson["records"]["data"]:
        
        try:
            if ("PE" in element) : 
                putContractSumMod = putContractSumMod + (element["PE"]["openInterest"] * element["PE"]["lastPrice"])
                putContractSumReg = putContractSumReg + (element["PE"]["openInterest"])
            
            if ("CE" in element):
                callContractSumMod = callContractSumMod + (element["CE"]["openInterest"] * element["CE"]["lastPrice"])
                callContractSumReg = callContractSumReg + (element["CE"]["openInterest"] )
        
        except : 
            print("Error in mod PCR **********")
            print(element)
            print("**********")
    
    regPcr = pcr(putContractSumReg,callContractSumReg)
    ModPCR = pcr(putContractSumMod,callContractSumMod)


    
    return regPcr,ModPCR

def pcr(putContractSum,callContractSum):
    pcr = -1
    if(callContractSum != 0):
        pcr = putContractSum/callContractSum 
    else : 
        pcr = -1
    return pcr


def analyse(stock):

    finalJson = getStockOptionChainData(stock)
    
    resultDictionary = {}
    pcrResults = modPCR(finalJson)
    resultDictionary["Stock"] = stock
    resultDictionary["PCR"] = pcrResults[0]
    resultDictionary["ModPCR"] = pcrResults[1]
        
    # append result here 
    finPCR.append(resultDictionary)

def runner():
    threads= []

    i=1

    stocklist = getStockList()
    if(len(stocklist) != 0):
        with ThreadPoolExecutor(max_workers=20) as executor:
            for stock in stocklist:
                sleep(0.75)   
                future = executor.submit(analyse, stock)
                print(str(i)+ " " +str(future.result))
                i=i+1
                threads.append(future)


            for task in as_completed(threads):
                print(task.result())
    else:
        print("stockList empty") 

        
start = time.time()

runner()

df = pd.json_normalize(finPCR)
# df = df.sort_values('Stock')

print(df)

df.to_csv("results.csv")

end = time.time()

print("Total Time : " + str(end-start))



#use query to filter : df.query('ctg == "B" and val > 0.5')
# ctg and val here are column names. We can use this as text prompt from terminal


# The nlargest and nsmallest functions allow you to select rows that have the largest or smallest values in a column, respectively.
# df.nlargest(3, 'val')






