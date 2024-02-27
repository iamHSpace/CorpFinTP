import requests
from nsepython import *
import json
import time
import pandas as pd


start = time.time()

stocklist  = nsefetch("https://www.nseindia.com/api/master-quote")

finPCR = []

i= 1

# stocklist = stocklist[:5]
print(stocklist)

for stock in stocklist:

    putContractSum = 0
    callContractSum = 0


    finalJson = nse_optionchain_scrapper(stock)

        
    for element in finalJson["records"]["data"]:
        
        try:
            if ("PE" in element) : 
                putContractSum = putContractSum + (element["PE"]["openInterest"]* element["PE"]["lastPrice"])
            
            if ("CE" in element):
                callContractSum = callContractSum + (element["CE"]["openInterest"] * element["CE"]["lastPrice"])
        
        except : 
            print("Error**********")
            print(element)
            print("**********")

    pcr = -1

    if(callContractSum != 0):
        pcr = putContractSum/callContractSum 
    else : 
        pcr = -1

    temp = {}
    temp["Stock"] = stock
    temp["ModPCR"] = pcr

    finPCR.append(temp)


    print("Stock completed : " + str(i) + str(stock))
    i = i+1


end = time.time()



json_object = json.dumps(finPCR, indent=4)
with open("sample.json", "w") as outfile:
    outfile.write(json_object)


df = pd.json_normalize(finPCR)

print(df)

print(end-start)





