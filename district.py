import requests
import pprint
import json
import string
import csv

#for each state
#Query all districts
#store in json file

states = {"states":[]}

key = "26d5ae7b18de660b3e6115f03fba2a9d"

stateList = ["AL","AK","AZ","AR","CA","CO","CT","DE","DC","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"]
districts = {}

#debugMode controls whether various print statements will execute or not
debugMode = True

#Will print out a message to the console if debugMode is true
#@param message, some string with user specified text
def debugPrint(message):
  if debugMode:
    print message

def getDistricts(dId):
  #add districts to states
  

  districts[dId] = makeAPICall("schoolSearch", "districtleaid=" + dId)
  pprint.pprint(districts[dId])





#Generic wrapper function to handle all various api calls
# to education.com
# @param apiFunction: string
# @param apiParameter: string
# @return jsonObject
def makeAPICall(apiFunction, apiParameters):
  #Format the url call
  url = "http://api.education.com/service/service.php?f=" + apiFunction
  url = url + "&key="+key+"&sn=sf&v=4&" + apiParameters + "&Resf=json"
  debugPrint("Launching api");
  #Request the data and turn it into json form
  apiResponse = requests.get(url)
  jsonData = apiResponse.json()
  #debugPrint(jsonData)
  #Iterate through all metadata school objects in the json
  for school in jsonData:
    if school == 'faultCode' or school == 'faultString' or school == 'faultType':
      #debugPrint("Fault encountered")
      continue
  
  debugPrint("Done with api")
  return jsonData

def readEthnicityData():
  with open("districtEth.csv", 'r+') as csvfile:
    csvText = csv.reader(csvfile, delimiter=",")
    first = True
    skipCount = 0
    for fileRow in csvText:
      row = fileRow[0].split(";")
      if first:
        first = False
        continue

      if len(row) > 1:
        #print row
        name = row[0]
        state = row[7]
        stateCode = row[9]

        ameriIndian = row[2]
        asian = row[3]
        hispanic = row[4]
        black = row[5]
        white = row[6]

        districtID = row[8]

        if ameriIndian == "\xe2\x80\xa0":
          print "Skipped " + name
          skipCount += 1
          continue

        newDistrict = {}
        newDistrict["name"] = name
        newDistrict["state"] = state.strip()
        newDistrict["stateCode"] = stateCode
        newDistrict["districtID"] = districtID
        newDistrict["ameriIndian"] = ameriIndian
        newDistrict["asian"] = asian
        newDistrict["hispanic"] = hispanic
        newDistrict["black"] = black
        newDistrict["white"] = white
        districts[districtID] = newDistrict

    print "Skipped a total of: " + str(skipCount)
    print "Total districts added: " + str(len(districts))

def readFinanceData():
  with open("allDistFinance.csv", 'r+') as csvfile:
    csvText = csv.reader(csvfile, delimiter=",")
    first = True
    count = 0
    for fileRow in csvText:
      row = fileRow[0].split(";")
      if first:
        first = False
        continue

      if len(row) > 1:
        state = row[2]
        districtID = row[3]
        latitude = row[5]
        longitude = row[6]

        studentTeacherRatio = row[7]
        stateRevenuePerStu = row[8]
        federalRevenuePerStu = row[9]
        totalInstructPerStu = row[10]
        totalExpendPerStu = row[11]

      if districtID in districts:
        districts[districtID]["lat"] = latitude
        districts[districtID]["long"] = longitude
        districts[districtID]["stRatio"] = studentTeacherRatio
        districts[districtID]["stateRevenue"] = stateRevenuePerStu
        districts[districtID]["federalRevenue"] = federalRevenuePerStu
        districts[districtID]["totalInstruct"] = totalInstructPerStu
        districts[districtID]["totalExpend"] = totalExpendPerStu
        count+=1

  print "Added finance data to " + str(count) + " districts"

def formStates():
  statesDict = {}

  for districtID in districts:

    if not districts[districtID]["state"] in statesDict:
      statesDict[districts[districtID]["state"]]  = []

    statesDict[districts[districtID]["state"]].append(districts[districtID])

  with open("districtsByState.json","w") as outfile:
    json.dump(statesDict, outfile)
  print "Number of states: " + str(len(statesDict))


def outputDistrictsFile():
  districtsList = []
  #print "Length of districtsList is " + str(len(districtsDict)) 
  for dist in districts:
    #print districtsDict[dist]["districtId"]
    districtsList.append(districts[dist])

  with open("allDistricts.json", 'w') as outfile:
    json.dump(districtsList, outfile)


#getDistricts("5605830")
readEthnicityData()
readFinanceData()
formStates()
outputDistrictsFile()
