import requests
import pprint
import json
import csv
from starbase import Connection
# GLOBALS
#old key
#key = "a79efc9741696b8e159b8c3adb3a1719"

#key = "f8a7b2a297ed4f57bef68fa089bebd6e"
#141.161.133.203
#key = "092f7ea38ffe50260b4362bae71e4b17"
#141.161.133.172 - TA room
#key = "7a561bdd96a7f251eab07ae1eb90cc49"

#API Key for education API
#The key is associated with my home ip address key
#This is a very stupid way of doing client auth
#To test this code, you will need to provide your own
#API key from api.education.com
key = "141a9146b994a54b1c5bce195c17845d"

#This dictionary maps fields from the json response
#of the education.com api, to columns in my hbase
#database
jsonToDBMapping = {
  "json":"db",
  "schooltype": "stats:schooltype",
  "districtid": "apiid:districtid",
  "testrating_year":"stats:testrating_year",
  "phonenumber":"addr:phone",
  "city":"addr:city",
  "testrating_image_small": "stats:rating_image",
  "districtleaid":"apiid:lea_id",
  "gradelevel":"stats:gradelevel",
  "nces_id":"apiid:nces_id",
  "studentteacherratio":"stats:s_t_ratio",
  "state":"addr:state",
  "latitude":"addr:latitude",
  "AYPResultYear":"stats:ayp_result_year",
  "website":"web:website",
  "testrating_text":"stats:testrating_text",
  "schooldistrictname":"addr:districtname",
  "address":"addr:address",
  "gradesserved":"stats:grades",
  "schoolname":"addr:schoolname",
  "schoolid": "apiid:schoolid",
  "zip":"addr:zip",
  "AYPResult":"stats:ayp_bool",
  "enrollment":"stats:enrollment",
  "longitude":"addr:longitude",
  "url":"web:url"
}

#Global string defining a table name
#that program will use
#NOTE: If the table is not present in the hbase database,
#this script WILL create it :)
tableName = "schools16"

#List of nces_ids of the schools requested
#This is used to output to a text file
#and for subsequent api calls
idList = []

#Certain fields have embedded totals that we want to post instead of
#a json string, this list tracks which fields those are
embeddedTotals = ["studentteacherratio", "enrollment"]

#debugMode controls whether various print statements will execute or not
debugMode = True

#Will print out a message to the console if debugMode is true
#@param message, some string with user specified text
def debugPrint(message):
  if debugMode:
    print message

#Generic wrapper function to handle all various api calls
# to education.com
# @param apiFunction: string
# @param apiParameter: string
# @return jsonObject
def makeAPICall(apiFunction, apiParameters):
  #Format the url call
  url = "http://api.education.com/service/service.php?f=" + apiFunction
  url = url + "&key="+key+"&sn=sf&v=4&" + apiParameters + "&Resf=json"
  #Request the data and turn it into json form
  apiResponse = requests.get(url)
  jsonData = apiResponse.json()
  #Iterate through all metadata school objects in the json
  for school in jsonData:
    if school == 'faultCode' or school == 'faultString' or school == 'faultType':
      continue
    else:
      #special handling if makeAPICall is being used to search for schools
      if apiFunction == "schoolSearch":
        schoolData = school["school"]
        postSchoolToDB(schoolData)
  return jsonData

#Function that will setup the proper columns in the hbase database
#@param hbase database connection through starbase package
def setupDB(conn):
  #this command will create a table in hbase if it 
  #does not already exist once column families are added
  schoolTable = conn.table(tableName)
  debugPrint("Does table exist? " + str(schoolTable.exists()))

  if not schoolTable.exists():
    schoolTable.create('addr')

  columnFamilies = schoolTable.columns()
  requiredColumns = ["addr", "web", "stats", "apiid", "tests"]
  for col in requiredColumns:
    if not col in columnFamilies:
      debugPrint("Adding column: " + str(col))
      schoolTable.add_columns(col)

#This function will always come after posting to the databse
#so it is assumed the 'schools'table exists
#This function will query diversity information about a given
#school and post the results to the database
#@param unique id number for the relevant public school
def postDiversityToDB(nces_id):
  
  diversityJson = makeAPICall("getStudentStats", "nces_id=" + nces_id)

  diversityInsertList = {}
  if "school" in diversityJson:
    statsList = diversityJson["school"]

    for listIndex in range(0,len(statsList)):

      if statsList[listIndex]["stat_type"] == "Student Ethnicity":
        ethnicityList = statsList[listIndex]["data"]

        for ethStat in range(0,len(ethnicityList)):
          name = ethnicityList[ethStat]["stat_name"]
          percentage = ethnicityList[ethStat]["percentage"]
          total = ethnicityList[ethStat]["total"]
          diversityInsertList["stats:" + name + "_percent"] = percentage
          diversityInsertList["stats:" + name + "_total"] = total

  dbConn = Connection()
  schoolTable = dbConn.table(tableName)
  schoolTable.insert(nces_id, diversityInsertList)


#Posts fields about a school to local hbase database
#NOTE: For scope issues, availibility of information
#and rate limiting reasons, I ignore all private schools
#@param school: json object
#@return void
def postSchoolToDB(school):

  if "private" in school['schooltype']:
    return

  #default connection is to 127.0.0.1:8085,
  #other hostnames and port can be specified as
  # Connection(<hostname>,<port>)
  dbConn = Connection()

  #Check if proper tables are setup
  #If not, call setup method
  setupDB(dbConn)

  schoolTable = dbConn.table(tableName)

  #check if the school has unique api id, if it doesn't, no further information can be gained by it
  if 'nces_id' in school and (school['nces_id'] != '' or school['nces_id'] != 'None'):
    key = school['nces_id']
  else:
    return

  debugPrint(str(schoolTable.columns()))
  #declare a new dictionary object
  schoolInsertList = {}
  for var in school:
    #lookup json value to column name mapping
    debugPrint(str(var) + " : " + str(school[var]) + '\n')
    #for each variable in the school
    #check if it matches a desired database column
    #if so, add it to the set of key/values for posting
    if var in jsonToDBMapping:
      val = school[var]
      if var in embeddedTotals:
        val = school[var]["total"]
      schoolInsertList[jsonToDBMapping[var]] = val

  if key != None:
    schoolTable.insert(key, schoolInsertList)
    nces_id = school['nces_id']
    idList.append(nces_id)
    postDiversityToDB(nces_id)
    postTestScoresToDB(nces_id)
  debugPrint('Done posting to DB')

#Function to query test scores from api.education.com
#@param nces_id of the school being queried
def postTestScoresToDB(nces_id):
  testJsonData = makeAPICall("getTestScores", "nces_id=" + nces_id)

  dbTestScoreList = {}
  if "school" in testJsonData:
    testGradesList = testJsonData["school"]

    for testGradeListIndex in range(0, len(testGradesList)):

      if testGradesList[testGradeListIndex]["testname"] == "DC-CAS Results":
        subject = testGradesList[testGradeListIndex]["subject"]
        percentage = testGradesList[testGradeListIndex]["score"]["percentage"]
        dbTestScoreList["tests:" + subject] = percentage
        debugPrint("Found score for " + nces_id + " : " + subject + " -- percent: " + str(percentage))   

  dbConn = Connection()
  schoolTable = dbConn.table(tableName)
  schoolTable.insert(nces_id, dbTestScoreList)


#function that will query hbase and iterate through key
#attributes and check if they are present and properly formatted
def measureCleanliness():
  #I define cleanliness as possessing all the values I desire for possible analysis
  #I want at a minimum african american percentage at each school, hispanic percentage
  #at each school and I want the test scores for those schools
  #I define a cleanliness metric as number of schools in dc for which I have all of those
  #attributes over the number of all schools
  #I track these numbers using global variables and then calculate them across the data
  #set in this functions
  dbConn = Connection()
  schoolsTable = dbConn.table(tableName)
  globalAttributeErrorCount = 0
  globalAttributeTotalCount = 0

  #TEST
  with open("schoolIds.txt") as f:
    idList = f.read().splitlines()

  selectedAttributes = {"stats:enrollment":{"error":0, "total":0}, "stats:s_t_ratio":{"error":0, "total":0}, "addr:longitude":{"error":0, "total":0}, "addr:latitude":{"error":0, "total":0}, "addr:zip":{"error":0, "total":0}, "stats:enrollmentGroupBin":{"error":0, "total":0}, "addr:schoolname":{"error":0, "total":0}, "stats:Black_percent":{"error":0, "total":0}}
  for schoolIdIndex in range(0, len(idList)):
    tableData = schoolsTable.fetch(idList[schoolIdIndex])
    if tableData == None:
      print "Error: Table fetch returned null"
      continue

    for col in tableData:
      for subCol in tableData[col]:
        debugPrint(str(subCol) + " : " + str(tableData[col][subCol]))
        combinedColumnStr = str(col) + ":" + str(subCol) 
        if tableData[col][subCol] == '' or tableData[col][subCol] == None or tableData[col][subCol] == "None":
          globalAttributeErrorCount += 1
          if combinedColumnStr in selectedAttributes:
            selectedAttributes[combinedColumnStr]["error"] += 1

        globalAttributeTotalCount += 1
        if combinedColumnStr in selectedAttributes:
            selectedAttributes[combinedColumnStr]["total"] += 1

  fileWriter = open("cleanStats.txt", "w+")
  fileWriter.write("Table: " + tableName)
  fileWriter.write("Number of schools: " + str(len(idList)))
  fileWriter.write("Total attributes collected: " + str(globalAttributeTotalCount) + "\n")
  fileWriter.write("Total Invalid attributes: " + str(globalAttributeErrorCount)+ "\n")
  fileWriter.write("Percentage invalid: " + str(((float(globalAttributeErrorCount)/float(globalAttributeTotalCount)) * 100)) + "% \n")  
  for attr in selectedAttributes:
    fileWriter.write("Attribute: " + str(attr) + "  : \n")
    fileWriter.write("Total attribute count: " + str(selectedAttributes[attr]["total"]) + "\n")
    fileWriter.write("Total attribute error count: " + str(selectedAttributes[attr]["error"]) + "\n")
    fileWriter.write("Percentage: " + str((float(selectedAttributes[attr]["error"])/float(selectedAttributes[attr]["total"])) * 100) + " % \n")
  return

#function to write out the school id numbers of each school queried
#in this script
def outputIDLists():
  idWriter = open("schoolIds.txt", "w+")
  for index in range(0, len(idList)):
    idWriter.write(idList[index] + "\n")
  return

#function to determine N number of bins of schools by their enrollment size
#This function uses equi-depth binning so that each bin contains roughly the same
#number of schools
#NOTE: I am submitting this algorithms for MOST INEFFICIENT ALGORITHM ON THE PLANET
#because I am very very tired
def calculateBinning():
  #I use equidepth binning to assign a size category to the school
  #based on enrollment size
  #NOTE: the fetch_all() function was taking a very long time to return
  #hence I use the auxiliary file I create earilier and query the database one
  #row at a time - this slow speed is liekly related to the development computer
  fileReader = open("schoolIds.txt", "r+")
  dbConn = Connection()
  schoolTable = dbConn.table(tableName)
  enrollmentList = []
  for schoolId in fileReader:
    schoolId = schoolId.strip()
    debugPrint("Looking at id: " + str(schoolId))
    schoolRow = schoolTable.fetch(schoolId, ["stats"])
    if 'stats' in schoolRow:
      if 'enrollment' in schoolRow['stats']:
        enrollmentList.append(schoolRow['stats']['enrollment'])
        debugPrint("Adding school: " + str(schoolRow['stats']['enrollment']))

  fileReader.close()
  #sort the list so that it is easy to determine the bucket boundaries
  enrollmentList.sort()

  numBins = 5
  numSchools = len(enrollmentList)
  binUpperBounds = []

  #Setup the bin boundaries by adding the upper bound of the bin to a list
  #Bin 1 will be from 0 to the first 1/n th part of the list, etc.
  for i in range(0,numBins):
    binUpperBounds.append(enrollmentList[(i+1) * (numSchools/numBins) - 1])
  debugPrint(binUpperBounds)

  #Iterate through the set of schools again
  fileReader = open("schoolIds.txt", "r+")
  for schoolId in fileReader:
    schoolId = schoolId.strip()
    schoolRow = schoolTable.fetch(schoolId, ["stats"])
    
    debugPrint('Requesting school' + str(schoolId))
    if schoolRow == None:
      continue

    if 'enrollment' in schoolRow['stats']:
      binAssignment = 0
      enrollCount = schoolRow['stats']['enrollment']
      debugPrint("Enrolled: " + str(enrollCount))
      for i in range (0,numBins):
        if enrollCount > binUpperBounds[i]:
          continue
        binAssignment = i + 1
        break
      if enrollCount > binUpperBounds[numBins- 1]:
        binAssignment = numBins
      
      debugPrint("Bin assignment for school " + str(schoolId) + " with enrollment: " + str(enrollCount) + " is " + str(binAssignment))      
      schoolTable.insert(schoolId, {"stats:enrollmentGroupBin": binAssignment})

  return 

#script for accessing database of degree awarded by secondary universities
#includes major type and ethnicity data
#NOT IMPLEMENTED FOR THIS PROJECT
def makeUniversityCall():
  url = "https://inventory.data.gov/api/action/datastore_search?limit=5&q=title:jones"

#script for loading in data from downloaded csv file on budget and expediture
#on teachers for DC public schools
#param financeFile - path to csv file of finance information
def loadFinanceDataToDB(financeFile):
  dbConn = Connection()
  schoolTable = dbConn.table(tableName)
  with open(financeFile, "rb") as csvFile:
    financeReader = csv.reader(csvFile)
    indexToRowMapping = {}
    categoryToColumnNameMapping = {
    "Personnel salaries at school level - total":"stats:totalSalaries",
    "Non-personnel expenditures at school level":"stats:nonPersonnelExpediture",
    "Personnel salaries at school level - teachers only":"stats:teacherSalaries"
    }
    firstRowFlag = True

    for row in financeReader:
      print row

      #handle loading in header information
      if firstRowFlag:
        indexCount = 0
        for var in row:
          print var
          indexToRowMapping[indexCount] = var
          indexCount += 1
        firstRowFlag = False
        continue #this refers back to row 'for' loop

      schoolID = ''
      insertKVPair = {}
      #handles all rows after the first
      for varIndex in range(0,len(row)):
        if indexToRowMapping[varIndex] == 'ID':
          schoolID = row[varIndex]
        if indexToRowMapping[varIndex] == 'Category':
          if row[varIndex] in categoryToColumnNameMapping:
            #get the next column for the value, map it to a database column
            insertKVPair[categoryToColumnNameMapping[row[varIndex]]] = row[varIndex + 1]
      if schoolID != '' and insertKVPair:
        debugPrint("Inserting : " + str(insertKVPair))
        schoolTable.insert(schoolID, insertKVPair)   
  return


# MAIN SCRIPT - ALL CODE LAUNCHES FROM HERE

makeAPICall("schoolSearch", "state=DC&")
#makeAPICall("schoolSearch", "state=MD&")
#makeAPICall("schoolSearch", "state=VA&")

outputIDLists()
calculateBinning()
loadFinanceDataToDB("schoolFinanceDC.csv")
measureCleanliness()