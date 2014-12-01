
import json
import csv
#http://www.census.gov/did/www/saipe/downloads/sd10/README.txt
#http://datacenter.kidscount.org/data/tables/3-teen-births-by-race#detailed/2/10,22,48/false/133/10,11,9,12,1,13/250,249
#U.S. Department of Education, National Center for Education Statistics, Common Core of Data (CCD), "Local Education Agency (School District) Universe Survey", 2010-11 v.2a, 2011-12 v.1a; "Survey of Local Government Finances, School Systems (F-33)", 2010-11 (FY 2011) v.1a.
#11 DC
#24 MD
#51 VA


districtsDict = {}

def readPovertyFile(fileName):
  
  with open(fileName) as f:
    line = f.read()
    Dlines = line.split("\n")

    for districtLine in Dlines:
      state = districtLine[:2]
      districtId = districtLine[3:8]
      districtName = districtLine[9:81]
      totalPop = districtLine[82:90]
      relevantPop = districtLine[91:99]
      estNumPoverty = districtLine[100:108]

      #the first 2 numbers of the fill id have the state id as a prefix
      districtId = state + districtId

      if state == 51:
        state = "VA"

      if state == 11:
        state = "DC"

      if state == 24:
        state = "MD"
       

      print " ----- "
      print  state + "," + districtId + ","+ districtName + "," + totalPop + "," + relevantPop + "," + estNumPoverty
      
      if districtId == "":
        continue

      if not districtId in districtsDict:
        districtsDict[districtId.strip()] = {}

      districtsDict[districtId]["state"] = state.strip()
      districtsDict[districtId]["districtId"] = districtId.strip()
      districtsDict[districtId]["totalPop"] = totalPop.strip()
      districtsDict[districtId]["childrenPop"] = relevantPop.strip()
      districtsDict[districtId]["numInPoverty"] = estNumPoverty.strip() 


def readFinanceCsvFile(fileName):
  with open(fileName, 'r+') as csvfile:
    csvText = csv.reader(csvfile, delimiter=",")
    first = True
    numNew = 0
    existNum = 0
    for fileRow in csvText:
      if first:
        first = False
        continue
      #format of the csv per row is
      #district name 0
      #state 1
      #state abbr 2
      #district ID 3
      #district latitude 4
      #district longitude 5
      #Instruction expendature total in dollars 6
      #Instruction salary total 7
      #Teacher salaries regular education programs 8
      #total expendature PER student 9
      row = fileRow[0].split(";")
      name = row[0]
      stateAbbr = row[2]
      districtId = row[3].strip()
      lat = row[5]
      lon = row[6]
      instructExpend = row[7]
      instructSalary = row[12]
      totalSalaryEdu = row[13]
      expendPerStudent = row[10] 

      #print name + "," + stateAbbr + "," + districtId + "," + lat + "," + lon + "," + instructExpend + "," + instructSalary + "," + totalSalaryEdu + "," + expendPerStudent
      #print "Did:" + str(districtId) +" " + name

      if not districtId in districtsDict:
        districtsDict[districtId] = {}
        districtsDict[districtId]["districtId"] = districtId.strip()
        districtsDict[districtId]["state"] = stateAbbr
        numNew += 1
        #print "Num New" + str(numNew) + "--" + str(districtId) + "--" + name
      else:
        existNum += 1
        #print "Found existing id " + str(districtId)
      districtsDict[districtId]["latitude"] = lat
      districtsDict[districtId]["longitude"] = lon
      districtsDict[districtId]["instructExpend"] = instructExpend
      districtsDict[districtId]["instructSalary"] = instructSalary
      districtsDict[districtId]["totalSalaryEdu"] = totalSalaryEdu
      districtsDict[districtId]["expendPerStudent"] = expendPerStudent


def outputDistrictsJson(fileName):
  districtsList = []
  #print "Length of districtsList is " + str(len(districtsDict)) 
  for dist in districtsDict:
    #print districtsDict[dist]["districtId"]
    districtsList.append(districtsDict[dist])

  with open(fileName, 'w') as outfile:
    json.dump(districtsList, outfile)


readPovertyFile("mdPoverty.txt")
readPovertyFile("dcPoverty.txt")
readPovertyFile("vaPoverty.txt")
readFinanceCsvFile("finance2.csv")

outputDistrictsJson("districts.json")