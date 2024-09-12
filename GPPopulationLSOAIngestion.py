#GP Populations LSOA Ingestion file created by Donald Maruta, Senior Geospatial Manager, NCL ICB, 28 May 24
#Quarterly publications in January, April, July and October will include Lower Layer Super Output Area (LSOA) populations
#Source ZIP file can be downloaded here https://digital.nhs.uk/data-and-information/publications/statistical/patients-registered-at-a-gp-practice

#Connect to AGOL
from arcgis.gis import GIS
gis = GIS("home")

#Import required modules
import arcpy, glob, zipfile, shutil, requests, csv, json
import pandas as pd
from zipfile import ZipFile
from arcgis.features import FeatureLayerCollection
from urllib import request

#Turn off field names table prefixes
arcpy.env.qualifiedFieldNames = False

#This creates a unique date time code for trouble shooting
todayDate = datetime.datetime.now().strftime("%y%m%d%y%H%M")
print(todayDate)

#Sets up folder variables
FGDBpath = '/arcgis/home/PracticePopulations/PracPops' + todayDate + '.gdb'
fldrPath = '/arcgis/home/PracticePopulations/'

#Create File GDB
arcpy.CreateFileGDB_management(fldrPath, 'PracPops' + todayDate + '.gdb')
tempTable = FGDBpath + "/tempTable"

#IMD2019 LSOA 2011
shpFile = '/arcgis/home/PracticePopulations/IMD2019_LSOA2011.shp'
outputShp = '/arcgis/home/PracticePopulations/PracPop.shp'

#Get name of ZIP file - looks for a ZIP file in the arcgis/home/PracticePopulations folder starting with gp
zipFile = (glob.glob("/arcgis/home/PracticePopulations/gp*.zip"))
strFile = str(zipFile) #Creates the file locations as a string
strFile = (strFile.strip("[']")) #Removes the ['] characters
print(strFile)

#Unzip the ZIP file
with zipfile.ZipFile(strFile, "r") as zip_ref:
    zip_ref.extractall(fldrPath)
print("Done!")

#Get name of CSV file - looks for a CSV file in the arcgis/home/PracticePopulations folder with ends with 'all'
csvFile = (glob.glob("/arcgis/home/PracticePopulations/*all.csv"))
strFile = str(csvFile) #Creates the file locations as a string
strFile = (strFile.strip("[']")) #Removes the ['] characters
print(strFile)

#Get GP Data from NHS Digital
#List of datasets
datasets = [
    {
        'filter': "OrganisationTypeID eq 'GPB'", #GPs
        'orderby': "geo.distance(Geocode, geography'POINT(-0.15874 51.6116)')",
        'top': 1000,
        'skip': 0,
        'count': True
    },
#Add more datasets as needed
]

#Specify the file paths where you want to save the CSV files
csv_file_paths = [
    "/arcgis/home/PracticePopulations/GPB.csv",
    #Add more file paths as needed
]

for dataset, csv_file_path in zip(datasets, csv_file_paths):
    response = requests.request(
        method='POST',
        url='https://api.nhs.uk/service-search/search?api-version=1',
        headers={
            'Content-Type': 'application/json',
            'subscription-key': '557d555fd712449f894e78e50a460000'
        },
        json=dataset
    )

    #Parse the response as JSON
    data = response.json()

    #Extract the required data from the JSON response
    output = []
    for item in data.get('value', []):
        output.append([
            item.get('OrganisationID'),
            item.get('NACSCode'),
            item.get('OrganisationName'),
            item.get('Postcode'),
            item.get('Latitude'),
            item.get('Longitude'),
            item.get('Contacts'),
            item.get('LastUpdatedDate'),
        ])

    #Open the CSV file in write mode
    with open(csv_file_path, 'w', newline='') as csvfile:
        #Create a CSV writer object
        csv_writer = csv.writer(csvfile)

        #Write the header row
        csv_writer.writerow(['OrganisationID', 'OCS_Code', 'OrganisationName', 'Postcode', 'Latitude', 'Longitude', 'Contacts', 'LastUpdatedDate'])

        #Write the output to the CSV file
        csv_writer.writerows(output)

    #Confirmation message
    print(f"Output saved as CSV: {csv_file_path}")

    #Load the CSV file into a pandas DataFrame
    df = pd.read_csv(csv_file_path, encoding = "cp1252")

    #Create a new column to store the extracted phone numbers
    df['PhoneNumber'] = ""

    #Iterate through each row in the DataFrame
    for index, row in df.iterrows():
        contacts_data = row['Contacts']
    
        #Check if the value is NaN (not a string)
        if isinstance(contacts_data, str):
            #Parse the JSON data
            parsed_data = json.loads(contacts_data)

            #Extract the telephone number
            telephone_number = parsed_data[0]["OrganisationContactValue"]

            #Update the "PhoneNumber" column with the extracted telephone number
            df.at[index, 'PhoneNumber'] = telephone_number

    #Drop the original "Contacts" column
    df.drop(columns=['Contacts'], inplace=True)

    #Save the modified DataFrame back to a CSV file
    df.to_csv(csv_file_path, index=False)

#Confirmation message
print("All datasets processed successfully.")

#Import CSV File into FGDB
arcpy.conversion.TableToGeodatabase(strFile, FGDBpath)

#Import GBP File into FGDB
arcpy.conversion.TableToGeodatabase("/arcgis/home/PracticePopulations/GPB.csv", FGDBpath)

#Import SHP File into FGDB
arcpy.conversion.FeatureClassToGeodatabase(shpFile, FGDBpath)

#Get name of Table in FGDB
arcpy.env.workspace = FGDBpath
tempTab = arcpy.ListTables()
tempTab = tempTab[0]
print(tempTab)

#Filter Out Unrequired locations
arcpy.conversion.TableToGeodatabase("/arcgis/home/PracticePopulations/PostCodeLookup_V3.csv", FGDBpath)
tempdata = arcpy.management.AddJoin("GPB", "Postcode", "PostCodeLookup_V3", "PostCode", "KEEP_COMMON")
arcpy.management.CopyRows(tempdata, "GPData")

#Delete unwanted columns
deleteMe = ["OBJECTID_1", "PostCode", "BNG_X", "BNG_Y", "OA21CD", "LSOA21CD", "LSOA21NM", "MSOA21CD", "MSOA21NM", "LTLA22CD", "LTLA22NM", "UTLA22CD", "UTLA22NM"]
arcpy.management.DeleteField("GPData", deleteMe)

#Add PRACTICE_NAME column
tempdata = arcpy.management.AddJoin("GPData", "OCS_Code", tempTab, "PRACTICE_CODE", "KEEP_COMMON")
arcpy.management.CopyRows(tempdata, "GPData2")

#Delete unwanted columns
#deleteMe = ["OBJECTID_1", "PUBLICATION", "EXTRACT_DATE", "PRACTICE_CODE", "PRACTICE_CODE_X", "PRACTICE_CODE_Y", "LSOA_CODE", "NUMBER_OF_PATIENTS"]
deleteMe = ["OBJECTID_1", "PUBLICATION", "EXTRACT_DATE", "PRACTICE_CODE", "PRACTICE_CODE_X", "PRACTICE_CODE_Y", "LSOA_CODE", "NUMBER_OF_PATIENTS", "SEX"]
arcpy.management.DeleteField("GPData2", deleteMe)

#Delete Duplicate Features
arcpy.management.DeleteIdentical("GPData2", "OCS_Code")

#Export GDB Table to CSV
arcpy.conversion.ExportTable("GPData2", "GPData.csv")

#Add NUMBER OF PATIENTS column
tempdata = arcpy.management.AddJoin("GPData2", "OCS_Code", tempTab, "PRACTICE_CODE", "KEEP_COMMON")
arcpy.management.CopyRows(tempdata, "GPData3")

#Delete Unrequired Columns
deleteMe = ["OrganisationID", "OCS_Code", "OrganisationName", "Latitude", "Longitude", "LastUpdatedDate", "PhoneNumber", "PostCode_1", "OBJECTID_1", "PUBLICATION", "PRACTICE_CODE_X", "PRACTICE_CODE_Y", "PRACTICE_NAME_1"]
arcpy.management.DeleteField("GPData3", deleteMe)

#Create a new field for the sum of patients by Practice
arcpy.analysis.Statistics("GPData3", "tempSum", [["NUMBER_OF_PATIENTS", "SUM"]], "PRACTICE_CODE")

#Join the sum of patients by Practice to the main Table
tempdata = arcpy.management.AddJoin("GPData3", "PRACTICE_CODE", "tempSum", "PRACTICE_CODE", "KEEP_COMMON")
arcpy.management.CopyRows(tempdata, "GPData4")

#Calculate Percentage Field (NUMBER OF PATIENTS / SUM NUMBER OF PATIENTS)
exp = "!NUMBER_OF_PATIENTS! / !SUM_NUMBER_OF_PATIENTS! * 100"
arcpy.management.CalculateField("GPData4", "PERCENTAGE", exp)

#Delete Unrequired Columns
deleteMe = ["OBJECTID_1", "PRACTICE_CODE_1", "FREQUENCY"]
arcpy.management.DeleteField("GPData4", deleteMe)

#Join the sum of patients by Practice to the main Table
tempdata = arcpy.management.AddJoin("IMD2019_LSOA2011", "LSOA11CD", "GPData4", "LSOA_CODE")
#tempdata = arcpy.management.AddJoin("GPData4", "LSOA_CODE", shpFile, "LSOA11CD")
arcpy.management.CopyFeatures(tempdata, "GPDataFC")

#Delete unrequired fields
deleteMe = ["LSOA_CODE", "OBJECTID_1", "PRACTICE_1", "EXTRACT__1", "PRACTICE_2", "LSOA_CODE_", "SEX_1", "NUMBER_OF1", "SUM_NUMB_1", "PERCENTA_1", "Shape_Leng"]
arcpy.management.DeleteField("GPDataFC", deleteMe)

#Export the FC to a SHP
arcpy.management.Rename("GPDataFC", "PracPop")
arcpy.conversion.FeatureClassToShapefile("PracPop", fldrPath)

#List of files in complete directory
finalName = "PracPop"
file_list = [finalName + ".shp", finalName + ".shx", finalName + ".dbf", finalName + ".prj"]
os.chdir(fldrPath)

#Create Zip file
shpzip = finalName + ".zip"
with zipfile.ZipFile(shpzip, 'w') as zipF:
    for file in file_list:
        zipF.write(file, compress_type=zipfile.ZIP_DEFLATED)

#Initial Publish LSOAs to AGOL
#item = gis.content.add({}, shpzip)
#published_item = item.publish()
#published_item.share(everyone=True)

#Initial Publish GPs to AGOL
#my_csv = ("/arcgis/home/PracticePopulations/GPData.csv")
#item_prop = {'title':'GPData'}
#csv_item = gis.content.add(item_properties=item_prop, data=my_csv)
#params={"type": "csv", "locationType": "coordinates", "latitudeFieldName": "Latitude", "longitudeFieldName": "Longitude"}
#csv_item.publish(publish_parameters=params)
#csv_item.publish(overwrite = True)

#Overwrite the existing service - PracPop LSOAs
search_result = gis.content.search(query=finalName, item_type="Feature Layer")
feature_layer_item = search_result[0]
feature_layer = feature_layer_item.layers[0]
feat_id = feature_layer.properties.serviceItemId
item = gis.content.get(feat_id)
item_collection = FeatureLayerCollection.fromitem(item)
#call the overwrite() method which can be accessed using the manager property
item_collection.manager.overwrite(finalName)
item.share(everyone=True)
update_dict = {"capabilities": "Query,Extract"}
item_collection.manager.update_definition(update_dict)
item.content_status="authoritative"

#Overwrite the existing service - GPData
feat_id = 'd376e5382bb849e8a96b23f8d40ffb42'
item = gis.content.get(feat_id)
item_collection = FeatureLayerCollection.fromitem(item)
#call the overwrite() method which can be accessed using the manager property
item_collection.manager.overwrite('/arcgis/home/PracticePopulations/GPData.csv')
#arcpy.management.DeleteRows(item)
item.share(everyone=True)
update_dict = {"capabilities": "Query,Extract"}
item_collection.manager.update_definition(update_dict)
item.content_status="authoritative"

#Code to delete unnecessary files

#Get a list of all subdirectories (folders) in the specified folder
folders = [f for f in os.listdir(fldrPath) if os.path.isdir(os.path.join(fldrPath, f))]

for folder in folders:
    folder = os.path.join(fldrPath, folder)
    shutil.rmtree(folder)

#List of files to preserve
files_to_preserve = ["IMD2019_LSOA2011.dbf", "IMD2019_LSOA2011.prj", "IMD2019_LSOA2011.shp", "IMD2019_LSOA2011.shx", "IMD2019_LSOA2011.zip", "PostCodeLookup_V3.csv"]

#Get a list of all files in the directory
all_files = glob.glob(os.path.join(fldrPath, "*"))

#Iterate over each file
for file_path in all_files:
    #Get the file name
    file_name = os.path.basename(file_path)
    
    #Check if the file name is not in the list of files to preserve
    if file_name not in files_to_preserve:
        #Delete the file
        os.remove(file_path)
        print(f"Deleted {file_name}")

print("All files except the specified ones have been deleted.")
