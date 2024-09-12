# GP-Population-LSOA-Ingestion

Python code desigend to be run in ArcGIS OnLine Notebook

This allows users to import GP Population Data at LSOA level (Open Source Data) has been updated since the previous run.

#GP Populations LSOA Ingestion file created by Donald Maruta - 28 May 24
#Quarterly publications in January, April, July and October will include Lower Layer Super Output Area (LSOA) populations
#Source ZIP file can be downloaded here https://digital.nhs.uk/data-and-information/publications/statistical/patients-registered-at-a-gp-practice

It carries out the following actions in the following order
 - Connect to AGOL
 - Import required modules
 = Sets up folder variables
 = Create File GDB
 - Get name of ZIP file - looks for a ZIP file in the arcgis/home/PracticePopulations folder starting with gp
 - Get name of CSV file - looks for a CSV file in the arcgis/home/PracticePopulations folder with ends with 'all'
 - Get GP Data from NHS Digital
 - Import CSV File into FGDB
 - Import GBP File into FGDB
 - Import SHP File into FGDB
 - Filter Out Unrequired locations
 - Delete Duplicate Features
 - Add NUMBER OF PATIENTS column
 - Create a new field for the sum of patients by Practice
 - Join the sum of patients by Practice to the main Table
 - Calculate Percentage Field (NUMBER OF PATIENTS / SUM NUMBER OF PATIENTS)
 - Join the sum of patients by Practice to the main Table
 - Export the FC to a SHP
 - Create Zip file
 - Initial Publish LSOAs to AGOL
 - Initial Publish GPs to AGOL
 - Overwrite the existing service - PracPop LSOAs
 - Overwrite the existing service - GPData
 - Code to delete unnecessary files
