
#Remember to run this using python27, 64 bit. I typically run in from the python interpretter that comes with SPSS.

import spss, SpssClient,spssaux
import os
import re
import sys
import sqlite3
import time
import glob
import fileinput

file_name = sys.argv[1]
dimensions_list = sys.argv[2]

dimensions = dimensions_list.split('|')

SpssClient.StartClient()
    
#import spss .sav file
spss.Submit("GET FILE='" + os.getcwd() + "\\" + file_name + ".sav'.")

#list of variable names and order
primary_variable_list = []
for i in range(spss.GetVariableCount()):
    primary_variable_list.append(spss.GetVariableName(i))

#text for creating table which will be used to create the input file, one column for each item in the dimension list, plus two extra columns for survey variables and values
#generic column names are given to avoid any errors if the fields in the file are numeric or duplicated
sql_list = []
z =0
for item in dimensions:
    z += 1
    sql_list.append("field"+str(z)+" TEXT")
sql_str = "Create table response ("+', '.join(sql_list)+", survey_variable TEXT, survey_value NUMBER)"

#remove from the dimension list any variables which are not in the spss file
dimensions_clean = []
for dim in dimensions:
    if dim in primary_variable_list:
        dimensions_clean.append(dim)

#find the index of every variable in the dimension list which is not in the list of variables in the SPSS file
#this is used later to add null values in the input file
indices = [i for i, x in enumerate(dimensions) if x not in primary_variable_list]

constant_list = []
for item in dimensions:
    if item not in primary_variable_list:
        constant_list.append(item)

#text for inserting into table created above, the number of ?s is determined by the length of the dimension list passed as an argument
value_insert = []
size = len(sql_list)
size2=size+2
for i in range(size2):
    value_insert.append("?")
values = "insert into response values ("+', '.join(value_insert)+")"

def get_survey_metadata(file_name):
    """
    Uses an SPSS .sav file to generate a file with survey question metadata.
    
    INPUTS:
        file_name: Takes name of spss .sav file as an argument,
    OUTPUTS:
        [file_name]_values_input.txt: A txt file with survey question metadata, value labels, etc.
    """
    print file_name
    
    SpssClient.StartClient()
    
    #import spss .sav file
    spss.Submit("GET FILE='" + os.getcwd() + "\\" + file_name + ".sav'.") #This will need to be changed to get the file name from the argument, right?
    
    #output file
    f2 = open(os.getcwd() + "\\" + file_name + "_values_input.txt", 'w') 
    
    sdict = spssaux.VariableDict()

    #list of variable names and order
    variable_list = []
    for i in range(spss.GetVariableCount()):
        variable_list.append(spss.GetVariableName(i))

    #create data for survey.value 
    f2.write("")
    i=0 
    for var in sdict.expand(variable_list):
        question_label = str(spss.GetVariableLabel(i).encode('ascii','ignore'))
        
        #try to get stems
        stem =""
        #index_of_delimiter = question_label.find("-")
        #if (index_of_delimiter != -1) and (var!='work_ftpt') and (var!='school_ft') :
        #	stem = question_label[:index_of_delimiter]
        #	question_label = question_label[index_of_delimiter+1:]
        #print index_of_delimiter
        vallabs = sdict[sdict.VariableIndex(var)].ValueLabels
        for val,lab in vallabs.iteritems():
            f2.write(var +"|"+ val +"|" + (lab.upper()).encode('ascii','ignore') + "|" + question_label + "|" + stem +  "\n")
        i=i+1

    f2.close()
        

def reshape_survey_data(file_name):
    """
    Reshapes responses in a .sav file.  Currently, the id variable (what we are pivoting on is hardcoded since it doesn't change).
    
    INPUTS:
        file_name: Takes name of spss .sav file as an argument,
    OUTPUTS:
        [file_name]_nominal_survey_responses_input.dat: A txt file with a nomimal responses.  
    """
    SpssClient.StartClient()
    
    #import spss .sav file
    spss.Submit("GET FILE='" + os.getcwd() + "\\" + file_name + ".sav'.")
    
    #list of variable names only for nominal variable
    variable_list=[] 
    for i in range(spss.GetVariableCount()):
        print spss.GetVariableName(i)
        dimensionList = dimensions_clean
        dimensionListUpper = []
        for var in dimensionList:
            dimensionListUpper.append(var.upper())
        if ((spss.GetVariableType(i)==0) & (spss.GetVariableName(i).upper() not in dimensionListUpper)):
            variable_list.append(spss.GetVariableName(i))
        keep_list = " ".join(dimensionList)
    variable_list_text = ' '.join([str(i) for i in variable_list])
    print variable_list_text

    #import spss .sav file
    spss.Submit("DATASET NAME DataSet1 WINDOW=FRONT.")
    
    spss_string= "VARSTOCASES /ID=id /MAKE trans1 FROM " + variable_list_text + "  /INDEX=Index1(trans1) /KEEP="+keep_list+" /NULL=KEEP."
    print spss_string
    spss.Submit(spss_string)
    spss.Submit("rename variables (trans1=value)(Index1=var_name)")
    spss.Submit("delete variables id")
    spss.Submit("SAVE TRANSLATE OUTFILE='"+ os.getcwd() +"\\" + file_name + "_nominal_survey_responses_input.dat' /TYPE=TAB /ENCODING='UTF8' /MAP /REPLACE /FIELDNAMES /CELLS=VALUES.")
    
    #import spss .sav file
    spss.Submit("GET FILE='" + os.getcwd() + "\\" + file_name + ".sav'.")
    
def merge_survey_data(survey):  
    """
    Joins the question metadata and response data files together.  This is a good place for additional cleaning via update sql.
    
    INPUTS:
        survey: Takes name of spss .sav file as an argument,
    OUTPUTS:
        mitx_survey_[survey ]: File for loading.  Combines responses and question metadata. 
    """    
    
    #Connect
    connect = sqlite3.connect('survey.db')
    cursor = connect.cursor()
    
    #Create
    cursor.execute("Drop table if exists response")
    cursor.execute(sql_str)
    
    cursor.execute("Drop table if exists value")
    cursor.execute("""Create table value (
                    SURVEY_VARIABLE VARCHAR2(64) NOT NULL,
                    SURVEY_VALUE NUMBER(5),
                    SURVEY_VALUE_LABEL VARCHAR2(200),
                    SURVEY_QUESTION_LABEL VARCHAR2(256),
                    SURVEY_STEM_TEXT VARCHAR2(1000)
               )""")
    
    cursor.execute("create index idx1 on response(SURVEY_VARIABLE)")
    cursor.execute("create index idx2 on value(SURVEY_VARIABLE)")
    
    #Load
    infile = open(os.getcwd() + "\\" + survey + "_nominal_survey_responses_input.dat")
    for line in infile.readlines()[1:]:
        row = line.replace("\n","").split("\t")
        counter = -1
        for elm in indices:
            counter += 1
            row.insert(elm, constant_list[counter])
        cursor.execute(values,row)
    infile.close()
    infile = open(os.getcwd() + "\\" + survey + "_values_input.txt")
    for line in infile.readlines():
        row = line.replace("\n","").split("|")
        cursor.execute("insert into value values (?,?,?,?,?)",row)
    infile.close()
    
    #alter table response to add additional columns
    cursor.execute("alter table response add column SURVEY_VALUE_LABEL VARCHAR2(200)")
    cursor.execute("alter table response add column SURVEY_QUESTION_LABEL VARCHAR2(256)")
    cursor.execute("alter table response add column SURVEY_STEM_TEXT VARCHAR2(1000)")
    
    #Merge
    cursor.execute("update response set SURVEY_VALUE_LABEL = (select value.SURVEY_VALUE_LABEL from value where value.SURVEY_VALUE=response.SURVEY_VALUE and value.SURVEY_VARIABLE = response.SURVEY_VARIABLE)")
    connect.commit()
    
    cursor.execute("update response set SURVEY_QUESTION_LABEL = (select value.SURVEY_QUESTION_LABEL from value where value.SURVEY_VARIABLE = response.SURVEY_VARIABLE)")
    connect.commit()
    
    cursor.execute("update response set SURVEY_STEM_TEXT = (select value.SURVEY_STEM_TEXT from value where value.SURVEY_VARIABLE = response.SURVEY_VARIABLE)")
    connect.commit()
    
    #export
    cwd = os.getcwd()
    print cwd
    outfile=open(os.getcwd() + "\\"+file_name+"_Input.txt","w")
    
    #header - Just for Jon to do mockup Tableau
    #outfile.write("INSTITUTION_KEY\tACADEMIC_YEAR_KEY\tGENDER_CODE\tBIRTH_YEAR\tCITIZENSHIP_CODE\tIS_HISPANIC\tIS_NATIVE_AMERICAN\tIS_HAWAIIAN\tIS_ASIAN\tIS_AFRICAN_AMERICAN\tIS_WHITE\tDEGREE_CODE\tCIP_KEY\tCASEID_LOCAL\tCASEID_UNIQUE\tSURVEY_VARIABLE\tSURVEY_VALUE\tSURVEY_VALUE_LABEL\tSURVEY_QUESTION_LABEL\tSURVEY_STEM_TEXT\n")
    
    cursor.execute("select * from response")
    for row in cursor:
        outrow =""
        for cell in row:
            #clean up null whitespace in responses
            if (str(cell) == " "):
                cell = str(cell).replace(" ","")
            outrow = outrow + str(cell) + "\t"
        outfile.write(outrow + "\n")
    outfile.close()
    
    connect.close()

# path=os.getcwd()
# dirList=os.listdir(path)
global_start_time = time.clock()
#log_file = open(os.getcwd() + "\\log" + str(global_start_time),"w")
log_file = open(os.getcwd() + "\\log.txt","w")



filename = file_name
log_file.write(str(file_name) + "\n-------------------------------------------------------\n")
#print filename[0]
print file_name
        
file_start_time = time.clock()
#spsspython = 'C:\\\"Program Files\"\\IBM\\SPSS\\Statistics\\22\\Python\\python.exe'         #Shortcut for using SPSS' python installization to run the scripts

#1. get survey question metadata
step_start_time = time.clock()
get_survey_metadata(file_name)
log_file.write("\tSurvey Metadata(seconds)\t" + str(time.clock() - step_start_time) +"\n")
            
#2. run the reshape
step_start_time = time.clock()
reshape_survey_data(file_name)
log_file.write("\tData Reshape(seconds)\t" + str(time.clock() - step_start_time) +"\n")
        
#3. run merge
step_start_time = time.clock()
merge_survey_data(file_name)
log_file.write("\tData Merge(seconds)\t" + str(time.clock() - step_start_time) +"\n")
        
log_file.write("TOTAL SURVEY(seconds)\t" + str(time.clock() - file_start_time) +"\n\n")




