
# Remember to run this using python27, 64 bit. I typically run in from the python interpretter that comes with SPSS.
# Usage: //Path//to//SPSS//python survey_converter.py survey_data.sav dim1|dim2|dim3|dim4
# The first argument is the path to the SPSS version of Python. This is used because it plays well with the
# SPSS packages necessary.
# The second argument is the name of the script
# The third argument is the name of the SPSS file
# The fourth argument is the pipe ( this thing-> | ) delimited list of "dimensions" in the lingo of survey
# conversions, dimensions are the attributes of the respondents that you want to be available on any row
# so these are usually demographics, the things you would want to crosstab other survey variables by


import spss, SpssClient,spssaux
import os
import re
import sys
import sqlite3
import time
import glob
import fileinput
import zipfile
import operator
import uuid
import nose


def dimension_setup(dims, primary_vars):
    """
    dims: list, list of variable names to have unpivoted
    primary_vars: list, list of all variables in SPSS datafile
    uses the user-specified dimensions to create SQL table creation and INSERT language
    and checks for the existence of those dimensions in the SPSS file
    returns SQL code for insert and create, dictionary of variables not in
    the SPSS file with var names as keys and index in dimension list as values
	and a list of the dimensions that are in the file
    """
    null_vars = {}
    dims_clean = None
    if dims_clean is None:
        dims_clean = []
    sql_list = None
    if sql_list is None:
        sql_list = []
    val_insert = None
    if val_insert is None:
        val_insert = []
    for i in range(len(dims)):
        if dims[i] not in primary_vars:
            null_vars[dims[i]] = i
        else:
            dims_clean.append(dims[i])
        sql_list.append("field"+str(i)+" TEXT")
        val_insert.append("?")
    create_stmnt = "Create table response ("+', '.join(sql_list)+", survey_variable TEXT, survey_value NUMBER)"
    for i in range(2):
        val_insert.append("?")
    insert_stmnt = "insert into response values ("+', '.join(val_insert)+")"
    return insert_stmnt, create_stmnt, null_vars, dims_clean

def get_survey_metadata(file_name, clean_dims):
    """
    Uses an SPSS .sav file to generate a file with survey question metadata.
    
    INPUTS:
        file_name: Takes name of spss .sav file as an argument,
        clean_dims: list of dimensions in the SPSS file, to be used to save off
        the labels for these dimensions
    OUTPUTS:
        [file_name]_values_input.txt: A txt file with survey question metadata, value labels, etc.
        dimension_labels: a dictionary of dictionaries for labeling dimensions
    """
    print file_name
    
    SpssClient.StartClient()
    
    #import spss .sav file
    spss.Submit("GET FILE='" + os.getcwd() + "\\" + file_name + ".sav'.")
    
    #output file
    f2 = open(os.getcwd() + "\\" + file_name + "_values_input.txt", 'w') 
    
    sdict = spssaux.VariableDict()

    #list of variable names and order
    variable_list = None
    if variable_list is None:
        variable_list = []
    for i in range(spss.GetVariableCount()):
        variable_list.append(spss.GetVariableName(i))

    #create data for survey.value 
    f2.write("")
    i=0
    dimension_labels = {}
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
        if not vallabs:
            f2.write(var +"|None|None|" + question_label + "|" + stem +  "\n")
        if var in clean_dims:
            dimension_labels[var.upper()] = vallabs
        for val,lab in vallabs.iteritems():
            f2.write(var +"|"+ val +"|" + (lab.upper()).encode('ascii','ignore') + "|" + question_label + "|" + stem +  "\n")
        i=i+1
    f2.close()
    return dimension_labels

def reshape_survey_data(file_name, clean_dim_list):
    """
    Reshapes responses in a .sav file.  Currently, the id variable (what we are pivoting on is hardcoded since it doesn't change).
    
    INPUTS:
        file_name: Takes name of spss .sav file as an argument,
        clean_dim_list: list, dimensions that were specified and are in the SPSS file
    OUTPUTS:
        [file_name]_nominal_survey_responses_input.dat: A txt file with a nomimal responses.  
    """
    SpssClient.StartClient()
    
    #import spss .sav file
    spss.Submit("GET FILE='" + os.getcwd() + "\\" + file_name + ".sav'.")
    
    #list of variable names only for nominal variable
    variable_list = None
    if variable_list is None:
        variable_list = []
    dimensionListUpper = None
    if dimensionListUpper is None:
        dimensionListUpper = []
    for i in range(spss.GetVariableCount()):
        print spss.GetVariableName(i)
        for var in clean_dim_list:
            dimensionListUpper.append(var.upper())
        if ((spss.GetVariableType(i)==0) & (spss.GetVariableName(i).upper() not in dimensionListUpper)):
            variable_list.append(spss.GetVariableName(i))
        keep_list = " ".join(clean_dim_list)
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


def merge_survey_data(survey,dim_insert_dict,insert_statement, dim_labels, dimclean):  
    """
    Joins the question metadata and response data files together.  This is a good place for additional
	cleaning via update sql.
    
    INPUTS:
        survey: Takes name of spss .sav file as an argument,
        dim_insert_dict: dictionary with keys as variables not in SPSS file, values indices of
        these variables in original user-specified dimension list, this is the product of the
        dimension_setup function
        insert_statement: sqlite code to insert a row of data, product of the dimension_setup fn.
        dim_labels: a dictionary of dictionaries for labeling the dimensions, product of the 
        get_survey_metadata function
        dimclean: list of dimensions actually in SPSS file, product of the dimension_setup function
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
    dimlen = len(dimclean)
    infile = open(os.getcwd() + "\\" + survey + "_nominal_survey_responses_input.dat")
    for line in infile.readlines()[1:]:
        row = line.replace("\n","").split("\t")
        newrow = None
        if newrow is None:
            newrow = []
        for i in range(len(row[:dimlen])):
            if dimclean[i].upper() in dim_labels.keys() and dim_labels[dimclean[i].upper()]:
                try:
                    label = dim_labels[dimclean[i].upper()][row[i]]
                except KeyError:
                    label = row[i]
                if label:
                    newrow.append(label)
                else:
                    newrow.append(row[i])
            else:
                newrow.append(row[i])
        sorted_d = sorted(dim_insert_dict.items(),key=operator.itemgetter(1))
        for pair in sorted_d:
            newrow.insert(pair[1], pair[0])
        newrow.append(row[-2])
        newrow.append(row[-1])
        cursor.execute(insert_statement,newrow)
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
    outfile=open(os.getcwd() + "\\"+survey+"_Input.txt","w")
    
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


def zip_file(file_name):
	file = zipfile.ZipFile(file_name + ".zip", "w")
	file.write(file_name, os.path.basename(file_name), zipfile.ZIP_DEFLATED)
	file.close()

def setup_func():
	"""
	setup for the nose tests
	creates temporary files
	to be used in tests
	"""
	test_filename = "temp_"+str(uuid.uuid4())+"_nosetest.txt"
	f = open(test_filename,"w")
	f.write("testing file")
	f.close()

def teardown_func():
	"""
	teardown for the nose tests
	deletes the temporary files
	generated by setup_func()
	"""
	for f in glob.glob("temp_*_nosetest.txt"):
		os.remove(f)
	for f in glob.glob("temp_*_nosetest.txt.zip"):
		os.remove(f)
	try:
		os.remove("var_label_val_label_testing_values_input.txt")
	except:
		pass
	try:
		SpssClient.StopClient()
	except:
		pass
	
	
@nose.with_setup(setup_func,teardown_func)
def zip_test():
	"""
	test that the zip_file() function creates a file 
	"""
	for f in glob.glob("temp_*_nosetest.txt"):
		zip_file(f)
		assert(os.path.isfile(f+".zip"))
		

def list_of_dimensions_in_file_all_in_file_test():
	"""
	tests that the dimension_setup correctly
	returns the list of dimensions that exist in the file
	Tests the case where all of the supplied dimensions
	are in the file
	"""
	dimensions = ["name", "age", "sex"]
	all_variables = ["name", "age", "sex", "var1", "var2", "var3"]
	null1, null2, null3, clean_list = dimension_setup(dimensions, all_variables)
	assert clean_list == dimensions

def list_of_dimensions_in_file_some_missing_test():
	"""
	tests that the dimension_setup correctly
	returns the list of dimensions that exist in the file
	Tests the case where some of the supplied dimensions
	are not in the file
	"""
	dimensions = ["name", "age", "sex", "foo"]
	clean_dimensions = dimensions[:-1]
	all_variables = ["name", "age", "sex", "var1", "var2", "var3"]
	null1, null2, null3, clean_list = dimension_setup(dimensions, all_variables)
	assert clean_list == clean_dimensions

def dimensions_not_in_file_returned_as_dict_test():
	"""
	tests that dimension_setup correctly
	returns a dictionary of the dimensions that
	are not in the SPSS file with the variable names
	as keys and the index in the list of values as keys
	"""
	dimensions = ["name", "age", "sex", "foo"]
	all_variables = ["name", "age", "sex", "var1", "var2", "var3"]
	null1, null2, invalid_dimensions, null3 = dimension_setup(dimensions, all_variables)
	assert invalid_dimensions == {"foo" : 3}

def dimension_setup_insert_statement_test():
	"""
	tests that the dimension_setup function
	returns a correct SQL statement for inserting
	values into a sqlite database
	"""
	dimensions = ["name", "age", "sex", "foo"]
	all_variables = ["name", "age", "sex", "var1", "var2", "var3"]
	sql_statement, null1, null2, null3 = dimension_setup(dimensions, all_variables)
	# the sql statement should be the length of all of the dimensions
	# plus two extra columns for the response value and response variable
	assert sql_statement == "insert into response values (?, ?, ?, ?, ?, ?)"

def dimension_setup_create_statement_test():
	"""
	tests that the dimension_setup function
	returns a correct SQL statement for creating
	the table of responses
	"""
	dimensions = ["name", "age", "sex", "foo"]
	all_variables = ["name", "age", "sex", "var1", "var2", "var3"]
	null1, sql_statement, null2, null3 = dimension_setup(dimensions, all_variables)
	# basically just tests that the create statement has the right number of fields
	# as the field names are generic and don't depend on inputs
	assert sql_statement == "Create table response (field0 TEXT, field1 TEXT, field2 TEXT, field3 TEXT, survey_variable TEXT, survey_value NUMBER)"

@nose.with_setup(teardown=teardown_func)
def metadata_dimension_labels_test():
	"""
	tests whether the get_survey_metadata()
	function returns the correct dictionary of
	dimensions with their labels
	NOTE: currently aims at a real SPSS file created for testing
	but probably should be converted to use a mock object
	make sure the specific tesing SPSS file exists in same directory
	for this test to succeed
	"""
	dimension_label_dict = get_survey_metadata("var_label_val_label_testing",["var1", "var3"])
	testing_dictionary =  {"VAR1" : {"1":"one", "2": "two", "3":"three"},
									"VAR3" : {"a":"the letter a", "b":"the letter b" }}
	assert dimension_label_dict == testing_dictionary

	
if __name__ == '__main__':	
	# SPSS file name, file name and script are supposed to be in the same directory to run correctly
	file_name = sys.argv[1]
	# split off the .sav extension to use as a name for the output file later
	file_name = file_name.split(".")[0]
	
	# List of dimensions (see detailed description in header)
	dimensions_list = sys.argv[2]
	dimensions = dimensions_list.split('|')
	
	SpssClient.StartClient()
		
	# import spss .sav file
	# Script must be in same directory as the SPSS file that is to be reshaped
	spss.Submit("GET FILE='" + os.getcwd() + "\\" + file_name + ".sav'.")
	
	# list of variable names and order
	primary_variable_list = []
	for i in range(spss.GetVariableCount()):
		primary_variable_list.append(spss.GetVariableName(i))
		
	values, sql_str, dimension_dict, dimensions_clean = dimension_setup(dimensions, primary_variable_list)

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
	dimension_labs = get_survey_metadata(file_name, dimensions_clean)
	log_file.write("\tSurvey Metadata(seconds)\t" + str(time.clock() - step_start_time) +"\n")
				
	#2. run the reshape
	step_start_time = time.clock()
	reshape_survey_data(file_name, dimensions_clean)
	log_file.write("\tData Reshape(seconds)\t" + str(time.clock() - step_start_time) +"\n")
			
	#3. run merge
	step_start_time = time.clock()
	merge_survey_data(file_name, dimension_dict ,values, dimension_labs, dimensions_clean)
	zip_file(file_name+"_Input.txt")
	log_file.write("\tData Merge(seconds)\t" + str(time.clock() - step_start_time) +"\n")
			
	log_file.write("TOTAL SURVEY(seconds)\t" + str(time.clock() - file_start_time) +"\n\n")
	
	SpssClient.StopClient()


