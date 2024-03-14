import ast
import sys
from datetime import datetime
import json
import csv
import os

#Function for handling Quoted Values
def fquotes(v,val):
	quotes='\''
	if (v['Dtype']=='QSTR'):
			if (v['Type']=='SQL'):
				val=[quotes+str(i)+quotes for i in val]
			else:
				val=quotes+val+quotes
	#print(val)
	return val

##Fetching the variables
dbutils.widgets.text("FILE_NM", "test_param")
dbutils.widgets.text("PARAM_FILE", "test_param")
dbutils.widgets.text("PATH", "")
dbutils.widgets.text("GLOBAL_CONFIGS", "")
dbutils.widgets.text("GLOBAL_APP_PREFIX", "")

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson() 
try: 
	taskKey=(json.loads(ctx)['tags']['taskKey'])
except:	   
	taskKey=''
print("taskKey: ",taskKey)
if taskKey!= '':
	OVERWRITE_PARAMS="OVERWRITE_PARAMS_"+taskKey
	dbutils.widgets.text(OVERWRITE_PARAMS, "{}")
else:
	OVERWRITE_PARAMS="OVERWRITE_PARAMS"
	dbutils.widgets.text(OVERWRITE_PARAMS, "{}")
	
#Printing User Input Variables
print("FILE_NM: ",dbutils.widgets.get("FILE_NM"))
print("PARAM_FILE: ",dbutils.widgets.get("PARAM_FILE"))
print("PATH: ",dbutils.widgets.get("PATH"))
print("GLOBAL_CONFIGS: ",dbutils.widgets.get("GLOBAL_CONFIGS"))
print("GLOBAL_APP_PREFIX: ",dbutils.widgets.get("GLOBAL_APP_PREFIX"))
print(OVERWRITE_PARAMS)
print("OVERWRITE_PARAMS: ",dbutils.widgets.get(OVERWRITE_PARAMS))

##Reading Overwrite Params
sys_params=ast.literal_eval(dbutils.widgets.get(OVERWRITE_PARAMS))
#print(sys_params)


##Getting Param File from path
if dbutils.widgets.get("PATH") == "":
	raise Exception("Please provide valid Path")
# print(PARAM_FILE)
else:
	PARAM_FILE = str(dbutils.widgets.get("PATH")) + str(dbutils.widgets.get("PARAM_FILE"))

params = ast.literal_eval(dbutils.notebook.run(PARAM_FILE, 60))
ppqueries = params['post_processing_query']


##Getting System Full Load Ind if available:
if 'FULL_LOAD' in sys_params:
	if sys_params['FULL_LOAD']!='':
		params['flag_dict']['FULL_LOAD'] = sys_params['FULL_LOAD']

print("FULL_LOAD_IND is :",params['flag_dict']['FULL_LOAD'])

##Checking for FULL Load or INCREMENTAL Load
if params['flag_dict']['FULL_LOAD'] == 'Y':
	params = params['full_params']
else:
	params = params['incremental_params']


##Adding parameters from param file

print("Using following Params: ",params)
param_k = []
param_v = []
quotes='\''
for k, v in params.items():
	for a, b in v.items():
		if (a=='Type' and b in ['STATIC','SQL','SHELL','PYTHON']):
			param_k.append(k)
			if (b=='STATIC'):
				val=v['Value']
				val=fquotes(v,val)					 
				param_v.append(val)
			elif (b=='SQL'):
				result = spark.sql(v['Value'])
				val = result.rdd.flatMap(lambda x: x).collect()
				val= fquotes(v,val)
				param_v+=val
			elif (b=='PYTHON'):		
				#print("print("+v['Value']+")")
				exec('val = '+v['Value'])
				val=fquotes(v,val)
				param_v.append(val)
			elif (b=='SHELL'):
				stream = os.popen(v['Value'])
				val = stream.read().strip('\n')
				val= fquotes(v,val)
				param_v.append(val)

param_load = dict(zip(param_k, param_v))
print(param_load)

##Replacing Available Params with Sys Params
if 'FULL_LOAD' in sys_params:
	if sys_params['FULL_LOAD']=='Y':
		for k,v in sys_params.items():
			if (k in param_load.keys() and v!=''):
				param_load[k] = v
				print('Updated Param: ',k,v)


##Adding App Name to Custom Configs List
global_prefix = dbutils.widgets.get("GLOBAL_APP_PREFIX")
if global_prefix == '':
	appName = dbutils.widgets.get("FILE_NM")
else:
	appName = global_prefix + '_' + dbutils.widgets.get("FILE_NM")
set_appname = 'SET spark.app.name={appName}'.format(appName=appName)

if param_load['Custom_Settings'] != '':
	param_load['Custom_Settings'] = str(param_load['Custom_Settings']) + ',' + str(set_appname)
else:
	param_load['Custom_Settings'] = str(set_appname)
	
# Ading Global configs if available
if dbutils.widgets.get("GLOBAL_CONFIGS") != "":
	configs = param_load['Custom_Settings']
	global_configs = dbutils.notebook.run(dbutils.widgets.get("GLOBAL_CONFIGS"), 60)
	configs_final = str(configs + ',' + global_configs)
	param_load['Custom_Settings'] = configs_final

print('Params Passing to File are: ',param_load)

if dbutils.widgets.get("FILE_NM") != '' and dbutils.widgets.get("PARAM_FILE") != '' and dbutils.widgets.get("PATH") != '':
	FILE_NM = str(dbutils.widgets.get("PATH")) + str(dbutils.widgets.get("FILE_NM"))
	print('Running Following File: ',FILE_NM)
	status = dbutils.notebook.run(FILE_NM, 86400, param_load)
	if status == 'SUCCESS':
		for i in ppqueries:
			if i!='':
				print("Running Following Post Processing Quesry: ",i)
				spark.sql(i)
				dbutils.notebook.exit('Job Completed Successfully with Post Processing')
			else:
				dbutils.notebook.exit('Job Completed Successfully')
	else:
		raise Exception(status)
		
elif dbutils.widgets.get("FILE_NM") == '':
	raise Exception("Please provide valid File Name")
elif dbutils.widgets.get("PARAM_FILE") == '':
	raise Exception("Please provide valid Param File Name")
elif dbutils.widgets.get("PATH") == '':
	raise Exception("Please provide valid Path")
else:
	raise Exception("Please verify the Arguements")
