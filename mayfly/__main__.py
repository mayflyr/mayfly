"""mayfly

Usage:
  mayfly <str_component_def> <str_json_kwargs> <str_loadimports> <str_requirements_jsonlst> <lcbus_uid> <predef_uid>
"""

import sys
import os

from .run_component import main

if __name__ == '__main__':
	
	str_component_def = sys.argv[1]
	str_json_kwargs = sys.argv[2]
	
	try:
		str_loadimports = sys.argv[3]
	except:
		str_loadimports = None
	try:
		str_requirements_jsonlst = sys.argv[4]
	except:
		str_requirements_jsonlst = None
	else:
		if str_requirements_jsonlst == '':
			str_requirements_jsonlst = None
	
	try:
		lcbus_uid = sys.argv[5]
	except:
		lcbus_uid = None
	try:
		predef_uid = sys.argv[6]
	except:
		predef_uid = None
	
	main(str_component_def, str_json_kwargs, str_loadimports, str_requirements_jsonlst, lcbus_uid, predef_uid)
	
