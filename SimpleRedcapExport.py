""" Simple extraction of records from redcap projects using API token
    Requirements: pandas, PyCap, pathlib
"""

# Standard Libraries #
import pandas as pd
from redcap import Project
import pathlib

# User-specified inputs #
patient_id = 'PR05'
api_key = 'AlphanumericKeyThatOnlyYouShouldHave' #API token assigned to unique user AND project by REDCap administrator (to request: log in with credentials on REDCap, go to a project, click API under Applications)
out_dir = f'/userdata/dastudillo/patient_data/'

# Main code #
#Export project info and records
api_url = 'https://redcap.ucsf.edu/api/'
project = Project(api_url, api_key)
project_info = project.export_project_info()
project_id, project_title = project_info['project_id'], project_info['project_title']
project_records = project.export_records(format_type='df').reset_index()

#Assign name to output file
out_filename = project_title.replace(' ', '').replace(':', '') + '_ProjectID' + str(project_id) + '.csv'

#Save output file
project_records.to_csv(pathlib.Path(out_dir, out_filename), index=False)

"""End of code"""

