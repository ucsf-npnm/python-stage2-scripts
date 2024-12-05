""" Extract data in hdf5 file containing Stage 2 data
    version v2.0
"""

# Standard Libraries #
import pandas as pd
#import numpy as np
import datetime
from datetime import timedelta
import pathlib
import h5py

# Custom functions #
from Tools import GetNeuropaceCatalog, GetChannelAxis, GetPeakFrequencies, GetWaveletsArray, GetTimeAxis, TabulateWavelets, GetArtifactTags, format_time

# User-specified inputs #
patient_id = 'PR05'
input_dir = f'/userdata/dastudillo/patient_data/stage2/{patient_id}' #surveys csv from redcap should be located in this directory 
output_dir = f'/userdata/dastudillo/patient_data/stage2/{patient_id}'
surveys_raw = pd.read_csv(pathlib.Path(input_dir, f'{patient_id}_Stage2Surveys.csv'))
rns_raw = GetNeuropaceCatalog(patient_id) #retrieve ecog catalog based on defined patient_id

magnet_on    = True
scheduled_on = False
realtime_on  = False

start, stop = '2024-09-01', '2024-11-12'

period = 1/250 #250 Hz is sampling frequency set on RNS devices
artifact_version = 'stimartifact_v2' #change according to artifact dataset name within hdf5 you want to use

# Clean up and extract relevant columns from rns catalog # 
# Do not modify
rns_raw = rns_raw.rename(columns={'ECoG trigger':'TriggerType',
                                  'Timestamp':'Start_Timestamp_PT',
                                  'Raw local timestamp':'Trigger_Timestamp_Local',
                                  'ECoG length':'Total_Duration',
                                  'ECoG pre-trigger length':'PreTrigger_Duration'})

if (magnet_on==True) & (scheduled_on==False) & (realtime_on==False):
    mask = (rns_raw.TriggerType=='Magnet')
if (magnet_on==True) & (scheduled_on==True) & (realtime_on==False):
    mask = ((rns_raw.TriggerType=='Magnet') ^ (rns_raw.TriggerType=='Scheduled'))
if (magnet_on==False) & (scheduled_on==False) & (realtime_on==True):
    mask = (rns_raw.TriggerType=='Real_Time')

rns_clean = rns_raw.loc[mask, ['Filename', 'TriggerType', 'Trigger_Timestamp_Local', 'PreTrigger_Duration', 'Total_Duration']].reset_index(drop=True)
rns_clean['Trigger_Timestamp_Local'] = pd.to_datetime(rns_clean['Trigger_Timestamp_Local'], format='%Y-%m-%d %H:%M:%S.%f')

start_timestamp_local = []
for i, j in zip(rns_clean.Trigger_Timestamp_Local, rns_clean.PreTrigger_Duration):
    start_timestamp_local.append(i - timedelta(seconds=j))
rns_clean.insert(2, 'Start_Timestamp_Local', start_timestamp_local)

stop_timestamp_local = []
for i, j in zip(rns_clean.Start_Timestamp_Local, rns_clean.Total_Duration):
    stop_timestamp_local.append(i + timedelta(seconds=j))
rns_clean.insert(3, 'Stop_Timestamp_Local', stop_timestamp_local)

# Read HDF5 file containing preprocessed data #
PREPROC_DIR = f'/data_store0/presidio/NeuroPace_DB/{patient_id}/NP_PROC_HDF/signals_db-pipeline_d8bc5025145576d05d1934d0f027bbb1.hdf5'
PREPROC_DATA = h5py.File(PREPROC_DIR, 'r')

#filter files according to date range specified in user inputs. 
mask = (rns_clean.Trigger_Timestamp_Local>=datetime.datetime.strptime(start, '%Y-%m-%d')) & (rns_clean.Trigger_Timestamp_Local<=datetime.datetime.strptime(stop, '%Y-%m-%d'))
rns_filter = rns_clean.loc[mask].reset_index(drop=True)

# Get list of file names and start timestamps of recordings
files       = rns_filter.Filename
files_start = rns_filter.Start_Timestamp_Local
files_triggertype = rns_filter.TriggerType
files_triggertimestamp = rns_filter.Trigger_Timestamp_Local

print('Number of files: ', len(files))

#Use custom functions to get wavelets tabulated
#NOTE: Suuuper time consuming, don't use to tabulate large number of files (max 5), warning: huge dataframe!!
#TO DO: adapt script to send as server job

allfiles_df = pd.DataFrame()

for i in range(len(files)):
    channels = GetChannelAxis(PREPROC_DATA[files[i]])
    peak_freqs = GetPeakFrequencies(PREPROC_DATA[files[i]])
    wavelets = GetWaveletsArray(PREPROC_DATA[files[i]])
    n_samples = wavelets.shape[2]
    time_axis = GetTimeAxis(PREPROC_DATA[files[i]], n_samples, files_start[i])

    artifact_tags = GetArtifactTags(PREPROC_DATA[files[i]], files_start[i], period, artifact_version)
    artifact_tags['ArtifactTimestamp'] = pd.to_datetime(pd.Series(format_time(artifact_tags.ArtifactTimestamp)))

    wavelets_ch1 = TabulateWavelets(channels[0], wavelets[0], peak_freqs, time_axis)
    wavelets_ch2 = TabulateWavelets(channels[1], wavelets[1], peak_freqs, time_axis)
    wavelets_ch3 = TabulateWavelets(channels[2], wavelets[2], peak_freqs, time_axis)
    wavelets_ch4 = TabulateWavelets(channels[3], wavelets[3], peak_freqs, time_axis)
    all_wavelets = pd.concat([wavelets_ch1, wavelets_ch2, wavelets_ch3, wavelets_ch4], axis=1).T.drop_duplicates().T
    all_wavelets['Timestamp'] = pd.to_datetime(all_wavelets['Timestamp'])
    del wavelets_ch1, wavelets_ch2, wavelets_ch3, wavelets_ch4
    del wavelets

    file_df = all_wavelets.merge(artifact_tags, how='inner', left_on='Timestamp', right_on='ArtifactTimestamp')
    file_df['Filename'] = files[i]
    file_df['TriggerType'] = files_triggertype[i]
    file_df['TriggerLocalTimestamp'] = files_triggertimestamp[i]

    col0 = file_df.pop('Filename')
    col1 = file_df.pop('TriggerType')
    col2 = file_df.pop('TriggerLocalTimestamp')
    file_df.insert(0, 'Filename', col0)
    file_df.insert(1, 'TriggerType', col1)
    file_df.insert(2, 'TriggerLocalTimestamp', col2)
    
    del artifact_tags
    print(f'Adding file {i+1} to final dataframe')
    print('')
    
    allfiles_df = pd.concat([allfiles_df, file_df], ignore_index=True)

allfiles_df.to_csv(pathlib.Path(output_dir, f'{patient_id}_Stage2_wavelets_{start}_{stop}.csv'), index=False)
del allfiles_df
PREPROC_DATA.close() #close hdf5 file

"""End of code"""
