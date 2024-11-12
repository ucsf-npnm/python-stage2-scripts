""" Custom functions to extract data in hdf5 file containing Stage 2 data
"""

# Standard Libraries #
import pandas as pd
import numpy as np
import datetime
from datetime import timedelta

#Custom functions #

##Get channel labels, decoding bytes to str
def GetChannelAxis(File):
    channels = [x.decode() for x in np.array(File['transforms']['morlet_full'].attrs['channels'])]
    return channels

##Get peak frequencies used in wavelets transformation
def GetPeakFrequencies(File):
    peak_freqs = np.array(File['transforms']['morlet_full'].attrs['bands'])
    return peak_freqs

##Get 3-D array
def GetWaveletsArray(File):
    wavelets = np.abs(File['transforms']['morlet_full'][...].astype(complex))
    return wavelets

##Get time axis by using sampling period and start timestamp of recording
def GetTimeAxis(File, Samples, FileStart):
    sp = File['transforms']['morlet_full'].attrs['T'] #sampling period
    factor_ls = list(range(Samples))
    timedelta_ls = [x*sp for x in factor_ls]

    time_axis = []
    for seconds in timedelta_ls:
        time_axis.append(FileStart + timedelta(seconds=seconds))
    return time_axis

#Tabulate data from one channel
def TabulateWavelets(Channel, Array, Index, Columns):
    DataFrame = pd.DataFrame(data=Array, index=Index, columns=Columns).T.reset_index().rename(columns={'index':'Timestamp'})
    NewDataFrame = pd.melt(DataFrame, id_vars=['Timestamp'], var_name='PeakFrequency').rename(columns={'value':Channel})
    return NewDataFrame

#Get artifacts array
def GetArtifactTags(File, FileStart):
    #fs = File['detectors']['stimartifact_v2'].attrs['fs'] #sampling FREQUENCY (Hz)
    period = 1/250
    factor_ls = list(range(np.array(File['detectors']['stimartifact_v2']).shape[0]))
    timedelta_ls = [x*period for x in factor_ls]

    time_axis = []
    for seconds in timedelta_ls:
        time_axis.append(FileStart + timedelta(seconds=seconds))

    NewDataFrame = pd.DataFrame(data=None, index=factor_ls)
    NewDataFrame['ArtifactTimestamp'] = time_axis
    NewDataFrame['ArtifactTag'] = np.array(File['detectors']['stimartifact_v2'])
                                           
    return NewDataFrame

def format_time(Timestamps):
    NewTimestamps = []
    for i in Timestamps:
        NewTimestamps.append(i.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
    return NewTimestamps