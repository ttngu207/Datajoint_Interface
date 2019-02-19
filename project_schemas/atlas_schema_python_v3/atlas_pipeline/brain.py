import re
import os
import sys
from datetime import datetime

import numpy as np
import scipy.io as sio
import datajoint as dj
import pandas as pd

from lib.initialization_of_db import get_raw_files, get_processed_files
from lib.utilities import get_sorted_filenames

#from neuroglancer_utilities import neuroglancer_utilities

from . import reference

schema = dj.schema(dj.config.get('database.prefix', '') + 'brain')


@schema
class Mouse(dj.Manual):
    definition = """
    mouse : char(18)               # Name for lab mouse, max 8 chars
    ---
    date_of_birth  : date          # (date) the mouse's date of birth
    sex            : enum('M','F') # (M/F) either 'M' for male, 'F' for female
    weight=null    : double        # (double) weight of the mouse in grams. -1 if unknown
    -> reference.AnimalSource
    """

    class Strain(dj.Part):
        definition = """
        -> master
        -> reference.Strain
        """


@schema
class Perfusion(dj.Manual):  # Everyone should be doing the same type of perfusion
    definition = """
    -> Mouse                        # One perfusion per mouse
    ---
    injection_date  : date          # (date) what day was the injection performed

    post_fixation_condition_hours  : int   # (int) How long kept in fix (overnight)
    percent_sucrose_of_fix         : int   # (int) 10 or 20 percent for CSHL stuff

    date_frozen    : date     # (date) The date the brain was frozen
    date_sectioned : date     # (date) The date the brain was sectioned

    -> reference.InjectionType  # TODO: "injection type" in "perfusion" table??
    -> reference.Laboratory.proj(perfusion_lab="lab_name")  # (Str) Which lab perfused the mouse? This lab also kept the mouse

    assessment=''   : varchar(1000) # (Str) optional, qualitative assessment of injection
    """


@schema
class Injection(dj.Manual):  # Viral injections
    definition = """
    -> Mouse                        # One injection per mouse
    injection_number : int          # iterative, how many injections have already been performed
    ---
    injection_date  : date          # (date) what day was the injection performed
    -> reference.InjectionType 
    injection_length: int           # UNSURE. Assumed: the length of time the virus was allowed to propagate

    assessment=''   : varchar(1000) # (Str) qualitative assessment of injection
    """


@schema
class Histology(dj.Manual):
    definition = """
    -> Mouse                        # One Histology per injection per mouse
    -> Injection
    ---
    region         : varchar(10)    # (Str) [UNSURE]
    thickness      : int            # (int) thickness of each slice in microns
    orientation    : enum('sagittal','coronal','horozontal')    # (Str) horizontal, sagittal, coronal
    counter_stain  : varchar(30)    # (Str) what stain was used on the brain (thionin or NeuroTrace)
    -> reference.Laboratory.proj(histology_lab="lab_name")  # (Str) Which lab did the histology
    series         : enum('all','every other','unknown') # Every section OR alternate sections
    """


# AFTER sectioning, the reporter can either be directly visualized with fuorscence or needs to be
#  amplified with immunostaining
# Hannah, with Axio Scanner, will manually select level of exposure to reduce saturation but make sure the
#  the fluorescent molecules are visible
#    - add: CSHL_did_their_own_blackbox_preprocessing : True or False
# Assume calibration, we don't do


@schema
class Stack(dj.Manual):
    definition = """
    -> Histology            # one image stack per histology
    ------------
    stack_name       : varchar(10)   # (Str) unique designation for each mouse
    num_slices       : int           # (int) total number of histology slices
    num_valid_slices : int           # (int) total number of useable histology slices
    channels         : int           # (int) number of channels for each slice
    human_annotated  : boolean       # (bool) does this stack have human annotations

    planar_resolution_um : double    # (double) 0.325 for AxioScanner, 0.46 from CSHL
    section_thickness_um : double    # (double) typically 20um

    unique index (stack_name)   # Adds constraint, stack name must be unique across brains
    """


@schema
class RawSlice(dj.Imported):
    definition = """
    -> Stack
    ---
    aws_raw_bucket:  varchar(40)
    """

    class Slice(dj.Part):
        definition = """
        -> master
        slice_num       : int           # (int) the unique index of the brain slice. Thickness found in Histology table
        ---
        slice_name      : varchar(100)  # (str) the name of the slice. Naming scheme may vary from lab to lab
        valid           : boolean       # (bool) if false, the slice does not exist
        raw_s3_fp       : varchar(200)  # (str)
        """

    def make(self, key):
        """
        For every major key in the master table (Stack) the make function will run, once for every unique stack.
        """
        stack_info = (Stack & key).fetch1()
        stack_name = stack_info["stack_name"]
        raw_files = get_raw_files(s3_client,
                                  stack = stack_name,
                                  returntype = "list")

        # Load the sorted_filenames.txt into a dictionary
        try:
            sorted_fns, _, _ = get_sorted_filenames(s3_client, stack_name = stack_name, return_type ='dictionary')
            self.insert1(dict(key, aws_raw_bucket='mousebrainatlas-rawdata'))
            for slice_num, slice_name in sorted_fns.items():
                key['slice_num'] = slice_num
                key['slice_name'] = str(slice_name)
                key['valid'] = False if key['slice_name'] == 'Placeholder' else True

                # Fill in the RAW S3 filepaths
                key['raw_s3_fp'] = ''
                for fp in raw_files:
                    if key['slice_name'] in fp:
                        key['raw_s3_fp'] = fp
                        break
                self.Slice.insert1(key)
        except Exception as e:
            print(e)

        print(f'Ingestion of raw slices for {stack_name}')


@schema
class ProcessedSlice(dj.Imported):
    definition = """
    -> Stack
    ---
    aws_processed_bucket:  varchar(40)
    """

    class Slice(dj.Part):
        definition = """
        -> master
        slice_num       : int           # (int) the unique index of the brain slice. Thickness found in Histology table
        ---
        slice_name      : varchar(100)  # (str) the name of the slice. Naming scheme may vary from lab to lab
        valid           : boolean       # (bool) if false, the slice does not exist
        processed_s3_fp : varchar(200)  # (str)
        """

    def make(self, key):
        """
        For every major key in the master table (Stack) the make function will run, once for every unique stack.
        """
        stack_info = (Stack & key).fetch1()
        stack_name = stack_info["stack_name"]
        processed_files = get_processed_files(s3_client,
                                              stack = stack_name,
                                              prep_id = "2",
                                              version = "",
                                              resol = "raw",
                                              returntype = "list")

        # Load the sorted_filenames.txt into a dictionary
        try:
            sorted_fns, _, _ = get_sorted_filenames(s3_client, stack_name = stack_name, return_type = 'dictionary')
            self.insert1(dict(key, aws_processed_bucket='mousebrainatlas-data'))
            for slice_num, slice_name in sorted_fns.items():
                key['slice_num'] = slice_num
                key['slice_name'] = str(slice_name)
                key['valid'] = False if key['slice_name'] == 'Placeholder' else True
                # Fill in the PROCESSED S3 filepaths
                key['processed_s3_fp'] = ''
                for fp in processed_files:
                    if key['slice_name'] in fp:
                        key['processed_s3_fp'] = fp
                        break
                self.Slice.insert1(key)
        except Exception as e:
            print(e)

        print(f'Ingestion of processed slices for {stack_name}')


@schema
class PrecomputedBrain(dj.Computed):
    definition = """
    -> ProcessedSlice
    ---
    aws_precomputed_bucket: varchar(40)         
    precomputed_url: varchar(255)   
    """

    def make(self, key):
        # -- define some parameters --
        s3_creds_file = 's3-creds.json'
        folder_to_download_to = os.path.join('.', 's3_precomputed_temp')  # dir in local machine to download the image slices to
        s3_bucket_name_for_upload = 'mousebrainatlas-datajoint-jp2k'  # s3 bucket to upload the precomputed format image stack to
        # -- getting image file paths
        stack_info = (Stack & key).fetch1('stack_name', 'planar_resolution_um', 'section_thickness_um')
        sorted_processedslices = pd.DataFrame(ProcessedSlice.Slice & key
                                              - {'processed_s3_fp': '', 'valid': False}).fetch('slice_num',
                                                                                               'slice_name',
                                                                                               'processed_s3_fp')
        # -- download_ordered_files_from_s3
        if not os.path.exists(folder_to_write_to):
            os.makedirs(folder_to_write_to)
        folder_to_download_to = os.path.join(folder_to_download_to, stack_info['stack_name'])
        bucket = neuroglancer_utilities.get_bucket(s3_creds_file, (ProcessedSlice & key).fetch1('aws_processed_bucket'))
        for _, row in sorted_processedslices.iterrows():
            fname_to_write = '_'.join([f'{row["slice_num"]:04d}', row['slice_name']])
            dest = os.path.join(folder_to_download_to, fname_to_write)
            bucket.download_file(row['processed_s3_fp'], dest)

        # -- convert to precomputed
        folder_to_convert_to = os.path.join('.', 's3_precomputed_temp', stack_info['stack_name'] + '_precomputed')
        neuroglancer_utilities.convert_to_precomputed(folder_to_download_to, folder_to_convert_to,
                                                      voxel_resolution = (stack_info['planar_resolution_um'],
                                                                          stack_info['planar_resolution_um'],
                                                                          stack_info['section_thickness_um']) * 1000,
                                                      voxel_offset = (0, 0, 0))
        # -- upload to S3
        s3_dir_to_write_to = f'precomputed/{stack_info["stack_name"]}'
        upload_directory_to_s3(s3_creds_file, s3_bucket_name_for_upload, folder_to_convert_to, s3_dir_to_write_to,
                               overwrite = False)

        # insert
        precomputed_url = f'https://{s3_bucket_name_for_upload}.s3.amazonaws.com/{s3_dir_to_write_to}'
        self.insert1(dict(key,aws_precomputed_bucket=s3_bucket_name_for_upload, precomputed_url = precomputed_url))
