# %%
import pandas as pd
import re
import glob
import os
import datetime as dt
from datetime import date
import subprocess
import wget
import sys
import glob
import os
import sys
from osgeo import gdal
import rasterio as rio
from rasterio.merge import merge
import numpy as np
import matplotlib.pyplot as plt
import fiona
import rasterio.mask
import h5py
import rasterio as rio
import datetime as dt
from datetime import date
import subprocess
import matplotlib.dates as mdates
from unittest import result
import shutil

BASE_PATH = '/Volumes/Sandisk/BlackMarble'

# %%


def runcmd(cmd, verbose, *args, **kwargs):

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        shell=True
    )
    std_out, std_err = process.communicate()
    if verbose:
        print(std_out.strip(), std_err)
    pass


today = date.today()
tt = today.timetuple()
julian_date = int(tt.tm_yday)
adjusted_date = julian_date - 11
print(f'Today\'s Julian date is {adjusted_date}.')

url = 'https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/5000/VNP46A2/2022/' + \
    str(adjusted_date) + '.csv'
auth = 'Authorization: Bearer ZXRoYW5qZXZ2ZWxsOlpYUm9ZVzR1YW1WM1pXeHNRRzVyYm1WM2N5NXZjbWM9OjE2NjU0Nzc4ODQ6ZWIwYjU2Y2MwOTgyYmM5NDM1NGQ0ZGQ1YWU3MTY3OTMxMjZhZDA4YQ'
csv_dest = '/Volumes/Sandisk/BlackMarble/CSVs/'

runcmd(
    f'wget -e robots=off -m -np -R .html,.tmp -nH --cut-dirs=3 "{url}" --header "{auth}" -P {csv_dest}', verbose=True)

csv = str(adjusted_date) + '.csv'
path = f'{csv_dest}/VNP46A2/2022/{csv}'
print(path)

try:
    data = pd.read_csv(path)
except FileNotFoundError:
    sys.exit(
        f'The CSV for day {adjusted_date} was not successfully downloaded; check LADSWEB.')

files_only = data['name']
relevant_files = []

for file in files_only:
    if ('h30v04' in file) or ('h30v05' in file) or ('h31v04' in file) or ('h31v05' in file):
        relevant_files.append(file)

print(f'Files extracted from .csv: {str(relevant_files)}')
print('Beginning downloads...')

for file in relevant_files:
    runcmd('wget -e robots=off -m -np -R .html,.tmp -nH --cut-dirs=3 "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/5000/VNP46A2/2022/' +
           str(adjusted_date) + '/' + file + '"' + f' --header "{auth}" -P /Users/ethanjewell/Desktop/Python\ Env/Scripting/BlackMarble/downloads/', verbose=True)

source = '/Users/ethanjewell/Desktop/Python Env/Scripting/BlackMarble/downloads/VNP46A2/2022/' + \
    str(adjusted_date) + '/'
destination = '/Volumes/Sandisk/BlackMarble/RAW/'

# gather all files
allfiles = os.listdir(source)

# iterate on all files to move them to destination folder
for f in allfiles:
    shutil.move(str(source + f), destination)
    print(f'{str(f)} moved to RAW folder.')

print('Program complete.')

# %%
# List input raster files


def buildInitialTifs():

    for file in TILES:

        # Initiate opening of HDF5 file
        hdflayer = gdal.Open(file, gdal.GA_ReadOnly)

        # Read the Gap_Filled_DNB_BRDF-Corrected_NTL field
        subhdflayer = hdflayer.GetSubDatasets()[2][0]
        # Load NTL data into rlayer
        rlayer = gdal.Open(subhdflayer, gdal.GA_ReadOnly)

        # Read the Mandatory_Quality_Flag field
        qualitylayer = hdflayer.GetSubDatasets()[4][0]
        # Load quality layer data into qlayer
        qlayer = gdal.Open(qualitylayer, gdal.GA_ReadOnly)

        # /Volumes/Sandisk/BlackMarble/RAW/
        outputName = str(file).split('/')[5]
        qualityOutputName = str(file).split('/')[5] + ' Quality'

        HorizontalTileNumber = int(rlayer.GetMetadata_Dict()[
                                   "HorizontalTileNumber"])
        VerticalTileNumber = int(rlayer.GetMetadata_Dict()[
                                 "VerticalTileNumber"])
        WestBoundCoord = (10*HorizontalTileNumber) - 180
        NorthBoundCoord = 90-(10*VerticalTileNumber)
        EastBoundCoord = WestBoundCoord + 10
        SouthBoundCoord = NorthBoundCoord - 10

        EPSG = "-a_srs EPSG:4326"  # WGS84

        translateOptionText = EPSG + " -a_ullr " + str(WestBoundCoord) + " " + str(
            NorthBoundCoord) + " " + str(EastBoundCoord) + " " + str(SouthBoundCoord)

        translateoptions = gdal.TranslateOptions(
            gdal.ParseCommandLine(translateOptionText))
        gdal.Translate(f'{BASE_PATH}/Tiles as Tifs/' +
                       outputName + '.tif', rlayer, options=translateoptions)
        gdal.Translate(f'{BASE_PATH}/Quality as Tifs/' +
                       qualityOutputName + '.tif', qlayer, options=translateoptions)

    raster_files = glob.glob(f'{BASE_PATH}/Tiles as Tifs/**.tif')
    raster_files.extend(glob.glob(f'{BASE_PATH}/Quality as Tifs/**.tif'))
    raster_files.sort()

    master_array = []

    i = 0
    while i != len(raster_files):
        master_array.append(
            [raster_files[i], raster_files[i+1], raster_files[i+2], raster_files[i+3]])
        i += 4

    for quad in master_array:

        raster_to_mosaic = []

        for item in quad:
            individual_frame = rio.open(item)
            raster_to_mosaic.append(individual_frame)
            output_meta = individual_frame.meta.copy()

        output_name = quad[0].split('/')[5].split('.')[1]
        mosaic, output = merge(raster_to_mosaic)
        output_meta.update(
            {"driver": "GTiff", "height": mosaic.shape[1], "width": mosaic.shape[2], "transform": output})

        qual_extension = ' Quality'

        if 'Quality' in quad[0]:
            output_path = f'{BASE_PATH}/Merged Tiles/' + \
                output_name + qual_extension + '.tif'
        else:
            output_path = f'{BASE_PATH}/Merged Tiles/' + output_name + '.tif'

        with rio.open(output_path, 'w', **output_meta) as m:
            m.write(mosaic)

    large_mosaics = glob.glob(f'{BASE_PATH}/Merged Tiles/**.tif')
    large_mosaics.sort()

    # /Volumes/Sandisk/BlackMarble/Merged Tiles
    for file in large_mosaics:

        output_name = file.split('/')[5]

        with fiona.open('/Users/ethanjewell/Desktop/Python Env/Scripting/Map Data/Shapefiles/NATIONAL/National Border.shp') as shapefile:
            shapes = [feature["geometry"] for feature in shapefile]

        with rio.open(file) as src:
            out_image, out_transform = rio.mask.mask(
                src, shapes, nodata=65535, crop=True)
            out_meta = src.meta
            out_meta.update({"driver": "GTiff",
                             "height": out_image.shape[1],
                             "width": out_image.shape[2],
                             "transform": out_transform})

        if 'Quality' in output_name:
            with rio.open(f'{BASE_PATH}/DPRK Only/quality_masks/' + str(output_name), "w", **out_meta) as dest:
                dest.write(out_image)

        else:
            with rio.open(f'{BASE_PATH}/DPRK Only/ntl_data/' + str(output_name), "w", **out_meta) as dest:
                dest.write(out_image)

    print('Initial tifs created.')

# %%
