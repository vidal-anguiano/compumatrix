import os
import csv
import glob
import urllib
import zipfile
import centroids
import pandas as pd
import geopandas as gpd

FIPS =  {"al":"01","ak":"02","az":"04","ar":"05","ca":"06","co":"08",
         "ct":"09","de":"10","dc":"11","fl":"12","ga":"13","hi":"15",
         "id":"16","il":"17","in":"18","ia":"19","ks":"20","ky":"21",
         "la":"22","me":"23","md":"24","ma":"25","mi":"26","mn":"27",
         "ms":"28","mo":"29","mt":"30","ne":"31","nv":"32","nh":"33",
         "nj":"34","nm":"35","ny":"36","nc":"37","nd":"38","oh":"39",
         "ok":"40","or":"41","pa":"42","ri":"44","sc":"45","sd":"46",
         "tn":"47","tx":"48","ut":"49","vt":"50","va":"51","wa":"53",
         "wv":"54","wi":"55","wy":"56","as":"60","gu":"66","mp":"69",
         "pr":"72","vi":"78","us":"us"}

rFIPS = {'01':'al','02':'ak','04':'az','05':'ar','06':'ca','08':'co',
         '09':'ct','10':'de','11':'dc','12':'fl','13':'ga','15':'hi',
         '16':'id','18':'in','17':'il','19':'ia','20':'ks','21':'ky',
         '22':'la','23':'me','24':'md','25':'ma','26':'mi','27':'mn',
         '28':'ms','29':'mo','30':'mt','31':'ne','32':'nv','33':'nh',
         '34':'nj','35':'nm','36':'ny','37':'nc','38':'nd','39':'oh',
         '40':'ok','41':'or','42':'pa','44':'ri','45':'sc','46':'sd',
         '47':'tn','48':'tx','49':'ut','50':'vt','51':'va','53':'wa',
         '54':'wv','55':'wi','56':'wy','60':'as','66':'gu','69':'mp',
         '72':'pr','78':'vi','us':'us'}


def get_resource_url(state_abbr, geo, year=2010):
    '''
    Generates URL for shapefile download of desired state, geo type, and year.

    Parameters
    ----------
    state_abbr : str
        Two letter abbreviation for state
    geo : {'tract', 'county', 'zip', 'block', 'block_pop'}
        String name of the boundary level to use
    year : int
        Year of TIGER data to use

    Returns
    -------
    url : str
        URL for desired shapefile
    '''
    state_id = FIPS[state_abbr.lower()]

    URL  = {'block_pop': "https://www2.census.gov/geo/tiger/TIGER" + str(year) + "BLKPOPHU/",
            'county'   : "https://www2.census.gov/geo/tiger/TIGER" + str(year) + "/COUNTY/" + str(year) + "/",
            'block'    : "https://www2.census.gov/geo/tiger/TIGER" + str(year) + "/TABBLOCK/" + str(year) + "/",
            'state'    : "https://www2.census.gov/geo/tiger/TIGER" + str(year) + "/STATE/" + str(year) + "/",
            'tract'    : "https://www2.census.gov/geo/tiger/TIGER" + str(year) + "/TRACT/" + str(year) + "/",
            'zip'      : "https://www2.census.gov/geo/tiger/TIGER" + str(year) + "/ZCTA5/"+ str(year) + "/",}

    FILE = {'block_pop': "tabblock" + str(year) + "_" + str(state_id).zfill(2) + "_pophu",
            'county'   : "tl_" + str(year) + "_" + str(state_id).zfill(2) + "_county10",
            'block'    : "tl_" + str(year) + "_" + str(state_id).zfill(2) + "_tabblock10",
            'state'    : "tl_" + str(year) + "_" + str(state_id).zfill(2) + "_state10",
            'tract'    : "tl_" + str(year) + "_" + str(state_id).zfill(2) + "_tract10",
            'zip'      : "tl_" + str(year) + "_" + str(state_id).zfill(2) + "_zcta510",}

    if state_id == 'us' and geo == 'tract':
        return 'https://www2.census.gov/geo/pvs/tiger2010st/tl_2010_us_ttract10.zip'

    return URL[geo] + FILE[geo] + '.zip'


def setup_dirs(state_abbr, geo, outdir):
    '''
    Sets up the shapefile directory where shapefiles will be downloaded into and
    the outputs directory where outputs will be posted.

    Parameters
    ----------
    state_abbr : str
        Two letter abbreviation for state
    geo : {'tract', 'county', 'zip'}
        String name of the boundary level to use
    outdir : str
        Desired location for directory creation

    Returns
    -------
    None
    '''
    dir_paths = ()

    for dir in ['shapefiles', 'outputs']:
        dir_path = os.path.join(outdir, dir, geo, state_abbr.upper())
        if not os.path.isdir(dir_path):
            os.makedirs(dir_path)

        dir_paths = dir_paths + (dir_path,)

    return dir_paths


def download_and_extract_file(dl_url, dl_dir):
    '''
    Given the download url and a download directory path, this function will
    download the file only if it has not already been downloaded.

    Parameters
    ----------
    dl_url : str
        URL for the file to download
    dl_dir : str
        Directory where the downloaded file should be saved
    '''
    zip_file = dl_url.split('/')[-1]

    if not os.path.isfile(os.path.join(dl_dir, zip_file)):
        print(f'Downloading {zip_file}...')
        filepath =  os.path.join(dl_dir, zip_file)
        urllib.request.urlretrieve(dl_url, filepath)
        print('Downloaded!')
    else:
        print(f"Skipping {zip_file}, it's already downloaded.")

    dbf_file = zip_file[:-4] + ".dbf"
    if not os.path.isfile(os.path.join(dl_dir, dbf_file)):
        print(f'Extracting {zip_file}...')
        tract_zip = zipfile.ZipFile(os.path.join(dl_dir, zip_file))
        tract_zip.extractall(os.path.join(dl_dir))
        tract_zip.close()
        print('Extracted!')
    else:
        print(f"Skipping {zip_file}, it's already extracted.")

    return dbf_file


def get_resource(state_abbr, geo, outpath, year = 2010):
    '''
    Creates download and output directories, downloads, and extracts needed
    files.

    Parameters
    ----------
    state_abbr : str

    geo : {'tract', 'county', 'zip', 'block', 'block_pop'}

    outpath : str

    year : {2010}, default 2010

    Returns
    -------
    Tuple of the output directory path, download directory path and the file
    name of the downloaded resource.

    '''
    dl_dir, out_dir = setup_dirs(state_abbr, geo, outpath)
    url = get_resource_url(state_abbr, geo, year)
    file = download_and_extract_file(url, dl_dir)

    return (dl_dir, out_dir, file)


def get_bordering_states(state_abbr, outpath):
    '''
    Creates a list of state abbreviations for all states that border state_abbr.

    Parameters
    ----------
    state_abbr : str

    outpath : str

    Returns
    -------
    states_list : list
        list of state abbreviations for all states that border state_abbr
    '''
    dl_dir, out_dir, file = get_resource('US', 'state', outpath)

    states = gpd.read_file(os.path.join(dl_dir, file))

    if state_abbr.lower() == 'dc':
        return ['va', 'md', 'de', 'dc']

    origin_state = states.copy()[states.STATEFP10 == FIPS[state_abbr.lower()]]
    origin_state['geometry'] = origin_state.buffer(.5)
    bordering = gpd.sjoin(states, origin_state, how = 'inner', op = 'intersects')
    states_list = [rFIPS[x] for x in list(bordering.STATEFP10_left)]

    return states_list


def border_states_geodf(states, geo, outpath):
    '''
    Returns the geo level GeoDataFrame for all states provided in the states
    list input.

    Parameters
    ----------
    states : str
        list of state abbreviations
    geo : {'tract', 'county', 'zip'}
        String name of the boundary level to use
    outpath : str
        path of directory where output folder should be created

    Returns
    -------
    GeoDataFrame of all the  for the states in the input list
    '''
    file_paths = []
    for state in states:
        dl_dir, out_dir, file = get_resource(state, geo, outpath)
        file_paths.append(os.path.join(dl_dir, file))

    return pd.concat([gpd.read_file(path) for path in file_paths])


def get_pwcs(states, geo, outpath, replace=False):
    '''
    Computes then reads population weighted centroids.

    Parameters
    ----------
    state_abbr : str or list
        Two letter abbreviations for state
    geo : {'tract', 'county', 'zip'}
        String name of the boundary level to use

    Returns
    -------
    pandas DataFrame of the population weighted centroids for tracts/zips/counties
    the in desired state
    '''
    if type(states) != list: states = [ states ]

    pwc_files = []

    for state in states:
        pwc_file_path = centroids.compute_geo_centroids(state, geo, outpath, replace)
        pwc_files.append(pwc_file_path)

    pwcs = pd.concat([pd.read_csv(path) for path in pwc_files])

    zfill = {'zip'   : 7,
             'tract' : 11,
             'county': 3}

    pwcs['GEOID'] = pwcs['GEOID'].apply(lambda x: str(x).zfill(zfill[geo]))

    return pwcs[['GEOID', 'X', 'Y']]

def aggregate_parts(state_abbr, geo, outpath):
    base_dir = os.path.join(outpath, 'outputs', geo, state_abbr.upper())
    parts_dir = os.path.join(base_dir, 'parts')
    print(parts_dir)
    parts = glob.glob(parts_dir + '/subset_*.csv')

    outfile_path = os.path.join(base_dir, f'{state_abbr.upper()}-matrix-{geo.upper()}.csv')

    with open(outfile_path, 'w') as csvfile:
        csvwriter = csv.writer(csvfile)

        csvwriter.writerow(['origin', 'destination', 'minutes'])

        for file in parts:
            print(file)
            f = open(file)
            for line in f:
                clean_line = line.replace('\n','').split(',')
                csvwriter.writerow(clean_line)
            f.close()
