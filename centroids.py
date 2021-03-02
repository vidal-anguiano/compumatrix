import os
import numpy as np
import pandas as pd
import geopandas as gpd
from simpledbf import Dbf5
import utils

NAME = {'block': 'TABBLOCK',
        'tract': 'TRACT',
        'county': 'COUNTY',
        'zip': 'ZCTA5'}

def compute_geo_centroids(state_abbr, geo, outpath, year=2010, replace = False):
    '''
    Computes the population weighted centroids of all boundaries at the desired
    level (block, tract, county, zip) within a designated state.

    Parameters
    ----------
    state_abbr : str
        Two letter abbreviation for state
    geo : {'tract', 'county', 'zip'}
        String name of the boundary level to use
    outpath : str
        Path of directory where output folder should be created

    Returns
    -------
    File named [state_abbr]-pwc-[geo].csv in [outpath]/outputs/[geo]/[state_abbr]/
    '''
    out_dirs = {}
    dl_dirs  = {}
    files    = {}
    assert state_abbr.lower() in utils.FIPS.keys(), 'Not a known state abbreviation.'
    print(f'-------------Now processing {state_abbr.upper()}-------------')

    for geo_type in ['block_pop', 'block', geo]:
        dl_dir, out_dir, file = utils.get_resource(state_abbr, geo_type, outpath)

        out_dirs[geo_type] = out_dir
        dl_dirs[geo_type] = dl_dir
        files[geo_type] = file

    file_path = os.path.join(out_dirs[geo], f'{state_abbr.upper()}-pwc-{NAME[geo]}.csv')

    if not os.path.isfile(file_path) or replace:
        block_path = os.path.join(dl_dirs['block'], files['block'])
        pop_path   = os.path.join(dl_dirs['block_pop'], files['block_pop'])

        coords_w_pop = get_block_coords_w_pop(block_path, pop_path)

        geo_file = files[geo][:-4] + '.shp'
        geo_path = os.path.join(dl_dirs[geo], geo_file)
        geo_shape  = gpd.read_file(geo_path)

        pop_weighted_centroids = calc_pop_weighted_centroids(coords_w_pop, geo_shape, geo)

        pop_weighted_centroids.to_csv(file_path, index=False)

    else:
        print(f'Population weighted centroids for {state_abbr.upper()} have already been computed.')

    return file_path

def get_block_coords_w_pop(block_path, pop_path):
    '''
    Returns a GeoDataFrame with block level coordinates and population figures
    joined from separate block level data.

    Parameters
    ----------
    block_path : str
        Path for the block-level .dbf file containing the block centroid coordinates
    pop_path : str
        Path for the block-level .dbf file containing the block population data

    Returns
    -------
    coords_w_pop : GeoDataFrame
        Block level coordinates and population figures joined from block level data
    '''
    block_coords = Dbf5(block_path).to_dataframe()
    block_pop    = Dbf5(pop_path).to_dataframe()

    # Fix lat lon strings
    print('Fixing lat/lon strings from DBFs...')
    block_coords['INTPTLAT10'] = block_coords['INTPTLAT10'].str.replace('+', '').astype(float)
    block_coords['INTPTLON10'] = block_coords['INTPTLON10'].astype(float)

    # Merge block pops with block points
    print('Merging block locations and block populations...')
    coords_w_pop = block_coords.merge(
            block_pop[['BLOCKID10', 'POP10']],
            left_on = 'GEOID10',
            right_on = 'BLOCKID10',
            how = 'left')

    coords_w_pop = gpd.GeoDataFrame(coords_w_pop,
                                    geometry = gpd.points_from_xy(coords_w_pop.INTPTLON10,
                                                                  coords_w_pop.INTPTLAT10),
                                    crs = {'init':'epsg:4269'})

    return coords_w_pop

def calc_pop_weighted_centroids(coords_w_pop, geo_shape, geo):
    '''
    Computes population weighted centroids of desired geography using block level
    population data and returns in a GeoDataFrame.

    Parameters
    ----------
    coords_w_pop : GeoDataFrame
        GeoDataFrame containing block level centroid coordinates and population
    geo_shape : GeoDataFrame
        Boundary file for the geometry for which centroids should be computed
    geo : {'tract', 'county', 'zip'}
        String name of the boundary level to use

    Returns
    -------
    GeoDataFrame with one row for each boundary within the shapefile along with the
    X, Y coordinates of the population centroid and the sum of the population within
    the boundary.
    '''
    # Merge geo that each block centroid falls within
    geo_shape  = geo_shape[['GEOID10', 'geometry']].rename(columns={'GEOID10': NAME[geo]})
    geo_w_pop = gpd.sjoin(coords_w_pop, geo_shape, how='left', op='intersects')

    geo_w_pop = geo_w_pop[~geo_w_pop[NAME[geo]].isna()].reset_index(drop=True)
    print('Converting block centroids to Albers...')
    geo_w_pop = geo_w_pop.to_crs(epsg = 2163)
    geo_w_pop['X'], geo_w_pop['Y'] = geo_w_pop.geometry.x, geo_w_pop.geometry.y
    geo_w_pop = geo_w_pop[['GEOID10', 'Y', 'X', 'POP10', NAME[geo]]]
    geo_w_pop = geo_w_pop.rename(
            index=str, columns={'GEOID10': 'GEOID', 'POP10': 'POP'})

    # return geo_w_pop
    print('Finding pop-weighted centroids...')
    wm = lambda x: np.average(x, weights=geo_w_pop.loc[x.index, "POP"] + 1)
    blocks_agg = geo_w_pop.groupby(NAME[geo]).agg(
            {'Y': wm, 'X': wm, 'POP': 'sum'}).reset_index()

    print('Saving zcta5 centroids to CSV...')
    blocks_agg_gdf = gpd.GeoDataFrame(blocks_agg, geometry=gpd.points_from_xy(
        blocks_agg.X, blocks_agg.Y), crs={'init':'epsg:2163'}).to_crs(epsg = 4326)
    blocks_agg_gdf['X'], blocks_agg_gdf['Y'] = blocks_agg_gdf.geometry.x, blocks_agg_gdf.geometry.y
    blocks_agg_gdf = blocks_agg_gdf.drop(columns=['geometry']).rename(index=str, columns={NAME[geo]: 'GEOID'})
    # print(blocks_agg_gdf)
    # blocks_agg_gdf['GEOID'] = blocks_agg_gdf['GEOID'].apply(lambda x: x[2:])

    return blocks_agg_gdf
