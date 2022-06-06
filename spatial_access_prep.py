import os
import utils
import pandas as pd
import geopandas as gpd



def create_destinations_file(state_abbr, moud_file, outpath, resource='all', geo='tract', parts=None, save=False):
    '''
    Creates destinations file for each MOUD that can be reached by origins in
    the state specified.

    Parameters
    ----------
    state_abbr : str

    moud_file : str

    outpath : str

    resource : str

    geo : str

    parts : int

    Returns
    -------
    '''
    assert resource in ['methadone', 'naltrexone/vivitrol', 'buprenorphine', 'STU', 'all'], "Not a valid resource type."

    bordering_states = utils.get_bordering_states(state_abbr, outpath)
    regional_shp = utils.border_states_geodf(bordering_states, geo, outpath)

    raw_matrix_file_name = f'{state_abbr}-matrix-TRACT.csv'
    raw_matrix_file_path = os.path.join(outpath, 'outputs', geo, state_abbr, raw_matrix_file_name)
    raw_matrix = pd.read_csv(raw_matrix_file_path, dtype={'destination':'str'})

    mouds_shp = gpd.read_file(moud_file)

    if resource != 'all':
        mouds_shp = mouds_shp[mouds_shp['category'] == resource]


    regional_shp.geometry = regional_shp.geometry.to_crs(epsg = 4326)
    mouds_shp.geometry = mouds_shp.geometry.to_crs(epsg = 4326)

    destinations = (gpd.sjoin(mouds_shp, regional_shp[['GEOID10', 'geometry']], how='inner', op='intersects')
                    .rename(columns={'GEOID10':'GEOID'})
                    .drop('index_right', axis = 1)
                    )

    destinations = destinations[destinations['GEOID'].isin(raw_matrix.destination)]

    destinations.reset_index(drop=True, inplace=True)
    destinations['ID'] = destinations.index + 1
    destinations = destinations[~destinations.GEOID.isna()]
    destinations['GEOID'] = destinations['GEOID'].astype(str)

    if save:
        save_path = os.path.join(outpath, 'inputs', geo, state_abbr, f'{state_abbr.upper()}-{resource[:3]}-moud-dests.csv')
        destinations['dX'] = destinations.geometry.x
        destinations['dY'] = destinations.geometry.y
        (destinations
        .drop('geometry', axis = 1)
        .to_csv(save_path, index=False)
        )
        print(f"{state_abbr} destinations saved to {save_path}")
        return None

    return destinations

def create_origins_file(state_abbr, outpath, geo='tract', save=False):
    '''
    '''
    origins_file_name = utils.get_resource_file_name(state_abbr, geo) + '.shp'
    origins_path = os.path.join(outpath, 'shapefiles', geo, state_abbr, origins_file_name)
    origins = gpd.read_file(origins_path)

    origins.geometry = origins.geometry.to_crs(epsg = 4326)
    origins = origins.rename(columns={'GEOID10':'GEOID'})

    if save:
        save_path = os.path.join(outpath, 'inputs', geo, state_abbr, f'{state_abbr.upper()}-origins.csv')
        origins['oX'] = origins.geometry.centroid.x
        origins['oY'] = origins.geometry.centroid.y
        cols = ['GEOID', 'oX', 'oY']
        (origins[cols]
        .to_csv(save_path, index=False)
        )
        print(f"{state_abbr} origins saved to {save_path}")
        return None

    return origins

def create_transformed_matrix(state_abbr, outpath, resource='all', geo='tract', parts=None, pad_origins=True, save=False):
    '''
    '''
    raw_matrix_file_name = f'{state_abbr}-matrix-TRACT.csv'
    raw_matrix_file_path = os.path.join(outpath, 'outputs', geo, state_abbr, raw_matrix_file_name)
    raw_matrix = pd.read_csv(raw_matrix_file_path, dtype={'destination':'str'})

    origins_file_name = f'{state_abbr.upper()}-origins.csv'
    origins_path = os.path.join(outpath, 'inputs', geo, state_abbr, origins_file_name)
    origins = gpd.read_file(origins_path)

    destinations_file_name = f'{state_abbr.upper()}-{resource[:3]}-moud-dests.csv'
    destinations_file_path = os.path.join(outpath, 'inputs', geo, state_abbr, destinations_file_name)

    if not os.path.exists(destinations_file_path):
        print(f"{destinations_file_path} not found. Must create destinations for {resource} MOUD type.")
        return None
    destinations = pd.read_csv(destinations_file_path, dtype={'destination':'str', 'GEOID':'str'})

    cost_matrix_w_ids = raw_matrix.merge(destinations[['ID', 'GEOID']], how='left', left_on='destination', right_on='GEOID')
    cost_matrix_w_ids = cost_matrix_w_ids[~cost_matrix_w_ids.ID.isna()]
    cost_matrix_w_ids = cost_matrix_w_ids.drop(columns=['destination','GEOID']).rename(columns={'ID':'destination'}) # drop extra columns
    cost_matrix_w_ids.destination = cost_matrix_w_ids.destination.astype(int)
    cost_matrix_w_ids.minutes = cost_matrix_w_ids.minutes.astype(float)
    cost_matrix_w_ids = cost_matrix_w_ids[['origin', 'destination', 'minutes']]
    m_cost_matrix = cost_matrix_w_ids.pivot_table(index='origin', columns='destination', values='minutes', fill_value=999)
    m_cost_matrix =  m_cost_matrix.reset_index().rename_axis(None, axis=1)
    m_cost_matrix.origin = m_cost_matrix.origin.astype(str)

    if pad_origins:
        missing_origins = origins[~(origins.GEOID.isin(m_cost_matrix.origin))]
        fill_origins = pd.DataFrame(index = missing_origins.GEOID, columns = m_cost_matrix.columns[1:m_cost_matrix.shape[1]]).fillna(999)
        fill_origins = fill_origins.reset_index().rename(columns={'GEOID':'origin'})
        m_cost_matrix = m_cost_matrix.append(fill_origins)

    if save:
        save_path = os.path.join(outpath, 'inputs', geo, state_abbr, f'{state_abbr.upper()}-transformed-matrix.csv')
        m_cost_matrix.to_csv(save_path, index=False)
        print(f"{state_abbr} transformed matrix saved to {save_path}")
        return None

    return m_cost_matrix
