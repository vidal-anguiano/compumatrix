import geopandas as gpd
import utils
import os

def create_od_pairs(state_abbr, buffer, geo, outpath, o_feature = 'boundary', d_feature = 'centroid', centroid = 'centroid', replace = False):
    '''
    Creates origin destination pair mappings with the lat (Y), lon (X) coordinates
    of each geounits centroid.

    Parameters
    ----------
    state_abbr : str
        Two letter abbreviation for state
    buffer : int
        Number of meters of the buffer to be applied to origin
    geo : {'tract', 'county', 'zip'}
        String name of the boundary level to use
    outpath: str
        Path of directory where output folder should be created
    o_feature : {'boundary', 'centroid', 'pwc'}, default 'boundary'
        Specify the geo feature to use as the origin buffer
    d_feature : {'boundary', 'centroid', 'pwc'}, default 'centroid'
        Specify the geo feature to use as the destination location
    centroid : {'centroid', 'pwc'}
        Specifies whether the output centroid should be the boundary centroid
        or the population weighted centroid (pwc)

    Returns
    -------
    A csv file containing the origin destination pairs for each geographical
    unit within the desired state along with the coordinates of the units
    centroid.
    '''
    file_path = os.path.join(outpath, 'outputs', geo, state_abbr.upper(), f'{state_abbr.upper()}-odpairs-{buffer}m-{geo.upper()}.csv')

    if not os.path.isfile(file_path) or replace:
        dl_dir, out_dir, file = utils.get_resource(state_abbr, geo, outpath)
        origins = gpd.read_file(os.path.join(dl_dir, file))[['GEOID10', 'geometry']]
        origins['oX'], origins['oY'] = create_xy_coords(gdf      = origins,
                                                        states   = state_abbr,
                                                        centroid = centroid,
                                                        geo      = geo,
                                                        outpath  = outpath)

        border_states = utils.get_bordering_states(state_abbr, outpath)
        destinations = utils.border_states_geodf(border_states, geo, outpath)[['GEOID10', 'geometry']]
        destinations['dX'], destinations['dY'] = create_xy_coords(gdf      = destinations,
                                                                  states   = border_states,
                                                                  centroid = centroid,
                                                                  geo      = geo,
                                                                  outpath  = outpath)

        origins = origins.to_crs(epsg = 2163).rename(columns = {'GEOID10':'origin'})
        destinations = destinations.to_crs(epsg = 2163).rename(columns = {'GEOID10':'destination'})

        if o_feature == 'boundary':
            origins['geometry'] = origins.copy().buffer(buffer)

        elif o_feature == 'centroid':
            origins['geometry'] = origins.copy().centroid.buffer(buffer)

        elif d_feature == 'centroid':
            destinations['geometry'] = destinations.copy().centroid

        result = gpd.sjoin(origins, destinations, how = 'inner', op = 'intersects')[['origin', 'oX', 'oY', 'destination', 'dX', 'dY']].reset_index(drop=True)

        result.to_csv(file_path, index=False)

        return result

    else:
        print(f'{os.path.abspath(file_path)} already exists.')

def create_xy_coords(gdf, states, centroid, geo, outpath):
    '''
    '''
    if centroid == 'pwc':
        pwc = utils.get_pwcs(states, geo, outpath)
        gdf = gdf.merge(pwc[['GEOID', 'X','Y']], how = 'left', left_on = 'GEOID10', right_on='GEOID')

    elif centroid == 'centroid':
        gdf['X'], gdf['Y'] = gdf.centroid.x, gdf.centroid.y

    return gdf['X'], gdf['Y']
