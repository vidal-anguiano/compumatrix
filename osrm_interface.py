import os
import csv
import json
import time
import requests
import pandas as pd
from collections import defaultdict

def create_json_obj_dep(mappings):
    obj = {}
    mappings['o_coords'] = mappings.apply(lambda x: str(x.oX) + ',' + str(x.oY), axis = 1)
    mappings['d_coords'] = mappings.apply(lambda x: str(x.dX) + ',' + str(x.dY), axis = 1)
    for origin in list(mappings.origin.unique()):
        sub_obj  = {}
        o_key = str(origin).zfill(5)
        subset   = mappings[(mappings['origin']  == origin) & (mappings['destination'] != origin)]
        if len(subset) != 0:
            o_coord  = subset.o_coords.iloc[0]
            dests    = [o_key] + [str(x).zfill(5) for x in list(subset.destination)]
            d_coords = list(subset.d_coords)
            coords   = [o_coord] + d_coords

            sub_obj['destinations'] = dests
            sub_obj['coordinates']  = coords

        else:
            sub_obj['destinations'] = [o_key]
            sub_obj['coordinates']  = mappings[mappings['origin']  == origin].o_coords.iloc[0]

        obj[o_key] = sub_obj

    return obj

def create_json_obj(csv_file, geo):
    def def_val():
        return {'destinations':[],
                'coordinates': []}

    zfill = {'zip'   : 7,
             'tract' : 11,
             'county': 3}

    obj = defaultdict(def_val)
    with open(csv_file) as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            origin = str(row[0]).zfill(zfill[geo.lower()])
            obj[origin]['destinations'] = obj[origin]['destinations'] + str([row[3]).zfill(zfill[geo.lower()])]
            obj[origin]['coordinates'] = obj[origin]['coordinates'] + [row[4] + ',' + row[5]]

    return obj

def prepare_osrm_inputs(state_abbr, geo, buffer, outpath):
    print(f"Preparing OSRM inputs for {state_abbr.upper()}...")
    state_path = os.path.join(outpath, 'outputs', geo, state_abbr.upper())
    write_to = os.path.join(state_path, f'{state_abbr.upper()}_osrm_inputs.json')

    odpairs_file_path = os.path.join(state_path, f'{state_abbr.upper()}-odpairs-{buffer}m-{geo.upper()}.csv')

    assert os.path.isfile(odpairs_file_path), f"odpairs file for {state_abbr.upper()} does not exist."

    if not os.path.isfile(write_to):
        inputs = create_json_obj(odpairs_file_path, geo)
        print(f"Writing OSRM inputs for {state_abbr.upper()} to json file...")
        with open(write_to, 'w') as fp:
            json.dump(inputs, fp)
        print(f"OSRM inputs for {state_abbr.upper()} complete!")

    else:
        print(f"OSRM inputs for {state_abbr.upper()} already exists!")

def create_base_url(ip, port):
    base_url = 'http://' + str(ip) + ':' + str(port) + '/table/v1/driving/'
    return base_url


def make_osrm_request(base_url, coordinates, sources=None, destinations=None):
    request_url = base_url + ';'.join(coordinates)
    if sources:
        sources = [str(x) for x in sources]
        sources = ';'.join(sources)

    if destinations:
        destinations = [str(x) for x in destinations]
        destinations = ';'.join(destinations)


    return requests.get(request_url, params = {'sources': sources,
                                               'destinations': destinations}).text


def extract_durations(request_result):
    result = json.loads(request_result)
    return result['durations'][0]


def results_to_df(origin, destinations, durations):
    result = pd.DataFrame({'duration':durations,
                           'destination': destinations})

    result['origin'] = origin
    result['minutes'] = round(result['duration'] / 60, 2)
    return result[['origin', 'destination', 'minutes']]


def get_durations(base_url, state_abbr, geo, buffer, outpath):
    state_path = os.path.join(outpath, 'outputs', geo, state_abbr.upper())
    osrm_inputs = os.path.join(state_path, f'{state_abbr.upper()}_osrm_inputs.json')

    assert os.path.isfile(osrm_inputs), f"osrm_inputs file for {state_abbr.upper()} does not exist."

    parts_path = os.path.join(state_path, 'parts')

    if not os.path.isdir(parts_path):
        os.makedirs(parts_path)

    inputs = json.load(open(osrm_inputs))

    count = 0
    total = len(inputs)

    for origin, contents in inputs.items():
        print(f'Working on {origin}...')
        id = contents['destinations'].index(origin)
        print(origin, id)
        req = make_osrm_request(base_url, coordinates = contents['coordinates'],
                                          sources = [id])

        durations = extract_durations(req)

        # df = results_to_df(origin, destinations = contents['destinations'],
        #                            durations = durations)

        # df.to_csv(os.path.join(out_dir, f'matrix_subset_{origin}.csv'), index = False)

        with open(os.path.join(parts_path, f'subset_{origin}.csv'), 'w') as csvfile:
            csvwriter = csv.writer(csvfile)

            for i, dest in enumerate(contents['destinations']):
                csvwriter.writerow([origin, dest, round(durations[i] / 60, 2)])

        print(f'{count} of {total} completed')
        count += 1

        # time.sleep(5)



def get_packages_subset(subset_ids, packages):
    subset = {}
    for id in subset_ids:
        subset[id] = packages[str(id)]

    return subset
