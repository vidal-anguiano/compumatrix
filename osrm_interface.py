
def create_json_obj(mappings):
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


def get_durations(base_url, packages, out_dir):
    count = 0
    total = len(packages)
    for origin, contents in packages.items():
        print(f'Working on {origin}...')
        req = make_osrm_request(base_url, coordinates = contents['coordinates'],
                                          sources = [0])

        durations = extract_durations(req)

        df = results_to_df(origin, destinations = contents['destinations'],
                                   durations = durations)

        df.to_csv(os.path.join(out_dir, f'matrix_subset_{origin}.csv'), index = False)
        print(f'{count} of {total} completed')
        count += 1



def get_packages_subset(subset_ids, packages):
    subset = {}
    for id in subset_ids:
        subset[id] = packages[str(id)]

    return subset
