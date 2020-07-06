import folium
import branca.colormap as cm
import geopandas as geopd
from folium.plugins import FastMarkerCluster


def calculate_integrity(data):
    tse_score = sum(data['precision'] == 'TSE') * 1
    rooftop_score = sum(data['precision'] == 'ROOFTOP') * 0.8
    ri_score = sum(data['precision'] == 'RANGE_INTERPOLATED') * 0.6
    gc_score = sum(data['precision'] == 'GEOMETRIC_CENTER') * 0.4
    approximate_score = sum(data['precision'] == 'APPROXIMATE') * 0.2
    nv_score = sum(data['precision'] == 'NO_VALUE') * 0.0

    precision_score = (tse_score + rooftop_score + ri_score + gc_score + approximate_score + nv_score) / len(data)

    in_score = sum(data['city_limits'] == 'in') * 1

    # Add manually the others
    boundary01_score = sum(data['city_limits'] == 'boundary_0.01') * 0.99
    boundary02_score = sum(data['city_limits'] == 'boundary_0.02') * 0.98
    boundary03_score = sum(data['city_limits'] == 'boundary_0.03') * 0.97

    out_score = sum(data['city_limits'] == 'out') * 0

    city_limits_score = (in_score + out_score + boundary01_score + boundary02_score + boundary03_score) / len(data)

    levenstein_score = sum(data['lev_dist']) / len(data)

    final_score = (precision_score + city_limits_score + levenstein_score) / 3

    return final_score

def generate_markdown_report(data,
                             filtered_data,
                             output_filepath,
                             integrity_score,
                             city_limits,
                             levenstein_threshold,
                             precision_categories,
                             aggregate_level):
    parameters_report = {'Aggregate level': aggregate_level,
                         'City limits:': [city_limits],
                         'Precision categories': [precision_categories],
                         'Levenstein threshold': levenstein_threshold}
    report_df = pd.DataFrame(parameters_report)
    parammeters_markdown = "## Filter Parameters \n" + report_df.to_markdown(showindex=False)

    cities = len(filtered_data.groupby('CD_MUNICIPIO'))
    cities_perc = 100 * cities / len(data.groupby('CD_MUNICIPIO'))
    cities = str(cities) + ' (' + str(round(cities_perc, 2)) + '%)'

    left_cities = len(data.groupby('CD_MUNICIPIO')) - len(filtered_data.groupby('CD_MUNICIPIO'))
    left_cities_perc = 100 * left_cities / len(data.groupby('CD_MUNICIPIO'))
    left_cities = str(left_cities) + ' (' + str(round(left_cities_perc, 2)) + '%)'

    rows = len(filtered_data)
    rows_perc = 100 * rows / len(data)
    rows = str(rows) + ' (' + str(round(rows_perc, 2)) + '%)'

    left_rows = len(data) - len(filtered_data)
    left_rows_perc = 100 * left_rows / len(data)
    left_rows = str(left_rows) + ' (' + str(round(left_rows_perc, 2)) + '%)'

    electorate_size = sum(filtered_data['QT_APTOS'])
    electorate_size_perc = 100 * electorate_size / sum(data['QT_APTOS'])
    electorate_size = str(electorate_size) + ' (' + str(round(electorate_size_perc, 2)) + '%)'

    left_electorate_size = sum(data['QT_APTOS']) - sum(filtered_data['QT_APTOS'])
    left_electorate_size_perc = 100 * left_electorate_size / sum(data['QT_APTOS'])
    left_electorate_size = str(left_electorate_size) + ' (' + str(round(left_electorate_size_perc, 2)) + '%)'

    turnout = sum(filtered_data['QT_COMPARECIMENTO'])
    turnout_perc = 100 * turnout / sum(data['QT_COMPARECIMENTO'])
    turnout = str(turnout) + ' (' + str(round(turnout_perc, 2)) + '%)'

    left_turnout = sum(data['QT_COMPARECIMENTO']) - sum(filtered_data['QT_COMPARECIMENTO'])
    left_turnout_perc = 100 * left_turnout / sum(data['QT_COMPARECIMENTO'])
    left_turnout = str(left_turnout) + ' (' + str(round(left_turnout_perc, 2)) + '%)'

    statistics_report = {'Cities': cities,
                         '(Not Included) Cities': left_cities,
                         aggregate_level: rows,
                         '(Not Included) ' + aggregate_level: left_rows,
                         'Electorate': electorate_size,
                         '(Not Included) Electorate': left_electorate_size,
                         'Turnout': turnout,
                         '(Not Included) Turnout': left_turnout}

    nulo = sum(filtered_data['Nulo'])
    nulo_perc = 100 * nulo / sum(data['Nulo'])
    nulo = str(nulo) + ' (' + str(round(nulo_perc, 2)) + ')'

    branco = sum(filtered_data['Branco'])
    branco_perc = 100 * branco / sum(data['Branco'])
    branco = str(branco) + ' (' + str(round(branco_perc, 2)) + '%)'

    bozo = sum(filtered_data['JAIR BOLSONARO'])
    bozo_perc = 100 * bozo / sum(data['JAIR BOLSONARO'])
    bozo = str(bozo) + ' (' + str(round(bozo_perc, 2)) + '%)'

    haddad = sum(filtered_data['FERNANDO HADDAD'])
    haddad_perc = 100 * haddad / sum(data['FERNANDO HADDAD'])
    haddad = str(haddad) + ' (' + str(round(haddad_perc, 2)) + '%)'

    votes_report = {'Null': nulo,
                    'Branco': branco,
                    'Jair Bolsonaro': bozo,
                    'Fernando Haddad': haddad}

    tse = sum(filtered_data['precision'] == 'TSE')
    rooftop = sum(filtered_data['precision'] == 'ROOFTOP')
    r_interpolated = sum(filtered_data['precision'] == 'RANGE_INTERPOLATED')
    g_center = sum(filtered_data['precision'] == 'GEOMETRIC_CENTER')
    approximate = sum(filtered_data['precision'] == 'APPROXIMATE')
    n_value = sum(filtered_data['precision'] == 'NO_VALUE')

    precision_report = {'TSE': tse,
                        'Rooftop': rooftop,
                        'Range interpolated': r_interpolated,
                        'Geometric center': g_center,
                        'Approximate': approximate,
                        'No value': n_value}

    # print(statistics_report)
    report_df = pd.DataFrame(statistics_report, index=[0])
    statistics_markdown = "\n ## Summary \n" + report_df.to_markdown(showindex=False)

    report_df = pd.DataFrame(precision_report, index=[0])
    precisions_markdown = "\n ## Precisions \n" + report_df.to_markdown(showindex=False)

    report_df = pd.DataFrame(votes_report, index=[0])
    votes_markdown = "\n ## Votes \n" + report_df.to_markdown(showindex=False)

    final_report = '# Dataset: IS_{} \n'.format(round(integrity_score,
                                                      5)) + parammeters_markdown + '\n' + statistics_markdown + '\n' + precisions_markdown + '\n' + votes_markdown

    print(final_report, file=open(output_filepath + 'IS_' + str(round(integrity_score, 5)) + '/summary.md', 'w'))


def generateBaseMap(default_location=[-22.010147, -47.890706], default_zoom_start=5):
    base_map = folium.Map(location=default_location, zoom_start=default_zoom_start)
    return base_map


def generate_plot(clusters_map, data, output_filepath, integrity_score):
    callback = ('function (row) {'
                'var circle = L.circle(new L.LatLng(row[0], row[1]));'
                'return circle};')

    clusters_map.add_child(FastMarkerCluster(data[['lat', 'lon']].values.tolist(), callback=callback))
    clusters_map.save(outfile=output_filepath + 'IS_' + str(round(integrity_score, 5)) + '/polling_places_map.html')


def generate_plot_2(data, filtered_data, path_geojson, digital_mesh, output_filepath, integrity_score):
    map = generateBaseMap()
    digital_mesh.rename(columns={'CD_GEOCMU': 'COD_LOCALIDADE_IBGE'}, inplace=True)
    digital_mesh['COD_LOCALIDADE_IBGE'] = digital_mesh['COD_LOCALIDADE_IBGE'].astype('float')

    list_city = []
    list_pl = []
    for index, city_data in data.groupby('COD_LOCALIDADE_IBGE'):
        list_city.append(index)
        list_pl.append(len(city_data))

    all_city_data = {'COD_LOCALIDADE_IBGE': list_city,
                     'all_polling_places': list_pl}
    all_city_data = pd.DataFrame(all_city_data)

    list_city = []
    list_pl = []
    for index, city_data in filtered_data.groupby('COD_LOCALIDADE_IBGE'):
        list_city.append(index)
        list_pl.append(len(city_data))

    filtered_city_data = {'COD_LOCALIDADE_IBGE': list_city,
                          'filtered_polling_places': list_pl}

    filtered_city_data = pd.DataFrame(filtered_city_data)

    city_data = all_city_data.merge(filtered_city_data, on='COD_LOCALIDADE_IBGE', how='left')

    city_data = digital_mesh.merge(city_data, on='COD_LOCALIDADE_IBGE', how='left')

    city_data['filtered_polling_places'].fillna(0, inplace=True)
    city_data['pl_perc'] = 100 * city_data['filtered_polling_places'] / city_data['all_polling_places']
    city_data['COD_LOCALIDADE_IBGE'] = city_data['COD_LOCALIDADE_IBGE'].astype('int64')

    city_data.rename(columns={'COD_LOCALIDADE_IBGE': 'CD_GEOCMU'}, inplace=True)
    city_data['pl_perc'].fillna(0, inplace=True)

    step = cm.LinearColormap(['#F9F9F3', '#bedbea', '#59a2cb', '#2166ac'],
                             vmin=city_data['pl_perc'].min(), vmax=city_data['pl_perc'].max())

    folium.GeoJson(data=city_data,
                   name='Br Cities',
                   tooltip=folium.GeoJsonTooltip(fields=['NM_MUNICIP', 'pl_perc'],
                                                 aliases=[
                                                     '<div style="background-color: lightyellow; color: black; padding: 3px; border: 2px solid black; border-radius: 3px;">' + item + '</div>'
                                                     for item in ['City', 'Percentual']],
                                                 labels=True,
                                                 sticky=True),
                   style_function=lambda city: {'fillColor': step(city['properties']['pl_perc']),
                                                'color': 'black',
                                                'fillOpacity': 0.7,
                                                'lineOpacity': 0.1,
                                                'weight': 0.3}).add_to(map)

    map.add_child(step)

    # FloatImage('https://i.ibb.co/pRQmCBR/legend-hotspots.png', bottom=75, left=80).add_to(m)

    # folium.LayerControl().add_to(m)
    # Save to html
    return m