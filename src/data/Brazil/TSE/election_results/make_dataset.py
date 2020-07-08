# -*- coding: utf-8 -*-
import pandas as pd
import warnings
from ast import literal_eval

make_political_office = __import__('1_make_political_office_dataset')
merge_datasets = __import__('2_merge_political_office_ds_with_polling_place_ds')
clean_dataset = __import__('3_clean_ds_by_per')
generate_statistics = __import__('4_make_ds_statistics')

warnings.filterwarnings('ignore')


def make_interim_data(params):
    params = params.drop_duplicates(subset=['year', 'political_office', 'office_folder', 'turn'],
                                    keep='first')
    for index, parameters in params.iterrows():
        make_political_office.run(year=parameters.year,
                                  political_office=parameters.political_office,
                                  office_folder=parameters.office_folder,
                                  turn=parameters.turn)

        merge_datasets.run(year=parameters.year,
                           office_folder=parameters.office_folder,
                           turn=parameters.turn)


def make_processed_data(params):
    for index, parameters in params.iterrows():
        per = clean_dataset.run(year=parameters.year,
                                office_folder=parameters.office_folder,
                                turn=parameters.turn,
                                candidates=parameters.candidates,
                                city_limits=parameters.city_limits,
                                levenshtein_threshold=parameters.levenshtein_threshold,
                                precision_categories=parameters.precision_categories,
                                aggregate_level=parameters.aggregate_level)

        generate_statistics.run(year=parameters.year,
                                office_folder=parameters.office_folder,
                                turn=parameters.turn,
                                candidates=parameters.candidates,
                                city_limits=parameters.city_limits,
                                levenshtein_threshold=parameters.levenshtein_threshold,
                                precision_categories=parameters.precision_categories,
                                aggregate_level=parameters.aggregate_level,
                                per=per)


if __name__ == '__main__':
    param_sets = pd.read_csv('parameters_to_generate_ds.csv', converters={'candidates': literal_eval,
                                                                          'city_limits': literal_eval,
                                                                          'precision_categories': literal_eval})
    # Set executions
    make_interim = False
    make_processed = True

    if make_interim:
        make_interim_data(param_sets)
    if make_processed:
        make_processed_data(param_sets)
