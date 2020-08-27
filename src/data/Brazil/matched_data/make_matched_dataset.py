# -*- coding: utf-8 -*-
import mlflow
import warnings
from os import environ
from pathlib import Path

from dotenv import find_dotenv, load_dotenv

spatial_join = __import__('1_make_spatial_join')
calculate_strangeness = __import__('2_calculate_strangeness')
merge_data = __import__('3_merge_census_data_and_strangeness')
make_spatial_folds = __import__('4_make_folds')

warnings.filterwarnings('ignore')


def make_data(region, tse_year, tse_office, tse_turn, tse_aggr, tse_per, candidates, ibge_year, ibge_aggr, folds_attr):
    with mlflow.start_run():
        mlflow.log_param("region", region)
        mlflow.log_param("tse_year", tse_year)
        mlflow.log_param("tse_office", tse_office)
        mlflow.log_param("tse_turn", tse_turn)
        mlflow.log_param("tse_aggr", tse_aggr)
        mlflow.log_param("tse_per", tse_per)
        mlflow.log_param("candidades", candidates)
        mlflow.log_param("ibge_year", ibge_year)
        mlflow.log_param("ibge_aggr", ibge_aggr)
        mlflow.log_param("folds_attr", folds_attr)

        spatial_join.run(region=region,
                         tse_year=tse_year,
                         tse_office=tse_office,
                         tse_turn=tse_turn,
                         tse_aggr=tse_aggr,
                         tse_per=tse_per,
                         candidates=candidates,
                         ibge_year=ibge_year,
                         ibge_aggr=ibge_aggr)

        calculate_strangeness.run(region=region,
                                  tse_year=tse_year,
                                  tse_office=tse_office,
                                  tse_turn=tse_turn,
                                  tse_per=tse_per,
                                  candidates=candidates,
                                  ibge_year=ibge_year,
                                  ibge_aggr=ibge_aggr)

        merge_data.run(region=region,
                       tse_year=tse_year,
                       tse_office=tse_office,
                       tse_turn=tse_turn,
                       tse_per=tse_per,
                       ibge_year=ibge_year,
                       ibge_aggr=ibge_aggr)

        make_spatial_folds.run(region=region,
                               tse_year=tse_year,
                               tse_office=tse_office,
                               tse_turn=tse_turn,
                               tse_per=tse_per,
                               ibge_year=ibge_year,
                               ibge_aggr=ibge_aggr,
                               fold_attr=folds_attr)

        mlflow.log_metric("Done", 1)


if __name__ == '__main__':
    # Set Parameters
    region = 'RS'
    tse_year = '2018'
    tse_office = 'president'
    tse_turn = 'turn_2'
    tse_aggr = 'aggr_by_polling_place'
    tse_per = 'PER_99.33031'
    candidates = ['FERNANDO HADDAD', 'JAIR BOLSONARO']
    ibge_year = '2010'
    ibge_aggr = 'weighting_area'
    folds_attr = 'Cod_Municipio'
    # Project path
    project_dir = str(Path(__file__).resolve().parents[5])
    # Find data.env automatically by walking up directories until it's found
    dotenv_path = find_dotenv(filename='data.env')
    # Load up the entries as environment variables
    load_dotenv(dotenv_path)
    # Get election results path
    path = 'file:' + project_dir + environ.get("{}_EXPERIMENTS".format(region))
    # Set mflow log dir
    mlflow.set_tracking_uri(path)
    try:
        mlflow.create_experiment('Make matched data sets')
    except:
        mlflow.set_experiment('Make matched data sets')

    # execute
    make_data(region, tse_year, tse_office, tse_turn, tse_aggr, tse_per, candidates, ibge_year, ibge_aggr, folds_attr)
