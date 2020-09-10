# -*- coding: utf-8 -*-
import weka.core.jvm as jvm
import mlflow
import warnings
import random
import pandas as pd
import numpy as np
import scipy.stats as stats
from os import environ, listdir
from os.path import join
from pathlib import Path
from dotenv import find_dotenv, load_dotenv
from weka.core.dataset import create_instances_from_matrices
from weka.attribute_selection import ASSearch, ASEvaluation, AttributeSelection

from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, mean_absolute_error
from skfeature.function.information_theoretical_based import FCBF
from skfeature.function.statistical_based import CFS
from skrebate import ReliefF
from scipy.stats import rankdata
from math import sqrt
from statistics import mean
from tqdm import tqdm
jvm.start()
warnings.filterwarnings('ignore')


def get_folds_path(parameters, data_dir):
    # Get folds path
    path_folds = data_dir + environ.get('{}_MATCHED_DATA'.format(parameters['region']))
    path_folds = join(path_folds, 'tse{}_ibge{}'.format(parameters['tse_year'], parameters['ibge_year']))
    path_folds = join(path_folds, '{}_{}'.format(parameters['tse_office'], parameters['tse_turn']))
    path_folds = join(path_folds, parameters['ibge_aggr'], parameters['tse_per'], parameters['fold_name'])
    return path_folds


def get_rep_folds(list_folds, k, rep):
    repetition_cv = dict()
    for i in range(rep):
        repetition_cv[str(i)] = random.sample(list_folds, k)
        list_folds = [f for f in list_folds if f not in repetition_cv[str(i)]]
    return repetition_cv


def create_eval_dict():
    return {'rmse_test': [],
            'mae_test': [],
            'rmse_train': [],
            'mae_train': []
            }


def make_train_test(path, fold, n_output, n_codes):
    train = pd.read_csv(join(path, fold, 'train.csv'))
    test = pd.read_csv(join(path, fold, 'test.csv'))
    input_space = train.columns.values[n_codes + 1:-2 * n_output]
    output_space = range(-2, -2*n_output-1, -2)
    output_space = train.columns.values[output_space]
    discret_output_space = range(-1, -2*n_output, -2)
    discret_output_space = train.columns.values[discret_output_space]
    return train[input_space], train[output_space], train[discret_output_space], test[input_space], test[output_space]


def calculate_regression_metrics(model, x, y_true):
    y_pred = model.predict(x)
    mae = mean_absolute_error(y_true=y_true, y_pred=y_pred)
    rmse = sqrt(mean_squared_error(y_true=y_true, y_pred=y_pred))
    return mae, rmse


def calculate_mean_wkendall(model, x, y_true, n_outputs):
    y_pred = model.predict(x)
    results = dict()
    i = 0
    for key, y_true in y_true.iteritems():
        level_pred = np.array([pred[i] for pred in y_pred])
        rank_pred = rankdata(-level_pred, method='ordinal')
        rank_true = rankdata(-y_true, method='ordinal')
        wkendall = stats.weightedtau(rank_true, rank_pred)
        results[key] = wkendall[0]
        i += 1
    return results


def baseline_spatial_cross_validation(model, fs_method, path, repetition_cv, n_output, n_codes, method):
    rep_metrics = create_eval_dict()
    for cv in tqdm(repetition_cv):
        cv_metrics = create_eval_dict()
        for fold in repetition_cv[cv]:
            x_train, y_train, disc_y_train, x_test, y_test = make_train_test(path, fold, n_output, n_codes)
            if fs_method is not None:
                # ==============================
                x_train_fs = x_train.copy()
                x_train_fs['target'] = disc_y_train.quantile_NM_UF_STRANGENESS
                x_train_fs = create_instances_from_matrices(x_train_fs.to_numpy())
                x_train_fs.class_is_last()
                # ===============================
                search = ASSearch(classname="weka.attributeSelection.BestFirst", options=["-D", "1", "-N", "5"])
                evaluator = ASEvaluation(classname="weka.attributeSelection.CfsSubsetEval", options=["-P", "1", "-E", "1"])
                attsel = AttributeSelection()
                attsel.search(search)
                attsel.evaluator(evaluator)
                attsel.select_attributes(x_train_fs)
                index_fs = [i - 1 for i in attsel.selected_attributes]
                s_features = x_train.columns.values[index_fs]
                x_train = x_train[s_features]
                x_test = x_test[s_features]
                # =============================
            model.fit(x_train, y_train)
            # ===========================
            mae, rmse = calculate_regression_metrics(model, x_train, y_train)
            cv_metrics['rmse_train'].append(rmse)
            cv_metrics['mae_train'].append(mae)
            # ===========================
            mae, rmse = calculate_regression_metrics(model, x_test, y_test)
            cv_metrics['rmse_test'].append(rmse)
            cv_metrics['mae_test'].append(mae)
            # ===========================
            wkendall = calculate_mean_wkendall(model, x_train, y_train, n_output)
            for key in wkendall:
                try:
                    cv_metrics[key+'_wkendall'].append(wkendall[key])
                except KeyError:
                    cv_metrics[key+'_wkendall'] = [wkendall[key]]

        for key in cv_metrics:
            try:
                rep_metrics[key].append(mean(cv_metrics[key]))
            except KeyError:
                rep_metrics[key] = [mean(cv_metrics[key])]

    final_metrics_l = [{key: mean(rep_metrics[key])} for key in rep_metrics]
    final_metrics = {}
    for entry in final_metrics_l:
        final_metrics.update(entry)

    final_metrics = pd.DataFrame(final_metrics, index=[method])
    return final_metrics


if __name__ == '__main__':
    # Set dataset parameters
    parameters = {'region': 'RS',
                  'tse_year': '2018',
                  'tse_office': 'president',
                  'tse_turn': 'turn_2',
                  'tse_aggr': 'aggr_by_polling_place',
                  'tse_per': 'PER_99.33031',
                  'candidates': ['FERNANDO HADDAD', 'JAIR BOLSONARO'],
                  'ibge_year': '2010',
                  'ibge_aggr': 'weighting_area',
                  'fold_name': 'folds_by_Municipio'}
    # =============================================
    # Find data.env automatically by walking up directories until it's found
    dotenv_path = find_dotenv(filename='data.env')
    # Load up the entries as environment variables
    load_dotenv(dotenv_path)
    # Get data root path
    data_dir = environ.get('ROOT_DATA')
    folds_path = get_folds_path(parameters, data_dir)
    # Results path
    path_results = data_dir + environ.get('{}_RESULTS_HIERARCHICAL_FEATURE_SELECTION'.format(parameters['region']))
    output_filepath = path_results.format('baselines')
    # =============================================
    # Project path
    project_dir = str(Path(__file__).resolve().parents[2])
    # Load up the entries as environment variables
    load_dotenv(dotenv_path)
    # Get election results path
    path = 'file:' + project_dir + environ.get("{}_EXPERIMENTS".format(parameters['region']))
    # Set mflow log dir
    mlflow.set_tracking_uri(path)
    try:
        mlflow.create_experiment('Hierarchical Feature Selection')
    except:
        mlflow.set_experiment('Hierarchical Feature Selection')

    # Spatial Cross Validation
    folds = [fold for fold in listdir(folds_path)]
    rep_cv = get_rep_folds(folds, k=10, rep=1)
    l_results = []
    l_results.append(baseline_spatial_cross_validation(LinearRegression(), None, folds_path, rep_cv, n_output=3, n_codes=19, method='baseline'))
    l_results.append(baseline_spatial_cross_validation(LinearRegression(), 'csf', folds_path, rep_cv, n_output=3, n_codes=19, method='csf baseline'))
    #l_results.append(
    #    baseline_spatial_cross_validation(LinearRegression(), 'cfs', folds_path, rep_cv, n_output=3, n_codes=19,
    #                                      method='cfs baseline'))

    final_result = pd.concat(l_results)
    final_result.to_csv(join(output_filepath, 'results.csv'))
    jvm.stop()
