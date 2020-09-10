# -*- coding: utf-8 -*-
import warnings
import logging
import pandas as pd
import numpy as np
from os import listdir
from os.path import join
from sklearn.linear_model import Lasso
from sklearn.metrics import mean_squared_error, mean_absolute_error
from math import sqrt
from tqdm import tqdm
warnings.filterwarnings('ignore')


def fit_model(train, alpha):
    target = train['NM_UF_STRANGENESS']
    train_cols = train.columns.values[20:-3]
    train = train[train_cols]
    lasso = Lasso(alpha=alpha)
    lasso.fit(train, target)
    return lasso


def eval_test(model, test):
    y_true = test['NM_UF_STRANGENESS']
    test_cols = test.columns.values[20:-3]
    test = test[test_cols]
    y_pred = model.predict(test)
    mae = mean_absolute_error(y_true=y_true, y_pred=y_pred)
    rmse = sqrt(mean_squared_error(y_true=y_true, y_pred=y_pred))
    return rmse, mae


def features_selected(model, features):
    indexes_features = np.where(model.coef_ != 0)
    selected = features[indexes_features]
    return selected


def run(input_filepath_matched, output_filepath, alpha):
    # Log text to show on screen
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)
    logger = logging.getLogger(__name__)
    # Get the folds
    folds = [fold for fold in listdir(input_filepath_matched)]
    l_rmse = []
    l_mae = []
    l_features = []
    for fold in tqdm(folds):
        fold_path = join(input_filepath_matched, fold)
        train = pd.read_csv(join(fold_path, 'train.csv'))
        test = pd.read_csv(join(fold_path, 'test.csv'))
        model = fit_model(train, alpha)
        s_features = features_selected(model, train.columns.values[20:-3])
        s_features = ','.join(s_features)
        l_features.append(s_features)

        rmse, mae = eval_test(model, test)
        l_rmse.append(rmse)
        l_mae.append(mae)

    results = {'fold': folds,
               'rmse': l_rmse,
               'mae': l_mae,
               'selected_features': l_features
               }
    results = pd.DataFrame(results)
    results.to_csv(join(output_filepath, 'lasso(alpha_{}).csv'.format(alpha)), index=False)
    return results

