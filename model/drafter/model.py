"""
The model
"""

import os
import logging
import pprint

import numpy as np
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import RandomizedSearchCV
from sklearn.model_selection import train_test_split
import scipy.stats as st
import xgboost as xgb
from joblib import dump, load

import data


logging.basicConfig(level=logging.DEBUG)


MAX_SAMPLES = None
BATCH_SIZE = 5000
EPOCHS = 20
PLAYER_LOSS_PLAYER_LIMIT = None
PLAYER_EPOCHS = 20

if os.environ.get('DEBUG') is not None:
    MAX_SAMPLES = 10000
    BATCH_SIZE = 50
    EPOCHS = 5
    PLAYER_EPOCHS = 5
    PLAYER_LOSS_PLAYER_LIMIT = 10


def fit(final_model=False):
    """
    Fits the model
    """
    mapped_data = data.get_mapped_data()

    X_train, X_test, y_train, y_test, sw_train, sw_test = train_test_split(
        mapped_data['X'], np.stack(mapped_data['y']).flatten(), mapped_data['w'], test_size=0.2, random_state=0)
    X_train, X_val, y_train, y_val, sw_train, sw_val = train_test_split(
        X_train, y_train, sw_train, test_size=0.2, random_state=0)

    print(pprint.pformat({
        'X_train': np.array(X_train).shape,
        'y_train': np.array(y_train).shape,
        'sw_train': np.array(sw_train).shape,

        'X_test': np.array(X_test).shape,
        'y_test': np.array(y_test).shape,
        'sw_test': np.array(sw_test).shape,

        'X_val': np.array(X_val).shape,
        'y_val': np.array(y_val).shape,
        'sw_val': np.array(sw_val).shape
    }))

    # From http://danielhnyk.cz/how-to-use-xgboost-in-python/

    params = {
        'colsample_bytree': 1,
        'gamma': 0.4,
        'learning_rate': 0.1,
        'max_depth': 42,
        'min_child_weight': 32,
        'n_estimators': 100,
        'reg_alpha': 100,
        'subsample': 1
    }

    regressor = xgb.XGBRegressor(
        random_state=0,
        n_jobs=-1,
        silent=False,
        **params
    )

    logging.debug('Fitting...')

    regressor.fit(
        X_train,
        y_train,
        sample_weight=sw_train,
        eval_metric='rmse',
        eval_set=[(X_val, y_val)],
        sample_weight_eval_set=[sw_val],
        early_stopping_rounds=10,
        verbose=True
    )

    mapped_data = data.get_mapped_data(
        player_basketball_reference_id='shumpim01'
    )

    y_pred = regressor.predict(mapped_data['X'])
    mse = mean_squared_error(mapped_data['y'], y_pred, sample_weight=mapped_data['w'])
    print({
        'mse': mse,
        'rmse': mse ** 0.5
    })


def grid_search():
    the_data = data.load_data(
        max_samples=MAX_SAMPLES,
        train_split=1,
        batch_size=BATCH_SIZE
    )

    # From http://danielhnyk.cz/how-to-use-xgboost-in-python/

    one_to_left = st.beta(10, 1)
    from_zero_positive = st.expon(0, 50)

    params = {
        'n_estimators': st.randint(3, 40),
        'max_depth': st.randint(3, 40),
        'learning_rate': st.uniform(0.05, 0.4),
        'colsample_bytree': one_to_left,
        'subsample': one_to_left,
        'gamma': st.uniform(0, 10),
        'reg_alpha': from_zero_positive,
        'min_child_weight': from_zero_positive,
    }

    regressor = xgb.XGBRegressor(
        random_state=0,
        n_jobs=-1,
        silent=False
    )

    logging.debug('Fitting...')

    gs = RandomizedSearchCV(
        regressor,
        params,
        n_iter=10,
        n_jobs=1,
        random_state=0,
        scoring='neg_mean_squared_error'
    )
    gs.fit(
        the_data['train_X'],
        the_data['train_y'].flatten(),
        sample_weight=the_data['train_w'],
        verbose=True
    )
    print(gs.cv_results_)
    print(gs.best_params_)
    print(gs.best_score_)
    dump(gs.best_estimator_, 'tmp/xgb.joblib')


if __name__ == '__main__':
    fit(final_model=os.environ.get('FINAL', False))
