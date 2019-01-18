"""
The model
"""

import os
import logging
import copy
import json

import numpy as np
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
import xgboost as xgb
import joblib
import progressbar
import random_name
from terminaltables import AsciiTable


import data
import scraping

np.random.seed(0)
logging.basicConfig(level=logging.DEBUG)

MODEL_DIR = 'tmp/models'

MAX_SAMPLES = None
PLAYER_LOSS_PLAYER_LIMIT = None

if os.environ.get('DEBUG') is not None:
    MAX_SAMPLES = 1000
    PLAYER_LOSS_PLAYER_LIMIT = 3


def make_model():
    # From http://danielhnyk.cz/how-to-use-xgboost-in-python/
    params = {
        'colsample_bytree': 1,
        'gamma': 0.4,
        'learning_rate': 0.1,
        'max_depth': 42,
        'min_child_weight': 32,
        'n_estimators': 50,
        'reg_alpha': 100,
        'subsample': 1
    }

    regressor = xgb.XGBRegressor(
        random_state=0,
        n_jobs=-1,
        silent=False,
        **params
    )

    return regressor



def fit(final_model=False):
    num = str(len([i for i in os.walk(MODEL_DIR)]))
    MODEL_NAME = num + '-' + random_name.generate_name()
    print(MODEL_NAME)

    mapped_data = data.get_mapped_data(limit=MAX_SAMPLES)

    x_train, x_test, y_train, y_test, sw_train, sw_test = train_test_split(
        mapped_data['x'], mapped_data['y'], mapped_data['sw'], test_size=0.2, random_state=0)
    x_train, x_val, y_train, y_val, sw_train, sw_val = train_test_split(
        x_train, y_train, sw_train, test_size=0.2, random_state=0)

    regressor = make_model()

    logging.debug('Fitting...')

    regressor.fit(
        x_train,
        y_train,
        sample_weight=sw_train,
        eval_metric='rmse',
        eval_set=[(x_val, y_val)],
        sample_weight_eval_set=[sw_val],
        early_stopping_rounds=10,
        verbose=True
    )

    y_pred = regressor.predict(x_test)
    mse = mean_squared_error(y_test, y_pred, sample_weight=sw_test)
    losses = {
        'mse': mse,
        'rmse': mse ** 0.5
    }
    print(losses)
    save_model(regressor, MODEL_NAME, losses)

    make_player_models(regressor, MODEL_NAME, final_model=final_model)


def make_player_models(original_model, model_name, final_model=False):
    df = scraping.parse_salary_file()

    if PLAYER_LOSS_PLAYER_LIMIT:
        df = df[0:PLAYER_LOSS_PLAYER_LIMIT]

    losses = []

    widgets = [
        ' [', progressbar.Timer(), '] ',
        progressbar.Bar(),
        ' (', progressbar.ETA(), ') '
    ]

    for i in progressbar.progressbar(range(len(df)), widgets=widgets):
        row = df.iloc[i]
        player_basketball_reference_id = row['basketball_reference_id']

        mapped_data = data.get_mapped_data(
            player_basketball_reference_id=player_basketball_reference_id)
        if (len(mapped_data['x']) < 41):
            continue
        x_train, x_test, y_train, y_test, sw_train, sw_test = train_test_split(
            mapped_data['x'], mapped_data['y'], mapped_data['sw'], test_size=0.2, random_state=0)
        x_train, x_val, y_train, y_val, sw_train, sw_val = train_test_split(
            x_train, y_train, sw_train, test_size=0.2, random_state=0)
        if final_model:
            x_train = mapped_data['x']
            y_train = mapped_data['y']
            sw_train = mapped_data['sw']

        # regressor = make_model()

        # regressor.fit(
        #     x_train,
        #     y_train,
        #     sample_weight=sw_train,
        #     eval_metric='rmse',
        #     eval_set=[(x_val, y_val)],
        #     sample_weight_eval_set=[sw_val],
        #     early_stopping_rounds=10,
        #     verbose=True
        # )

        # y_pred = regressor.predict(x_test)
        # mse = mean_squared_error(y_test, y_pred, sample_weight=sw_test)

        y_pred_og = original_model.predict(x_test)
        mse_og = mean_squared_error(y_test, y_pred_og, sample_weight=sw_test)

        player_losses = {
            'player_basketball_reference_id': player_basketball_reference_id,
            'train_samples': len(x_train),
            'test_samples': len(x_test),

            'mse_og': mse_og,
            'rmse_og': mse_og ** 0.5

            # 'mse': mse,
            # 'rmse': mse ** 0.5
        }
        save_model(None, model_name + '/' +
                   player_basketball_reference_id, player_losses)
        losses.append(player_losses)

    losses = sorted(losses, key=lambda k: k['rmse_og'])

    table_data = [list(losses[0].keys())] + [list(l.values()) for l in losses]
    print('\n'.join([' '.join([str(c) for c in r]) for r in table_data]))
    table = AsciiTable(table_data)
    print(table.table)


def predict(model, model_losses, batch):
    mappers = data.make_mappers()
    batch_x = np.stack([mappers.datum_to_x(datum) for datum in batch])
    predictions = model.predict(batch_x)

    return_batch = []
    for i in range(len(batch)):
        datum = copy.deepcopy(batch[i])
        datum['_predictions'] = mappers.y_to_datum([predictions[i]])
        datum['_losses'] = model_losses
        return_batch.append(datum)

    return return_batch


if not os.path.exists(MODEL_DIR):
    os.makedirs(MODEL_DIR)


def save_model(model, model_name, losses={}):
    directory = MODEL_DIR + '/' + model_name
    if not os.path.exists(directory):
        os.makedirs(directory)
    if model is not None:
        joblib.dump(model, directory + '/model.dat')
    with open(directory + '/losses.json', 'w') as fp:
        json.dump(losses, fp)


def load_model(model_name):
    model = None
    try:
        model = joblib.load(MODEL_DIR + '/' + model_name + '/model.dat')
    except OSError:
        pass
    with open(MODEL_DIR + '/' + model_name + '/losses.json', 'r') as fp:
        losses = json.loads(fp.read())
    return model, losses


def get_latest_model_name():
    for root, dirs, files in os.walk(MODEL_DIR, topdown=True):
        return sorted(dirs)[-1]


if __name__ == '__main__':
    fit(final_model=os.environ.get('FINAL', False))
