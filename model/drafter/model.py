"""
The model
"""

import os
import logging
import pprint
import copy
import json

import numpy as np
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import RandomizedSearchCV
from sklearn.model_selection import train_test_split
import scipy.stats as st
import xgboost as xgb
from joblib import dump, load
from keras.models import Sequential
from keras.engine.input_layer import Input
from keras.layers import Dense, BatchNormalization, ELU, Dropout
from keras.callbacks import EarlyStopping, ProgbarLogger
from keras.models import load_model as keras_load_model
import progressbar
import random_name
from terminaltables import AsciiTable


import data

np.random.seed(0)
os.environ['KERAS_BACKEND'] = 'plaidml.keras.backend'
logging.basicConfig(level=logging.DEBUG)

MAX_SAMPLES = None
BATCH_SIZE = 128
PLAYER_BATCH_SIZE = 64
EPOCHS = 30
PLAYER_LOSS_PLAYER_LIMIT = None
PLAYER_EPOCHS = 20
MODEL_DIR = 'tmp/models'

if os.environ.get('DEBUG') is not None:
    MAX_SAMPLES = 1000
    BATCH_SIZE = 32
    PLAYER_BATCH_SIZE = 16
    EPOCHS = 1
    PLAYER_EPOCHS = 1
    PLAYER_LOSS_PLAYER_LIMIT = 3


def make_model(input_dim):
    return Sequential([
        Dense(500, input_dim=input_dim),
        BatchNormalization(),
        ELU(),
        Dropout(0.5),

        Dense(500),
        BatchNormalization(),
        ELU(),
        Dropout(0.5),

        Dense(500),
        BatchNormalization(),
        ELU(),
        Dropout(0.5),

        Dense(500),
        BatchNormalization(),
        ELU(),
        Dropout(0.5),

        Dense(1)
    ])


def fit(final_model=False):
    '''
    Fits the model
    '''

    mapped_data = data.get_mapped_data(limit=MAX_SAMPLES)
    x_train, x_test, y_train, y_test, sw_train, sw_test = train_test_split(
        mapped_data['x'], mapped_data['y'], mapped_data['sw'], test_size=0.2, random_state=0)
    x_train, x_val, y_train, y_val, sw_train, sw_val = train_test_split(
        x_train, y_train, sw_train, test_size=0.2, random_state=0)
    if final_model:
        x_train = mapped_data['x']
        y_train = mapped_data['y']
        sw_train = mapped_data['sw']

    num = str(len([i for i in os.walk(MODEL_DIR)]))
    MODEL_NAME = num + '-' + random_name.generate_name()
    print(MODEL_NAME)
    model = make_model(input_dim=len(x_train[0]))
    model.compile('adam', loss='mse', metrics=['mse'])

    model.fit(
        x=x_train,
        y=y_train,
        sample_weight=sw_train,
        batch_size=BATCH_SIZE,
        epochs=EPOCHS,
        verbose=1,
        validation_data=(x_val, y_val, sw_val),
        callbacks=[
            EarlyStopping(patience=5)
        ]
    )

    y_pred = model.predict(x_test)
    mse = mean_squared_error(y_test, y_pred, sample_weight=sw_test)
    losses = {
        'mse': mse,
        'rmse': mse ** 0.5
    }
    print(losses)
    save_model(model, MODEL_NAME, losses)

    make_player_models(model, MODEL_NAME, final_model=final_model)


def make_player_models(original_model, model_name, final_model=False):
    # data.cache_player_data()

    players = list(data.get_players())

    if PLAYER_LOSS_PLAYER_LIMIT:
        players = players[0:PLAYER_LOSS_PLAYER_LIMIT]

    losses = []

    widgets = [
        ' [', progressbar.Timer(), '] ',
        progressbar.Bar(),
        ' (', progressbar.ETA(), ') '
    ]

    for i in progressbar.progressbar(range(len(players)), widgets=widgets):
        player_basketball_reference_id = players[i]

        mapped_data = data.get_mapped_data(
            player_basketball_reference_id=player_basketball_reference_id)
        if (len(mapped_data['x']) < 10):
            continue
        x_train, x_test, y_train, y_test, sw_train, sw_test = train_test_split(
            mapped_data['x'], mapped_data['y'], mapped_data['sw'], test_size=0.2, random_state=0)
        x_train, x_val, y_train, y_val, sw_train, sw_val = train_test_split(
            x_train, y_train, sw_train, test_size=0.2, random_state=0)
        if final_model:
            x_train = mapped_data['x']
            y_train = mapped_data['y']
            sw_train = mapped_data['sw']

        model = make_model(input_dim=len(x_train[0]))
        model.set_weights(original_model.get_weights())
        for layer in model.layers[:-9]:
            layer.trainable = False
        if os.environ.get('DEBUG') is not None:
            for layer in model.layers:
                print(layer, layer.trainable)
        model.compile('adam', loss='mse', metrics=['mse'])

        model.fit(
            x=x_train,
            y=y_train,
            sample_weight=sw_train,
            batch_size=PLAYER_BATCH_SIZE,
            epochs=PLAYER_EPOCHS,
            verbose=0,
            validation_data=(x_val, y_val, sw_val),
            callbacks=[
                EarlyStopping(patience=5)
            ]
        )

        y_pred = model.predict(x_test)
        mse = mean_squared_error(y_test, y_pred, sample_weight=sw_test)

        player_losses = {
            'player_basketball_reference_id': player_basketball_reference_id,
            'train_samples': len(x_train),
            'test_samples': len(x_test),

            'mse': mse,
            'rmse': mse ** 0.5
        }
        save_model(model, model_name + '/' +
                   player_basketball_reference_id, player_losses)
        losses.append(player_losses)

    losses = sorted(losses, key=lambda k: k['rmse'])

    table_data = [list(losses[0].keys())] + [list(l.values()) for l in losses]
    print('\n'.join([' '.join([str(c) for c in r]) for r in table_data]))
    table = AsciiTable(table_data)
    print(table.table)


def predict(model, batch):
    mappers = data.make_mappers()
    batch_x = np.stack([mappers.datum_to_x(datum) for datum in batch])
    predictions = model.predict_on_batch(batch_x)

    return_batch = []
    for i in range(len(batch)):
        datum = copy.deepcopy(batch[i])
        datum['_predictions'] = mappers.datum_to_y(predictions[i])
        datum['_losses'] = model.drafter_losses
        return_batch.push(datum)

    return return_batch


if not os.path.exists(MODEL_DIR):
    os.makedirs(MODEL_DIR)


def save_model(model, model_name, losses={}):
    directory = MODEL_DIR + '/' + model_name
    if not os.path.exists(directory):
        os.makedirs(directory)
    model.save(directory + '/model.h5')
    with open(directory + '/losses.json', 'w') as fp:
        json.dump(losses, fp)


def load_model(model_name):
    model = keras_load_model(MODEL_DIR + '/' + model_name + '/model.h5')
    with open(MODEL_DIR + '/' + model_name + '/losses.json', 'w') as fp:
        setattr(model, 'losses', json.load(fp))
    return model


if __name__ == '__main__':
    fit(final_model=os.environ.get('FINAL', False))
