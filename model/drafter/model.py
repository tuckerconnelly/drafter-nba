"""
The model
"""

import os
import logging
import pprint

from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import mean_squared_error

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
    the_data = data.load_data(
        max_samples=MAX_SAMPLES,
        train_split=1 if final_model else 0.9,
        validation_split=0 if final_model else 0,
        test_split=0.2 if final_model else 0.1,
        batch_size=BATCH_SIZE
    )

    regressor = RandomForestRegressor(
        max_depth=2,
        random_state=0,
        n_estimators=100,
        verbose=3,
        n_jobs=-1
    )
    # regressor = GradientBoostingRegressor(
    #     n_estimators=500,
    #     max_depth=4,
    #     min_samples_split=2,
    #     learning_rate=0.01,
    #     loss='ls',
    #     verbose=3
    # )

    logging.debug('Fitting...')
    regressor.fit(the_data['train_X'], the_data['train_y'].flatten(), sample_weight=the_data['train_w'])

    y_pred = regressor.predict(the_data['test_X'])
    mse = mean_squared_error(the_data['test_y'], y_pred, sample_weight=the_data['test_w'])
    print({
        'mse': mse,
        'rmse': mse ** 0.5
    })



if __name__ == '__main__':
    fit(final_model=os.environ.get('FINAL', False))
