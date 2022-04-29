import tempfile

import lightgbm as lgb
import numpy as np
import pandas as pd
import requests

from coalescenceml.integrations.constants import LIGHTGBM
from coalescenceml.logger import get_logger
from coalescenceml.pipeline import pipeline
from coalescenceml.step import BaseStepConfig, Output, step

logger = get_logger(__name__)

TRAIN_SET_RAW = "https://raw.githubusercontent.com/microsoft/LightGBM/master/examples/regression/regression.train"
TEST_SET_RAW = "https://raw.githubusercontent.com/microsoft/LightGBM/master/examples/regression/regression.test"


lgb.register_logger(logger)


class LightGBMConfig(BaseStepConfig):
    boosting_type: str = "gbdt"
    objective: str = "regression"
    num_leaves: int = 32
    learning_rate: float = 1e-2
    feature_fraction: float = 0.8
    bagging_fraction: float = 0.7
    bagging_freq: int = 5
    verbose: int = -1


@step
def data_loader() -> Output(train_mat=lgb.Dataset, test_mat=lgb.Dataset):
    with tempfile.NamedTemporaryFile(
            mode="w", delete=True, encoding="utf-8"
    ) as f:
        f.write(requests.get(TRAIN_SET_RAW).text)
        df_train = pd.read_csv(f.name, header=None, sep="\t")

    with tempfile.NamedTemporaryFile(
            mode="w", delete=True, encoding="utf-8"
    ) as f:
        f.write(requests.get(TEST_SET_RAW).text)
        df_test = pd.read_csv(f.name, header=None, sep="\t")

    # Parse dataframes
    y_train = df_train[0]
    y_test = df_test[0]
    X_train = df_train.drop(0, axis=1)
    X_test = df_test.drop(0, axis=1)

    # Creat lgb dataset
    train_mat = lgb.Dataset(X_train, y_train, params={"verbose": -1})
    test_mat = lgb.Dataset(X_test, y_test, params={"verbose": -1})
    return train_mat, test_mat


@step
def trainer(
    config: LightGBMConfig, train_mat: lgb.Dataset, test_mat: lgb.Dataset,
) -> Output(model=lgb.Booster):
    params = {
        "boosting_type": config.boosting_type,
        "objective": config.objective,
        "num_leaves": config.num_leaves,
        "learning_rate": config.learning_rate,
        "feature_fraction": config.feature_fraction,
        "bagging_fraction": config.bagging_fraction,
        "bagging_freq": config.bagging_freq,
        "verbose": config.verbose,
    }

    gbm = lgb.train(
        params,
        train_mat,
        num_boost_round=20,
        valid_sets=test_mat,
        callbacks=[lgb.early_stopping(stopping_rounds=5)],
    )
    return gbm


@step
def evaluator(
    model: lgb.Booster, test_mat: lgb.Dataset
) -> Output(preds=np.ndarray):
    return model.predict(np.random.rand(7,28))


@pipeline(enable_cache=False, required_integrations=[LIGHTGBM])
def lgbm_pipeline(
    data_loader,
    trainer,
    evaluator,
):
    train_mat, test_mat = data_loader()
    model = trainer(train_mat, test_mat)
    evaluator(model, test_mat)


if __name__ == '__main__':
    pipe = lgbm_pipeline(
        data_loader(),
        trainer(),
        evaluator(),
    )
    pipe.run()
