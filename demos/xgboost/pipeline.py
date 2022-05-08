import tempfile

import numpy as np
import requests
import xgboost as xgb

from coalescenceml.integrations.constants import XGBOOST
from coalescenceml.logger import get_logger
from coalescenceml.pipeline import pipeline
from coalescenceml.step import BaseStepConfig, Output, step

logger = get_logger(__name__)


TRAIN_SET_RAW = "https://raw.githubusercontent.com/dmlc/xgboost/master/demo/data/agaricus.txt.train"
TEST_SET_RAW = "https://raw.githubusercontent.com/dmlc/xgboost/master/demo/data/agaricus.txt.test"


class XGBoostConfig(BaseStepConfig):
    max_depth: int = 1
    eta: int = 1
    objective: str = "binary:logistic"
    num_round: int = 2


@step
def data_loader() -> Output(train_mat=xgb.DMatrix, test_mat=xgb.DMatrix):
    with tempfile.NamedTemporaryFile(mode="w", delete=True, encoding="utf-8") as f:
        f.write(requests.get(TRAIN_SET_RAW).text)
        train_mat = xgb.DMatrix(f.name)

    with tempfile.NamedTemporaryFile(mode="w", delete=True, encoding="utf-8") as f:
        f.write(requests.get(TEST_SET_RAW).text)
        test_mat = xgb.DMatrix(f.name)

    return train_mat, test_mat


@step
def trainer(
    config: XGBoostConfig,
    train_mat: xgb.DMatrix,
) -> Output(model=xgb.Booster):
    num_round = config.num_round
    params = {
        "max_depth": config.max_depth,
        "eta": config.eta,
        "objective": config.objective,
    }

    return xgb.train(params, train_mat, num_round)


@step
def predictor(model: xgb.Booster, test_mat: xgb.DMatrix) -> Output(preds=np.ndarray):
    return model.predict(test_mat)


@pipeline(enable_cache=True, required_integrations=[XGBOOST])
def xgb_pipeline(
    data_loader,
    trainer,
    predictor,
):
    train_mat, test_mat = data_loader()
    model = trainer(train_mat)
    preds = predictor(model, test_mat)


if __name__ == '__main__':
    pipe = xgb_pipeline(
        data_loader(),
        trainer(),
        predictor(),
    )

    pipe.run()
