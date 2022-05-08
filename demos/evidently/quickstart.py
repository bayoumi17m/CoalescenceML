from coalescenceml.integrations.evidently.step.evidently_profilestep import EvidentlyProfileTypes, TaskType
import numpy as np
import pandas as pd


from sklearn import model_selection
from sklearn.ensemble import RandomForestClassifier

from coalescenceml.pipeline import pipeline
from coalescenceml.step import BaseStepConfig, Output, step
from coalescenceml.integrations.constants import EVIDENTLY
from coalescenceml.integrations.evidently.step import (
    EvidentlyProfileConfig,
    EvidentlyProfileStep,
)

@step
def importer() -> Output(
    train_data=pd.DataFrame, test_data=pd.DataFrame, features=list
):
    data = pd.read_csv('WineQT.csv')

    train_data, test_data = model_selection.train_test_split(data, random_state=0)
    features = train_data.columns[:-2]

    #prep dataframe
    train_data = train_data.reset_index()
    train_data = train_data.drop(columns = ['index', 'Id'])
    test_data = test_data.reset_index()
    test_data = test_data.drop(columns = ['index', 'Id'])
    train_data.columns = ['target' if x == 'quality' else x for x in train_data.columns]
    test_data.columns = ['target' if x == 'quality' else x for x in test_data.columns]

    return train_data, test_data, features



@step
def trainer(
    train_data: pd.DataFrame,
    test_data: pd.DataFrame,
    features: list,
) -> Output(merged_train=pd.DataFrame, merged_test=pd.DataFrame):
    """"""
    target = "quality"

    model = RandomForestClassifier(random_state=1)
    model.fit(train_data[features], train_data[target])
    train_probas = pd.DataFrame(model.predict_proba(train_data[features]))
    test_probas = pd.DataFrame(model.predict_proba(test_data[features]))

    # Model Performance
    train_probas_head = train_data['quality'].drop_duplicates().sort_values().reset_index()
    train_probas.columns = train_probas_head['quality']

    test_probas_head = test_data['quality'].drop_duplicates().sort_values().reset_index()
    test_probas.columns = test_probas_head['quality']

    merged_train_data = pd.concat([train_data, train_probas], axis = 1)
    merged_test_data = pd.concat([test_data, test_probas], axis = 1)

    return merged_train_data, merged_test_data


@step
def profile_drift(
    train_data: pd.DataFrame,
    test_data: pd.DataFrame,
) -> Output(mse=float):
    # mse = np.mean(np.power(X_test@model - y_test, 2))
    config = EvidentlyProfileConfig(TaskType.Classification, target = "target")   
    html = EvidentlyProfileStep().entrypoint(config, train_data, test_data, [EvidentlyProfileTypes.CatTargetDrift], json=False)
    return html



@pipeline(required_integrations=[EVIDENTLY])
def sample_pipeline(importer, trainer, profile_drift):
    train_data, test_data, features = importer()
    merged_train_data, merged_test_data = trainer(train_data, test_data, features)
    html = profile_drift(merged_train_data, merged_test_data)

if __name__ == '__main__':
    pipe_1 = sample_pipeline(
        importer=importer(),
        trainer=trainer(),
        profiler=profile_drift()
    )
    pipe_1.run()

