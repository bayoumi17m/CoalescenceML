

import pandas as pd
from coalescenceml.integrations.evidently.step.evidently_profilestep import \
  EvidentlyProfileStep, EvidentlyProfileTypes
from coalescenceml.integrations.evidently.step.profile_config import \
  ProfileConfig, TaskType
import numpy as np
import json

from sklearn import model_selection
from sklearn.ensemble import RandomForestClassifier

data = pd.read_csv('WineQT.csv')

train_data, test_data = model_selection.train_test_split(data, random_state=0)

features = train_data.columns[:-2]
target = "quality"

# modeling
from sklearn.metrics import classification_report
model = RandomForestClassifier(random_state=1)
model.fit(train_data[features], train_data[target])
train_probas = pd.DataFrame(model.predict_proba(train_data[features]))
test_probas = pd.DataFrame(model.predict_proba(test_data[features]))

# Model Performance
train_probas_head = train_data['quality'].drop_duplicates().sort_values().reset_index()
train_probas.columns = train_probas_head['quality']

test_probas_head = test_data['quality'].drop_duplicates().sort_values().reset_index()
test_probas.columns = test_probas_head['quality']

#prep dataframe
train_data = train_data.reset_index()
train_data = train_data.drop(columns = ['index', 'Id'])
test_data = test_data.reset_index()
test_data = test_data.drop(columns = ['index', 'Id'])
train_data.columns = ['target' if x == 'quality' else x for x in train_data.columns]
test_data.columns = ['target' if x == 'quality' else x for x in test_data.columns]

merged_train_data = pd.concat([train_data, train_probas], axis = 1)
merged_test_data = pd.concat([test_data, test_probas], axis = 1)

# Data Drift
#wine_quality_dashboard = Dashboard(tabs=[DataDriftTab(), CatTargetDriftTab()])
#wine_quality_dashboard.calculate(train_data, test_data, column_mapping=None)
#wine_quality_dashboard.save('wine_quality_dashboard.html')

config = ProfileConfig(TaskType.Classification, target = "target", profiles=[EvidentlyProfileTypes.DataDrift, EvidentlyProfileTypes.CatTargetDrift]  )   

html = EvidentlyProfileStep.exec(self, config, merged_train_data, merged_test_data, save_loc="test_dashboard.html")