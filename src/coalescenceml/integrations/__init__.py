"""
The CoalescenceML integrations module contains sub-modules for each integration that we
support. This includes orchestrators like Apache Airflow as well as deep
learning libraries like PyTorch.
"""
# Imports here when we have them lol
from coalescenceml.integrations.azure_datalake import AzureIntegration
from coalescenceml.integrations.sklearn import SKLearnIntegration
from coalescenceml.integrations.statsmodels import StatsmodelsIntegration
from coalescenceml.integrations.tensorflow import TFIntegration
from coalescenceml.integrations.xgboost import XgboostIntegration