from abc import abstractmethod
from ast import Str
import json
import pandas
import enum
from typing import Optional
from coalescenceml.step.base_step import BaseStep
from evidently.model_profile import Profile
from evidently.dashboard import Dashboard

from evidently.pipeline.column_mapping import ColumnMapping

from evidently.model_profile.sections import DataQualityProfileSection, \
  DataDriftProfileSection, NumTargetDriftProfileSection, CatTargetDriftProfileSection,\
  RegressionPerformanceProfileSection, ClassificationPerformanceProfileSection,\
  ProbClassificationPerformanceProfileSection 
from evidently.dashboard.tabs import DataQualityTab, DataDriftTab, \
  NumTargetDriftTab, CatTargetDriftTab, RegressionPerformanceTab, \
  ClassificationPerformanceTab, ProbClassificationPerformanceTab

class TaskType(enum.Enum):
  Regression = 1
  Classification = 2

class EvidentlyProfileTypes(enum.Enum):
    DataQuality = 1
    DataDrift = 2
    NumTargetDrift = 3
    CatTargetDrift = 4
    RegressionPerformance = 5
    ClassificationPerformance = 6
    ProbClassificationPerformance = 7

class EvidentlyProfileConfig():
  task = TaskType.Regression
  target="target" 
  prediction="prediction" 
  datetime="datetime"
  id="id"
  numerical_features=[] 
  categorical_features=[]

  def __init__(self, 
    task : TaskType,
    target="target", 
    prediction="prediction", 
    datetime="datetime", 
    id="id",
    numerical_features=[], 
    categorical_features=[]):
      self.target = target
      self.prediction = prediction
      self.datetime = datetime
      self.id = id
      self.numerical_features = numerical_features
      self.categorical_features = categorical_features

      self.task = task


class EvidentlyProfileStep():
  def getColumnMapping(self, config : EvidentlyProfileConfig) -> ColumnMapping:
    column_mapping = ColumnMapping()
    print(column_mapping.target)
    print(column_mapping.prediction)
    print(config.prediction)

    column_mapping.target = 'target' # is the name of the column with the target function
    column_mapping.prediction = config.prediction #'pred' is the name of the column(s) with model predictions
    column_mapping.id = config.id #there is no ID column in the dataset
    column_mapping.datetime = config.datetime #'date' is the name of the column with datetime 

    column_mapping.numerical_features = config.numerical_features #list of numerical features
    column_mapping.categorical_features = config.categorical_features #list of categorical features

    column_mapping.task = 'classification' if config.task == TaskType.Classification else 'regression'

    return column_mapping

  def entrypoint(self,
      config : EvidentlyProfileConfig, 
      reference : pandas.core.frame.DataFrame, 
      current : pandas.core.frame.DataFrame,
      profiles: list,
      json = True) -> Str:    #train-test split??
    """
      Datadrift requires a reference and a current 
      Data quality either requires reference, or reference and current
      Target drift requires a reference, current, and targets and/or preds
      Regression performance requires reference and/or current, and targets AND preds

      Classification + Probabilistic require pred / targets in a specific config 
       - check evidently documentation, also requires inputs (not necessarly just the dataframe - pred)

      Returns a json string with all specified profiles included;
      if there is insufficient information to generate report, we state it in json???
    """
    # first check if columns of reference and current are the same
    evidentlyMap = {
      EvidentlyProfileTypes.DataQuality : [DataQualityTab(), DataQualityProfileSection()],
      EvidentlyProfileTypes.DataDrift : [DataDriftTab(), DataDriftProfileSection()],
      EvidentlyProfileTypes.NumTargetDrift : [NumTargetDriftTab(), NumTargetDriftProfileSection()],
      EvidentlyProfileTypes.CatTargetDrift : [CatTargetDriftTab(), CatTargetDriftProfileSection()],
      EvidentlyProfileTypes.RegressionPerformance : [RegressionPerformanceTab(), RegressionPerformanceProfileSection()],
      EvidentlyProfileTypes.ClassificationPerformance: [ClassificationPerformanceTab(), ClassificationPerformanceProfileSection()],
      EvidentlyProfileTypes.ProbClassificationPerformance: [ProbClassificationPerformanceTab(), ProbClassificationPerformanceProfileSection()],
    }
    # build tabs based on successful input 
    tabs = []
    for profile_type in profiles:
      tabs.append(evidentlyMap[profile_type][int(json is True)]) #if json, gets index 1

    print(tabs)
    if not json:
      dashboard = Dashboard(tabs=tabs)
      dashboard.calculate(reference, current, column_mapping=self.getColumnMapping(config))
      ret = dashboard.html()
    else:
      dashboard = Profile(sections=tabs)
      dashboard.calculate(reference, current, column_mapping=self.getColumnMapping(config))
      ret = dashboard.json()

    return ret
       



    
