from abc import abstractmethod
from ast import Str
import json
import pandas
import enum
from typing import Optional
from coalescenceml.monitors.ProfileConfig import ProfileConfig
from evidently.model_profile import Profile
from evidently.dashboard import Dashboard

from evidently import ColumnMapping

from evidently.model_profile.sections import DataQualityProfileSection, DataDriftProfileSection, NumTargetDriftSection
from evidently.dashboard.tabs import DataQualityTab, DataDriftTab, NumTargetDriftTab




class EvidentlyProfileTypes(enum.Enum):
    DataQualityProfile = 1
    DataDriftProfile = 2
    NumTargetDriftProfile = 3
    CatTargetDriftProfile = 4
    RegressionPerformance = 5
    ClassificationPerformance = 6
    ProbClassificationPerformance = 7

class EvidentlyProfileStep():
  def exec(self,
      profileConfig : ProfileConfig, 
      reference : pandas.core.frame.DataFrame, 
      current : pandas.core.frame.DataFrame,
      json = False,
      save_loc = None) -> Optional[str]:    #train-test split??
    """
      Datadrift requires a reference and a current 
      Data quality either requires reference, or reference and current
      Target drift requires a reference, current, and targets and/or preds
      Regression performance requires reference and/or current, and targets AND preds

      Classification + Probabilistic require pred / targets in a specific config ? 
       - check evidently documentation, also requires inputs (not necessarly just the dataframe - pred)

      Returns a json string with all specified profiles included;
      if there is insufficient information to generate report, we state it in json???
    """
    # first check if columns of reference and current are the same
    evidentlyMap = {
      EvidentlyProfileTypes.DataQualityProfile : [DataQualityTab(), DataQualityProfileSection()],
      EvidentlyProfileTypes.DataDriftProfile : [],
      EvidentlyProfileTypes.NumTargetDriftProfile : [NumTargetDriftTab()],
      EvidentlyProfileTypes.CatTargetDriftProfile : [],
      EvidentlyProfileTypes.RegressionPerformance : [],
      EvidentlyProfileTypes.ClassificationPerformance: [],
      EvidentlyProfileTypes.ProbClassificationPerformance: [],
    }
    # build tabs based on successful input 
    tabs = []
    for profile_type in profileConfig.profiles:
      tabs.append(evidentlyMap[profile_type][int(json is True)]) #if json, gets index 1

    if not json:
      dashboard = Dashboard(tabs=tabs)
      dashboard.calculate(reference, current, column_mapping=profileConfig.column_mapping)
      dashboard.save(save_loc)
    else:
      profile = Profile(sections=tabs)
      profile.calculate(reference, current, column_mapping=profileConfig.column_mapping)
      ret = profile.json()










       



    
