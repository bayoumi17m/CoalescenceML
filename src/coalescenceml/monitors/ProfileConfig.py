import enum


class TaskType(enum.Enum):
  Regression = 1
  Classification = 2

class ProfileConfig():
  def __init__(self, 
    task : TaskType,
    target="target", 
    prediction="prediction", 
    datetime="datetime", 
    id="id",
    profiles = [],
    numerical_features=[], 
    categorical_features=[]) -> None:
      self.target = target
      self.prediction = prediction
      self.datetime = datetime
      self.id = id
      self.numerical_features = numerical_features
      self.categorical_features = categorical_features

      self.profiles = profiles
      self.task = task

      





