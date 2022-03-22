from abc import ABC, abstractmethod
from modelobject import ModelObject
from typing import Optional

# inherit base profile step?

class BaseDeploymentStep(ABC):
    """Base class for a deployment step in a CoML pipeline. """

    @abstractmethod
    def entry_point(self, model_uri : str, deploy : bool) -> Optional[str]:
        pass
