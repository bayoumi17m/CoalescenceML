import base64
import json
from uuid import UUID

import yaml
from pydantic import BaseModel

from coalescenceml.enums import StackComponentType
from coalescenceml.stack import StackComponent


class StackComponentWrapper(BaseModel):
    """Serializable Configuration of a StackComponent"""

    type: StackComponentType
    flavor: str  # due to subclassing, can't properly use enum type here
    name: str
    uuid: UUID
    config: bytes  # b64 encoded yaml config

    @classmethod
    def from_component(
        cls, component: StackComponent
    ) -> "StackComponentWrapper":
        return cls(
            type=component.TYPE,
            flavor=component.FLAVOR,
            name=component.name,
            uuid=component.uuid,
            config=base64.b64encode(
                yaml.dump(json.loads(component.json())).encode()
            ),
        )