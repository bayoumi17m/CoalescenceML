import importlib
import inspect
import sys
from typing import List, Optional, Type, cast

from coalescenceml.integrations.integration import Integration, IntegrationMeta


def get_integration_for_module(module_name: str) -> Optional[Type[Integration]]:
    """Gets the integration class for a module inside an integration.
    If the module given by `module_name` is not part of a CoalescenceML integration,
    this method will return `None`. If it is part of a CoalescenceML integration,
    it will return the integration class found inside the integration
    __init__ file.
    """
    integration_prefix = "coalescenceml.integrations."
    if not module_name.startswith(integration_prefix):
        return None

    integration_module_name = ".".join(module_name.split(".", 3)[:3])
    try:
        integration_module = sys.modules[integration_module_name]
    except KeyError:
        integration_module = importlib.import_module(integration_module_name)

    for name, member in inspect.getmembers(integration_module):
        if (
            member is not Integration
            and isinstance(member, IntegrationMeta)
            and issubclass(member, Integration)
        ):
            return cast(Type[Integration], member)

    return None


def get_requirements_for_module(module_name: str) -> List[str]:
    """Gets requirements for a module inside an integration.
    If the module given by `module_name` is not part of a CoalescenceML integration,
    this method will return an empty list. If it is part of a CoalescenceML integration,
    it will return the list of requirements specified inside the integration
    class found inside the integration __init__ file.
    """
    integration = get_integration_for_module(module_name)
    return integration.REQUIREMENTS if integration else []