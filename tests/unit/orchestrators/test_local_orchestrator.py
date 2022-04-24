from coalescenceml.enums import StackComponentFlavor
from coalescenceml.orchestrator import LocalOrchestrator


def test_local_orchestrator_attributes():
    """Tests that the basic attributes of the local orchestrator are set
    correctly."""
    orchestrator = LocalOrchestrator(name="")
    assert orchestrator.TYPE == StackComponentFlavor.ORCHESTRATOR
    assert orchestrator.FLAVOR == "local"