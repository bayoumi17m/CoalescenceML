"""
## Orchestrator

An orchestrator is a special kind of backend that manages the running of each
step of the pipeline. Orchestrators administer the actual pipeline runs.
"""
from coalescenceml.orchestrator.base_orchestrator import BaseOrchestrator
from coalescenceml.orchestrator.local_orchestrator import LocalOrchestrator


__all__ = ["BaseOrchestrator", "LocalOrchestrator"]
