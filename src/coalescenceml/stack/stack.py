from __future__ import annotations
import os
import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Dict, List, Optional


from coalescenceml.config.global_config import GlobalConfiguration
from coalescenceml.constants import RUN_NAME_OPTION_KEY
from coalescenceml.enums import StackComponentFlavor
from coalescenceml.stack.stack_component import StackComponent
from coalescenceml.io import utils


if TYPE_CHECKING:
    from coalescenceml.pipeline.base_pipeline import BasePipeline
    from coalescenceml.pipeline.runtime_configuration import RuntimeConfiguration
    from coalescenceml.orchestrator.base_orchestrator import BaseOrchestrator
    from coalescenceml.metadata_store.base_metadata_store import BaseMetadataStore
    from coalescenceml.artifact_store.base_artifact_store import BaseArtifactStore
    from coalescenceml.container_registry.base_container_registry import BaseContainerRegistry
    from coalescenceml.step_operator.base_step_operator import BaseStepOperator


class Stack:
    """
    """

    def __init__(
        self,
        name: str,
        *,
        orchestrator: BaseOrchestrator,
        metadata_store: BaseMetadataStore,
        artifact_store: BaseArtifactStore,
        container_registry: Optional[BaseContainerRegistry] = None,
        step_operator: Optional[BaseStepOperator] = None,
        # feature_store: ,
        # secrets_manager: ,
        # model_deployer: ,
    ):
        """
        """
        self._name = name
        self._orchestrator = orchestrator
        self._metadata_store = metadata_store
        self._artifact_store = artifact_store
        self._container_registry = container_registry
        self._step_operator = step_operator

        self.validate()
    

    @property
    def name(self) -> str:
        return self._name
    
    @property
    def orchestrator(self) -> BaseOrchestrator:
        return self._orchestrator

    @property
    def metadata_store(self) -> BaseMetadataStore:
        return self._metadata_store
    
    @property
    def artifact_store(self) -> BaseArtifactStore:
        return self._artifact_store
    
    @property
    def container_registry(self) -> BaseContainerRegistry:
        return self._container_registry
    
    @property
    def step_operator(self) -> BaseStepOperator:
        return self._step_operator
    
    @property
    def components(self) -> Dict[StackComponentFlavor, StackComponent]:
        return {
            component.TYPE: component
            for component in [
                self._orchestrator,
                self._artifact_store,
                self._metadata_store,
                self._container_registry,
                self._step_operator,
            ]
            if component is not None
        }
    
    @classmethod
    def from_components(cls, name, components):
        return cls(
            name,
            **components
        )
    
    
    @classmethod
    def default_local_stack(cls) -> Stack:
        from coalescenceml.artifact_store.local_artifact_store import LocalArtifactStore
        from coalescenceml.metadata_store.sqlite_metadata_store import SQLiteMetadataStore
        from coalescenceml.orchestrator.local_orchestrator import LocalOrchestrator

        artifact_uuid = uuid.uuid4()
        artifact_uri_path = os.path.join(GlobalConfiguration().config_directory, "local_artifacts")

        utils.create_dir_recursive_if_not_exists(artifact_uri_path)

        artifact_store = LocalArtifactStore(name="default", path=artifact_uri_path, uuid=artifact_uuid)

        metadata_db_path = os.path.join(artifact_uri_path, "metadata.db")
        metadata_store = SQLiteMetadataStore(name="default", uri=metadata_db_path)
        
        orchestrator = LocalOrchestrator(name="defualt")

        return cls(
            name="default",
            orchestrator = orchestrator,
            metadata_store = metadata_store,
            artifact_store = artifact_store,
        )
    
    def dict(self) -> Dict[StackComponentFlavor, StackComponent]:
        return {
            component.TYPE: component.json()
            for component in [
                self._orchestrator,
                self._artifact_store,
                self._metadata_store,
                self._container_registry,
                self._step_operator,
            ]
            if component is not None
        }
    
    def validate(self):
        """
        """
        for component in self.components.values():
            validator = component.validator
            if validator:
                validator.validate(stack=self)
    
    def deploy_pipeline(
        self,
        pipeline: BasePipeline,
        runtime_configuration: RuntimeConfiguration
    ) -> None:
        """
        """
        for component in self.components.values():
            component.prepare_pipeline_deployment(pipeline=pipeline, stack=self, runtime_configuration=runtime_configuration)
        
        for component in self.components.values():
            component.prepare_pipeline_run()

        # TODO: CHECK FOR CACHE STATUS AND CORRECT AS NEEDED

        runtime_configuration[RUN_NAME_OPTION_KEY] = "pipeline-run-" + datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S")

        self._orchestrator.run_pipeline(
            pipeline=pipeline,
            stack=self,
            runtime_configuration=runtime_configuration
        )

        for component in self.components.values():
            component.cleanup_pipeline_run()
        

