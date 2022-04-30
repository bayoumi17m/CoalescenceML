from __future__ import annotations

import os
import time
import uuid
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Any,
    Dict,
    NoReturn,
    Optional,
    Set,
    Type,
)

from coalescenceml.config.global_config import GlobalConfiguration
from coalescenceml.constants import RUN_NAME_OPTION_KEY
from coalescenceml.enums import StackComponentFlavor
from coalescenceml.io import utils
from coalescenceml.logger import get_logger
from coalescenceml.stack.exceptions import ProvisioningError
from coalescenceml.utils import readability_utils


if TYPE_CHECKING:
    from coalescenceml.artifact_store import BaseArtifactStore
    from coalescenceml.container_registry import BaseContainerRegistry
    from coalescenceml.experiment_tracker import BaseExperimentTracker
    # from coalescenceml.feature_store import BaseFeatureStore
    from coalescenceml.metadata_store import BaseMetadataStore
    # from coalescenceml.model_deployer import BaseModelDeployer
    from coalescenceml.orchestrator import BaseOrchestrator
    from coalescenceml.pipeline import BasePipeline
    from coalescenceml.pipeline.runtime_configuration import (
        RuntimeConfiguration,
    )

    # from coalescenceml.secrets_manager import BaseSecretsManager
    from coalescenceml.stack import StackComponent
    from coalescenceml.step_operator import BaseStepOperator

logger = get_logger(__name__)


class Stack:
    """CoalescenceML stack class.

    A CoalescenceML stack is a collection of multiple stack components that are
    required to run CoalescenceML pipelines. Some of these components
    (orchestrator, metadata store and artifact store) are required to run
    any kind of pipeline, other components like the container registry are
    only required if other stack components depend on them.
    """

    def __init__(
        self,
        name: str,
        *,
        orchestrator: BaseOrchestrator,
        metadata_store: BaseMetadataStore,
        artifact_store: BaseArtifactStore,
        container_registry: Optional[BaseContainerRegistry] = None,
        # secrets_manager: Optional["BaseSecretsManager"] = None,
        step_operator: Optional[BaseStepOperator] = None,
        # feature_store: Optional["BaseFeatureStore"] = None,
        # model_deployer: Optional["BaseModelDeployer"] = None,
        experiment_tracker: Optional[BaseExperimentTracker] = None,
    ):
        """Initialize and validate a stack instance.

        Args:
            name: Name given to this stack
            orchestrator: Orchestrator instance to be used in stack
            metadata_store: Metadata store instance to be used in stack
            artifact_store: Artifact store instance to be used in stack
            container_registry: Container registry instance to be used in stack
            step_operator: Step operator instance to be used in stack
            experiment_tracker: Experiment tracker instance to be used
        """
        self._name = name
        self._orchestrator = orchestrator
        self._metadata_store = metadata_store
        self._artifact_store = artifact_store
        self._container_registry = container_registry
        self._step_operator = step_operator
        self._secrets_manager = None  # secrets_manager
        self._feature_store = None  # feature_store
        self._model_deployer = None  # model_deployer
        self._experiment_tracker = experiment_tracker

        self.validate()

    @classmethod
    def from_components(
        cls, name: str, components: Dict[StackComponentFlavor, StackComponent]
    ) -> "Stack":
        """Create a stack instance from a dict of stack components.

        Args:
            name: The name of the stack.
            components: The components of the stack.

        Returns:
            A stack instance consisting of the given components.
        """
        from coalescenceml.artifact_store import BaseArtifactStore
        from coalescenceml.container_registry import BaseContainerRegistry
        from coalescenceml.experiment_tracker import BaseExperimentTracker
        # from coalescenceml.feature_store import BaseFeatureStore
        from coalescenceml.metadata_store import BaseMetadataStore
        # from coalescenceml.model_deployer import BaseModelDeployer
        from coalescenceml.orchestrator import BaseOrchestrator
        # from coalescenceml.secrets_manager import BaseSecretsManager
        from coalescenceml.step_operator import BaseStepOperator

        def _raise_type_error(
            component: Optional[StackComponent], expected_class: Type[Any]
        ) -> NoReturn:
            """Raise a TypeError that the component has an unexpected type.

            Args:
                component: StackComponent that has the wrong type
                expected_class: Type of StackComponent that was expected

            Raises:
                TypeError: Always!
            """
            raise TypeError(
                f"Unable to create stack: Wrong stack component type "
                f"`{component.__class__.__name__}` (expected: subclass "
                f"of `{expected_class.__name__}`)"
            )

        orchestrator = components.get(StackComponentFlavor.ORCHESTRATOR)
        if not isinstance(orchestrator, BaseOrchestrator):
            _raise_type_error(orchestrator, BaseOrchestrator)

        metadata_store = components.get(StackComponentFlavor.METADATA_STORE)
        if not isinstance(metadata_store, BaseMetadataStore):
            _raise_type_error(metadata_store, BaseMetadataStore)

        artifact_store = components.get(StackComponentFlavor.ARTIFACT_STORE)
        if not isinstance(artifact_store, BaseArtifactStore):
            _raise_type_error(artifact_store, BaseArtifactStore)

        container_registry = components.get(
            StackComponentFlavor.CONTAINER_REGISTRY
        )
        if container_registry is not None and not isinstance(
            container_registry, BaseContainerRegistry
        ):
            _raise_type_error(container_registry, BaseContainerRegistry)

        experiment_tracker = components.get(
            StackComponentFlavor.EXPERIMENT_TRACKER
        )
        if experiment_tracker is not None and not isinstance(
            experiment_tracker, BaseExperimentTracker
        ):
            _raise_type_error(experiment_tracker, BaseExperimentTracker)

        # secrets_manager = components.get(
        #     StackComponentFlavor.SECRETS_MANAGER
        # )
        # if secrets_manager is not None and not isinstance(
        #     secrets_manager, BaseSecretsManager
        # ):
        #     _raise_type_error(secrets_manager, BaseSecretsManager)

        step_operator = components.get(StackComponentFlavor.STEP_OPERATOR)
        if step_operator is not None and not isinstance(
            step_operator, BaseStepOperator
        ):
            _raise_type_error(step_operator, BaseStepOperator)

        # feature_store = components.get(StackComponentFlavor.FEATURE_STORE)
        # if feature_store is not None and not isinstance(
        #     feature_store, BaseFeatureStore
        # ):
        #     _raise_type_error(feature_store, BaseFeatureStore)

        # model_deployer = components.get(StackComponentFlavor.MODEL_DEPLOYER)
        # if model_deployer is not None and not isinstance(
        #     model_deployer, BaseModelDeployer
        # ):
        #     _raise_type_error(model_deployer, BaseModelDeployer)

        return Stack(
            name=name,
            orchestrator=orchestrator,
            metadata_store=metadata_store,
            artifact_store=artifact_store,
            container_registry=container_registry,
            # secrets_manager=secrets_manager,
            step_operator=step_operator,
            # feature_store=feature_store,
            # model_deployer=model_deployer,
            experiment_tracker=experiment_tracker,
        )

    @classmethod
    def default_local_stack(cls) -> Stack:
        """Create a stack instance which is configured to run locally.

        Returns:
            Default setup for a local stack
        """
        from coalescenceml.artifact_store import LocalArtifactStore
        from coalescenceml.metadata_store import SQLiteMetadataStore
        from coalescenceml.orchestrator import LocalOrchestrator

        orchestrator = LocalOrchestrator(name="default")

        artifact_store_uuid = uuid.uuid4()
        artifact_store_path = os.path.join(
            GlobalConfiguration().config_directory,
            "local_stores",
            str(artifact_store_uuid),
        )
        utils.create_dir_recursive_if_not_exists(artifact_store_path)
        artifact_store = LocalArtifactStore(
            name="default",
            uuid=artifact_store_uuid,
            path=artifact_store_path,
        )

        metadata_store_path = os.path.join(artifact_store_path, "metadata.db")
        metadata_store = SQLiteMetadataStore(
            name="default", uri=metadata_store_path
        )

        return cls(
            name="default",
            orchestrator=orchestrator,
            metadata_store=metadata_store,
            artifact_store=artifact_store,
        )

    @property
    def components(self) -> Dict[StackComponentFlavor, StackComponent]:
        """Fetch all components of the stack.

        Returns:
            All set components of stack
        """
        return {
            component.TYPE: component
            for component in [
                self._orchestrator,
                self._metadata_store,
                self._artifact_store,
                self._container_registry,
                self._secrets_manager,
                self._step_operator,
                self._feature_store,
                self._model_deployer,
                self._experiment_tracker,
            ]
            if component is not None
        }

    @property
    def name(self) -> str:
        """Fetch name of the stack.

        Returns:
            name within stack
        """
        return self._name

    @property
    def orchestrator(self) -> "BaseOrchestrator":
        """Fetch orchestrator of the stack.

        Returns:
            orchestrator within stack
        """
        return self._orchestrator

    @property
    def metadata_store(self) -> "BaseMetadataStore":
        """Fetch metadata store of the stack.

        Returns:
            metadata store within stack
        """
        return self._metadata_store

    @property
    def artifact_store(self) -> "BaseArtifactStore":
        """Fetch artifact store of the stack.

        Returns:
            artifact store within stack
        """
        return self._artifact_store

    @property
    def container_registry(self) -> Optional["BaseContainerRegistry"]:
        """Fetch container registry of the stack.

        Returns:
            container registry within stack
        """
        return self._container_registry

    # @property
    # def secrets_manager(self) -> Optional["BaseSecretsManager"]:
    #     """The secrets manager of the stack."""
    #     return self._secrets_manager

    @property
    def step_operator(self) -> Optional["BaseStepOperator"]:
        """Fetch step operator of the stack.

        Returns:
            step operator within stack
        """
        return self._step_operator

    # @property
    # def feature_store(self) -> Optional["BaseFeatureStore"]:
    #     """The feature store of the stack."""
    #     return self._feature_store

    # @property
    # def model_deployer(self) -> Optional["BaseModelDeployer"]:
    #     """The model deployer of the stack."""
    #     return self._model_deployer

    @property
    def experiment_tracker(self) -> Optional[BaseExperimentTracker]:
        """Fetch experiment tracker of the stack.

        Returns:
            experiment tracker within the stack
        """
        return self._experiment_tracker

    @property
    def runtime_options(self) -> Dict[str, Any]:
        """Runtime options that are available to configure this stack.

        This method combines the available runtime options for all components
        of this stack. See `StackComponent.runtime_options()` for
        more information.

        Returns:
            All configurable runtime options
        """
        runtime_options: Dict[str, Any] = {}
        for component in self.components.values():
            duplicate_runtime_options = (
                runtime_options.keys() & component.runtime_options.keys()
            )
            if duplicate_runtime_options:
                logger.warning(
                    "Found duplicate runtime options %s.",
                    duplicate_runtime_options,
                )

            runtime_options.update(component.runtime_options)

        return runtime_options

    def dict(self) -> Dict[str, Any]:
        """Convert the stack into a dictionary.

        Returns:
            Stack in dictionary form
        """
        component_dict = {
            component_type.value: component.json(sort_keys=True)
            for component_type, component in self.components.items()
        }
        component_dict.update({"name": self.name})
        return component_dict

    def requirements(
        self,
        exclude_components: Optional[AbstractSet[StackComponentFlavor]] = None,
    ) -> Set[str]:
        """Collect set of PyPI requirements for the stack.

        This method combines the requirements of all stack components (except
        the ones specified in `exclude_components`).

        Args:
            exclude_components: Set of component types for which the
                requirements should not be included in the output.

        Returns:
            Set of PyPI requirments for stack
        """
        exclude_components = exclude_components or set()
        requirements = [
            component.requirements
            for component in self.components.values()
            if component.TYPE not in exclude_components
        ]
        return set.union(*requirements) if requirements else set()

    def validate(self) -> None:
        """Check whether the stack configuration is valid.

        To check if a stack configuration is valid, the following criteria must
        be met:
        - all components must support the execution mode (either local or
         remote execution) specified by the orchestrator of the stack
        - the `StackValidator` of each stack component has to validate the
         stack to make sure all the components are compatible with each other
        """
        for component in self.components.values():
            if component.validator:
                component.validator.validate(stack=self)

    def deploy_pipeline(
        self,
        pipeline: BasePipeline,
        runtime_configuration: RuntimeConfiguration,
    ) -> Any:
        """Deploy a pipeline on this stack.

        Args:
            pipeline: The pipeline to deploy.
            runtime_configuration: Contains all the runtime configuration
                options specified for the pipeline run.

        Returns:
            The return value of the call to `orchestrator.run_pipeline(...)`.
        """
        for component in self.components.values():
            component.prepare_pipeline_deployment(
                pipeline=pipeline,
                stack=self,
                runtime_configuration=runtime_configuration,
            )

        for component in self.components.values():
            component.prepare_pipeline_run()

        runtime_configuration[
            RUN_NAME_OPTION_KEY
        ] = runtime_configuration.run_name or (
            f"{pipeline.name}-"
            f'{datetime.now().strftime("%d_%h_%y-%H_%M_%S_%f")}'
        )

        logger.info(
            "Using stack `%s` to run pipeline `%s`...",
            self.name,
            pipeline.name,
        )
        start_time = time.time()

        original_cache_boolean = pipeline.enable_cache
        if "enable_cache" in runtime_configuration:
            logger.info(
                "Runtime configuration overwriting the pipeline cache settings"
                " to enable_cache=`%s` for this pipeline run. The default "
                "caching strategy is retained for future pipeline runs.",
                runtime_configuration["enable_cache"],
            )
            pipeline.enable_cache = runtime_configuration.get("enable_cache")

        return_value = self._orchestrator.run_pipeline(
            pipeline, stack=self, runtime_configuration=runtime_configuration
        )

        # Put pipeline level cache policy back to make sure the next runs
        #  default to that policy again in case the runtime configuration
        #  is not set explicitly
        pipeline.enable_cache = original_cache_boolean

        run_duration = time.time() - start_time
        logger.info(
            "Pipeline run `%s` has finished in %s.",
            runtime_configuration.run_name,
            readability_utils.get_human_readable_time(run_duration),
        )

        for component in self.components.values():
            component.cleanup_pipeline_run()

        return return_value

    def prepare_step_run(self) -> None:
        """Prepare running a step."""
        for component in self.components.values():
            component.prepare_step_run()

    def cleanup_step_run(self) -> None:
        """Cleanup resources after step is run."""
        for component in self.components.values():
            component.cleanup_step_run()

    @property
    def is_provisioned(self) -> bool:
        """If the stack provisioned resources to run locally.

        Returns:
            True if stack is provisioned to run locally
        """
        return all(
            component.is_provisioned for component in self.components.values()
        )

    @property
    def is_running(self) -> bool:
        """If the stack is running locally.

        Returns:
            Whether stack is running
        """
        return all(
            component.is_running for component in self.components.values()
        )

    def provision(self) -> None:
        """Provisions resources to run the stack locally."""
        logger.info("Provisioning resources for stack '%s'.", self.name)
        for component in self.components.values():
            if not component.is_provisioned:
                component.provision()
                logger.info("Provisioned resources for %s.", component)

    def deprovision(self) -> None:
        """Deprovisions all local resources of the stack."""
        logger.info("Deprovisioning resources for stack '%s'.", self.name)
        for component in self.components.values():
            if component.is_provisioned:
                try:
                    component.deprovision()
                    logger.info("Deprovisioned resources for %s.", component)
                except NotImplementedError as e:
                    logger.warning(e)

    def resume(self) -> None:
        """Resume the provisioned local resources of the stack.

        Raises:
            ProvisioningError: If any stack component is missing provisioned
                resources.
        """
        logger.info("Resuming provisioned resources for stack %s.", self.name)
        for component in self.components.values():
            if component.is_running:
                # the component is already running, no need to resume anything
                pass
            elif component.is_provisioned:
                component.resume()
                logger.info("Resumed resources for %s.", component)
            else:
                raise ProvisioningError(
                    f"Unable to resume resources for {component}: No "
                    f"resources have been provisioned for this component."
                )

    def suspend(self) -> None:
        """Suspends the provisioned local resources of the stack."""
        logger.info(
            "Suspending provisioned resources for stack '%s'.", self.name
        )
        for component in self.components.values():
            if component.is_running:
                try:
                    component.suspend()
                    logger.info("Suspended resources for %s.", component)
                except NotImplementedError:
                    logger.warning(
                        "Suspending provisioned resources not implemented "
                        "for %s. Continuing without suspending resources...",
                        component,
                    )
