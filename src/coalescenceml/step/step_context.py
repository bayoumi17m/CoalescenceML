from __future__ import annotations
from typing import TYPE_CHECKING, Dict, NamedTuple, Optional, Type, cast

from coalescenceml.step.exceptions import StepContextError
from coalescenceml.directory import Directory


if TYPE_CHECKING:
    from coalescenceml.artifacts.base_artifact import BaseArtifact
    from coalescenceml.producers.base_producer import BaseProducer
    from coalescenceml.metadata_store.base_metadata_store import BaseMetadataStore
    from coalescenceml.stack import Stack


class StepContextOutput(NamedTuple):
    """"""
    producer_class: Type[BaseProducer]
    artifact: BaseArtifact


class StepContext(object):
    """"""

    def __init__(self,
        step_name: str,
        output_producers: Dict[str, Type[BaseProducer]],
        output_artifacts: Dict[str, BaseArtifact],
    ):
        """
        """
        if output_producers.keys() != output_artifacts.keys():
            raise ValueError(f"Mismatched keys")

        self.step_name = step_name
        self._outputs = {
            key: StepContextOutput(
                output_producer[key], output_artifact[key],
            )
            for key in output_artifacts.keys()
        }
        self._metadata_store = Directory().active_stack.metadata_store
        self._stack = Directory().active_stack
    

    def _get_output(
        self, output_name: Optional[str] = None
    ) -> StepContextOutput:
        """Returns the producer and artifact URI for a given step output.

        Args:
            output_name: Optional name of the output for which to get the
                producer and URI.

        Returns:
            Tuple containing the producer and artifact URI for the
            given output.

        Raises:
            StepContextError: If the step has no outputs, no output for
                              the given `output_name` or if no `output_name`
                              was given but the step has multiple outputs.
        """
        output_count = len(self._outputs)
        if output_count == 0:
            raise StepContextError(
                f"Unable to get step output for step '{self.step_name}': "
                f"This step does not have any outputs."
            )

        if not output_name and output_count > 1:
            raise StepContextError(
                f"Unable to get step output for step '{self.step_name}': "
                f"This step has multiple outputs ({set(self._outputs)}), "
                f"please specify which output to return."
            )

        if output_name:
            if output_name not in self._outputs:
                raise StepContextError(
                    f"Unable to get step output '{output_name}' for "
                    f"step '{self.step_name}'. This step does not have an "
                    f"output with the given name, please specify one of the "
                    f"available outputs: {set(self._outputs)}."
                )
            return self._outputs[output_name]
        else:
            return next(iter(self._outputs.values()))

    @property
    def metadata_store(self) -> BaseMetadataStore:
        """
        Returns an instance of the metadata store that is used to store
        metadata about the step (and the corresponding pipeline) which is
        being executed.
        """
        return self._metadata_store

    @property
    def stack(self) -> Optional["Stack"]:
        """Returns the current active stack."""
        return self._stack

    def get_output_producer(
        self,
        output_name: Optional[str] = None,
        custom_producer_class: Optional[Type[Baseproducer]] = None,
    ) -> Baseproducer:
        """Returns a producer for a given step output.

        Args:
            output_name: Optional name of the output for which to get the
                producer. If no name is given and the step only has a
                single output, the producer of this output will be
                returned. If the step has multiple outputs, an exception
                will be raised.
            custom_producer_class: If given, this `Baseproducer`
                subclass will be initialized with the output artifact instead
                of the producer that was registered for this step output.

        Returns:
            A producer initialized with the output artifact for
            the given output.

        Raises:
            StepContextError: If the step has no outputs, no output for
                              the given `output_name` or if no `output_name`
                              was given but the step has multiple outputs.
        """
        producer_class, artifact = self._get_output(output_name)
        # use custom producer class if provided or fallback to default
        # producer for output
        producer_class = custom_producer_class or producer_class
        return producer_class(artifact)

    def get_output_artifact_uri(self, output_name: Optional[str] = None) -> str:
        """Returns the artifact URI for a given step output.

        Args:
            output_name: Optional name of the output for which to get the URI.
                If no name is given and the step only has a single output,
                the URI of this output will be returned. If the step has
                multiple outputs, an exception will be raised.

        Returns:
            Artifact URI for the given output.

        Raises:
            StepContextError: If the step has no outputs, no output for
                              the given `output_name` or if no `output_name`
                              was given but the step has multiple outputs.
        """
        return cast(str, self._get_output(output_name).artifact.uri)
