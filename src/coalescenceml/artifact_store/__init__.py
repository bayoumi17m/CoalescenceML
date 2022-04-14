"""
## Artifact Stores

the inputs and outputs which go through any step is treated as an
artifact and as its name suggests, an `ArtifactStore` is a place where these
artifacts get stored.
"""
from coalescenceml.artifact_store.base_artifact_store import BaseArtifactStore
from coalescenceml.artifact_store.local_artifact_store import LocalArtifactStore

__all__ = ["BaseArtifactStore", "LocalArtifactStore"]
