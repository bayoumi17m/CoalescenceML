import datetime as dt
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import ArgumentError, NoResultFound
from sqlmodel import Field, Session, SQLModel, create_engine, select

from coalescenceml.enums import StackComponentFlavor, DirectoryStoreFlavor
from coalescenceml.io import utils
from coalescenceml.logger import get_logger
from coalescenceml.stack.exceptions import StackComponentExistsError
from coalescenceml.stack_store import BaseStackStore
from coalescenceml.stack_store.model import StackComponentWrapper

logger = get_logger(__name__)


class CoalescenceUser(SQLModel, table=True):
    id: int = Field(primary_key=True)
    name: str


class CoalescenceStack(SQLModel, table=True):
    name: str = Field(primary_key=True)
    created_by: int
    create_time: Optional[dt.datetime] = Field(default_factory=dt.datetime.now)


class CoalescenceStackComponent(SQLModel, table=True):
    component_flavor: StackComponentFlavor = Field(primary_key=True)
    name: str = Field(primary_key=True)
    component_flavor: str
    configuration: bytes  # e.g. base64 encoded json string


class CoalescenceStackDefinition(SQLModel, table=True):
    """Join table between Stacks and StackComponents"""

    stack_name: str = Field(primary_key=True, foreign_key="coalescencestack.name")
    component_flavor: StackComponentFlavor = Field(
        primary_key=True, foreign_key="coalescencestackcomponent.component_flavor"
    )
    component_name: str = Field(
        primary_key=True, foreign_key="coalescencestackcomponent.name"
    )


class SqlStackStore(BaseStackStore):
    """Repository Implementation that uses SQL database backend"""

    def initialize(
        self,
        url: str,
        *args: Any,
        **kwargs: Any,
    ) -> "SqlStackStore":
        """Initialize a new SqlStackStore.

        Args:
            url: odbc path to a database.
            args, kwargs: additional parameters for SQLModel.
        Returns:
            The initialized stack store instance.
        """
        if not self.is_valid_url(url):
            raise ValueError(f"Invalid URL for SQL store: {url}")

        logger.debug("Initializing SqlStackStore at %s", url)
        self._url = url

        local_path = self.get_path_from_url(url)
        if local_path:
            utils.create_dir_recursive_if_not_exists(str(local_path.parent))

        self.engine = create_engine(url, *args, **kwargs)
        SQLModel.metadata.create_all(self.engine)
        with Session(self.engine) as session:
            if not session.exec(select(CoalescenceUser)).first():
                session.add(CoalescenceUser(id=1, name="LocalCoalescenceUser"))
            session.commit()

        super().initialize(url, *args, **kwargs)
        return self

    # Public interface implementations:

    @property
    def type(self) -> DirectoryStoreFlavor:
        """The type of stack store."""
        return DirectoryStoreFlavor.SQL

    @property
    def url(self) -> str:
        """URL of the repository."""
        if not self._url:
            raise RuntimeError(
                "SQL stack store has not been initialized. Call `initialize` "
                "before using the store."
            )
        return self._url

    @staticmethod
    def get_path_from_url(url: str) -> Optional[Path]:
        """Get the local path from a URL, if it points to a local sqlite file.

        This method first checks that the URL is a valid SQLite URL, which is
        backed by a file in the local filesystem. All other types of supported
        SQLAlchemy connection URLs are considered non-local and won't return
        a valid local path.

        Args:
            url: The URL to get the path from.

        Returns:
            The path extracted from the URL, or None, if the URL does not
            point to a local sqlite file.
        """
        if not SqlStackStore.is_valid_url(url):
            raise ValueError(f"Invalid URL for SQL store: {url}")
        if not url.startswith("sqlite:///"):
            return None
        url = url.replace("sqlite:///", "")
        return Path(url)

    @staticmethod
    def get_local_url(path: str) -> str:
        """Get a local SQL url for a given local path."""
        return f"sqlite:///{path}/coalescenceml.db"

    @staticmethod
    def is_valid_url(url: str) -> bool:
        """Check if the given url is a valid SQL url."""
        try:
            make_url(url)
        except ArgumentError:
            logger.debug("Invalid SQL URL: %s", url)
            return False

        return True

    @property
    def is_empty(self) -> bool:
        """Check if the stack store is empty."""
        with Session(self.engine) as session:
            return session.exec(select(CoalescenceStack)).first() is None

    def get_stack_configuration(
        self, name: str
    ) -> Dict[StackComponentFlavor, str]:
        """Fetches a stack configuration by name.

        Args:
            name: The name of the stack to fetch.

        Returns:
            Dict[StackComponentFlavor, str] for the requested stack name.

        Raises:
            KeyError: If no stack exists for the given name.
        """
        logger.debug("Fetching stack with name '%s'.", name)
        # first check that the stack exists
        with Session(self.engine) as session:
            maybe_stack = session.exec(
                select(CoalescenceStack).where(CoalescenceStack.name == name)
            ).first()
        if maybe_stack is None:
            raise KeyError(
                f"Unable to find stack with name '{name}'. Available names: "
                f"{set(self.stack_names)}."
            )
        # then get all components assigned to that stack
        with Session(self.engine) as session:
            definitions_and_components = session.exec(
                select(CoalescenceStackDefinition, CoalescenceStackComponent)
                .where(
                    CoalescenceStackDefinition.component_flavor
                    == CoalescenceStackComponent.component_flavor
                )
                .where(
                    CoalescenceStackDefinition.component_name == CoalescenceStackComponent.name
                )
                .where(CoalescenceStackDefinition.stack_name == name)
            )
            params = {
                component.component_flavor: component.name
                for _, component in definitions_and_components
            }
        return {StackComponentFlavor(typ): name for typ, name in params.items()}

    @property
    def stack_configurations(self) -> Dict[str, Dict[StackComponentFlavor, str]]:
        """Configuration for all stacks registered in this stack store.

        Returns:
            Dictionary mapping stack names to Dict[StackComponentFlavor, str]
        """
        return {n: self.get_stack_configuration(n) for n in self.stack_names}

    def register_stack_component(
        self,
        component: StackComponentWrapper,
    ) -> None:
        """Register a stack component.

        Args:
            component: The component to register.

        Raises:
            StackComponentExistsError: If a stack component with the same type
                and name already exists.
        """
        with Session(self.engine) as session:
            existing_component = session.exec(
                select(CoalescenceStackComponent)
                .where(CoalescenceStackComponent.name == component.name)
                .where(CoalescenceStackComponent.component_flavor == component.type)
            ).first()
            if existing_component is not None:
                raise StackComponentExistsError(
                    f"Unable to register stack component (type: "
                    f"{component.type}) with name '{component.name}': Found "
                    f"existing stack component with this name."
                )
            new_component = CoalescenceStackComponent(
                component_type=component.type,
                name=component.name,
                component_flavor=component.flavor,
                configuration=component.config,
            )
            session.add(new_component)
            session.commit()

    def deregister_stack(self, name: str) -> None:
        """Delete a stack from storage.

        Args:
            name: The name of the stack to be deleted.

        Raises:
            KeyError: If no stack exists for the given name.
        """
        with Session(self.engine) as session:
            try:
                stack = session.exec(
                    select(CoalescenceStack).where(CoalescenceStack.name == name)
                ).one()
                session.delete(stack)
            except NoResultFound as error:
                raise KeyError from error
            definitions = session.exec(
                select(CoalescenceStackDefinition).where(
                    CoalescenceStackDefinition.stack_name == name
                )
            ).all()
            for definition in definitions:
                session.delete(definition)
            session.commit()

    # Private interface implementations:

    def _create_stack(
        self, name: str, stack_configuration: Dict[StackComponentFlavor, str]
    ) -> None:
        """Add a stack to storage.

        Args:
            name: The name to save the stack as.
            stack_configuration: Dict[StackComponentFlavor, str] to persist.
        """
        with Session(self.engine) as session:
            stack = CoalescenceStack(name=name, created_by=1)
            session.add(stack)
            for ctype, cname in stack_configuration.items():
                if cname is not None:
                    session.add(
                        CoalescenceStackDefinition(
                            stack_name=name,
                            component_flavor=ctype,
                            component_name=cname,
                        )
                    )
            session.commit()

    def _get_component_flavor_and_config(
        self, component_flavor: StackComponentFlavor, name: str
    ) -> Tuple[str, bytes]:
        """Fetch the flavor and configuration for a stack component.

        Args:
            component_flavor: The type of the component to fetch.
            name: The name of the component to fetch.

        Returns:
            Pair of (flavor, configuration) for stack component, as string and
            base64-encoded yaml document, respectively

        Raises:
            KeyError: If no stack component exists for the given type and name.
        """
        with Session(self.engine) as session:
            component = session.exec(
                select(CoalescenceStackComponent)
                .where(CoalescenceStackComponent.component_flavor == component_flavor)
                .where(CoalescenceStackComponent.name == name)
            ).one_or_none()
            if component is None:
                raise KeyError(
                    f"Unable to find stack component (type: {component_flavor}) "
                    f"with name '{name}'."
                )
        return component.component_flavor, component.configuration

    def _get_stack_component_names(
        self, component_flavor: StackComponentFlavor
    ) -> List[str]:
        """Get names of all registered stack components of a given type.

        Args:
            component_flavor: The type of the component to list names for.

        Returns:
            A list of names as strings.
        """
        with Session(self.engine) as session:
            statement = select(CoalescenceStackComponent).where(
                CoalescenceStackComponent.component_flavor == component_flavor
            )
            return [component.name for component in session.exec(statement)]

    def _delete_stack_component(
        self, component_flavor: StackComponentFlavor, name: str
    ) -> None:
        """Remove a StackComponent from storage.

        Args:
            component_flavor: The type of component to delete.
            name: Then name of the component to delete.

        Raises:
            KeyError: If no component exists for given type and name.
        """
        with Session(self.engine) as session:
            component = session.exec(
                select(CoalescenceStackComponent)
                .where(CoalescenceStackComponent.component_flavor == component_flavor)
                .where(CoalescenceStackComponent.name == name)
            ).first()
            if component is not None:
                session.delete(component)
                session.commit()
            else:
                raise KeyError(
                    "Unable to deregister stack component (type: "
                    f"{component_flavor.value}) with name '{name}': No stack "
                    "component exists with this name."
                )

    # Implementation-specific internal methods:

    @property
    def stack_names(self) -> List[str]:
        """Names of all stacks registered in this StackStore."""
        with Session(self.engine) as session:
            return [s.name for s in session.exec(select(CoalescenceStack))]