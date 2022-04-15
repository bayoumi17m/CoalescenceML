from pathlib import Path
from shutil import rmtree
from typing import Optional

import click

from coalescenceml.cli.cli import cli
from coalescenceml.cli.utils import confirmation, info, error
from coalescenceml.config.global_config import GlobalConfiguration
from coalescenceml.constants import console, DIRECTORY_DIRECTORY_NAME
from coalescenceml.exceptions import InitializationException
from coalescenceml.directory import Directory

@cli.command("init", help="Initialize a CoalescenceML directory.")
@click.option(
    "--path",
    type=click.Path(
        exists=True, file_okay=False, dir_okay=True, path_type=Path
    ),
)
def init(path: Optional[Path]) -> None:
    """Initialize CoalescenceML on given path.
    Args:
      path: Path to the directory.
    Raises:
        InitializationException: If the repo is already initialized.
    """
    if path is None:
        path = Path.cwd()

    with console.status(f"Initializing CoalescenceML directory at {path}.\n"):
        try:
            Directory.initialize(root=path)
            info(f"CoalescenceML directory initialized at {path}.")
        except InitializationException as e:
            error(f"{e}")

    cfg = GlobalConfiguration()
    info(
        f"The local active profile was initialized to "
        f"'{cfg.active_profile_name}' and the local active stack to "
        f"'{cfg.active_stack_name}'. This local configuration will only take "
        f"effect when you're running CoalescenceML from the initialized directory "
        f"root, or from a subdirectory."
    )

# TODO: Make this actually clean the stuff LMAO.
@cli.command("clean")
@click.option("--yes", "-y", type=click.BOOL, default=False)
def clean(yes: bool = False) -> None:
    """Clean everything in directory.
    
    Args:
      yes: bool:  (Default value = False)
    """
    if not yes:
        yes = confirmation(
            "This will completely delete all pipelines, their associated "
            "artifacts and metadata ever created in this CoalescenceML directory."
            "Are you sure you want to proceed?"
        )
    
    if yes:
        dir_path = Path(Directory().get_active_root()) / DIRECTORY_DIRECTORY_NAME
        rmtree(dir_path)
    else:
        info("Skipping deletion of CoalescenceML directory...")
