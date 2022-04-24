import time

import click

from coalescenceml.cli.cli import cli
import coalescenceml.cli.utils as cli_utils
from coalescenceml.constants import console
from coalescenceml.logger import get_logger


@cli.command(name="over", help="Funny joke...hehe", hidden=True)
def print_deez_nutz_joke()->None:
    deez_nutz = r"""
      -                                                                                     -
    _/-\_                                                                                 _/-\_
 .-`-:-:-`-.   _____    ______   ______   ______    _   _   _    _   _______   ______  .-`-:-:-`-.
/-:-:-:-:-:-\ |  __ \  |  ____| |  ____| |___  /   | \ | | | |  | | |__   __| |___  / /-:-:-:-:-:-\
\:-:-:-:-:-:/ | |  | | | |__    | |__       / /    |  \| | | |  | |    | |       / /  \:-:-:-:-:-:/
 |`       `|  | |  | | |  __|   |  __|     / /     | . ` | | |  | |    | |      / /    |`       `|
 |         |  | |__| | | |____  | |____   / /__    | |\  | | |__| |    | |     / /__   |         |
 `\       /`  |_____/  |______| |______| /_____|   |_| \_|  \____/     |_|    /_____|  `\       /`
   `-._.-'                                                                               `-._.-'"""

    max_width = console.width
    deez_nutz_width = console.measure(deez_nutz).maximum
    styling = "bold cyan"
    console.print("coml over...", style="cyan", justify="left")
    with console.status("", spinner="monkey"):
        time.sleep(5)

    if deez_nutz_width >= max_width:
        console.print(deez_nutz, style=styling, overflow="crop")
        console.rule(f"")
    else:
        width_buffer = int((max_width - deez_nutz_width) / (2))
        deez_nutz = "\n".join([
            ((" "* width_buffer) + line) for line in deez_nutz.split("\n")
        ])
        console.print(deez_nutz, style=styling)
        console.rule("")
