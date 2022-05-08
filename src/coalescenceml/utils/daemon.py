# https://www.jejik.com/articles/2007/02/a_simple_unix_linux_daemon_in_python/
from asyncio.log import logger
import atexit
from cmath import pi
from concurrent.futures import process
import imp
import os
import signal
import sys
import types
from typing import Any, Callable, Optional, TypeVar, cast
from cv2 import log

import psutil

from coalescenceml.logger import get_logger

logger = get_logger(__name__)


F = TypeVar("F", bound=Callable(..., Any))

def daemonize(
    pid_file: str,
    log_file: Optional[str] = None,
    working_directory: str = "/"
) -> Callable[[F], F]:
    """"""
    def inner_decorator(func: F) -> F:
        def daemon(*args: Any, **kwargs: Any) -> None:
            if sys.platform == "win32":
                logger.error(
                    "Daemon functionality is currently not supported on Windows."
                )
            else:
                pass
        
        return cast(F, daemon)
    
    return inner_decorator

if sys.platform == "win32":
    logger.warning(
        "Daemon is not supported on windows. Sucky OS. Ugh"
    )
else:
    CHILD_WAIT_TIMEOUT = 5

    def kill_children() -> None:
        pid = os.getpid()
        try:
            parent = psutil.Process(pid)
        except psutil.Error:
            return
        
        children = parent.children(recursive=False)

        for p in children:
            p.terminate()
        
        _, alive = psutil.wait_procs(
            children, timeout=CHILD_WAIT_TIMEOUT,
        )
        for p in alive:
            p.kill()
        _, alive = psutil.wait_procs(
            children, timeout=CHILD_WAIT_TIMEOUT,
        )
    
    def get_daemon_pid(pid_file: str) -> Optional[str]:
        try:
            with open(pid_file, "r") as fp:
                pid = int(fp.read().strip())
        except (IOError, FileNotFoundError):
            return None
        
        if not pid or not psutil.pid_exists(pid):
            return None
        
        return pid
    
    def check_if_running(pid_file: str) -> bool:
        return get_daemon_pid(pid_file) is not None
    
    def stop_daemon(pid_file: str) -> None:
        try:
            with open(pid_file, "r") as fp:
                pid = int(fp.read().strip())
        except (IOError, FileNotFoundError):
            return None
        
        if psutil.pid_exists(pid):
            process = psutil.Process(pid)
            process.terminate()
        else:
            pass

    def run_daemon(
        daemon_function: F,
        *args: Any,
        pid_file: str,
        log_file: Optional[str],
        working_directory: str = "/",
        **kwargs: Any,
    ) -> None:
        """"""
        if pid_file:
            pid_file = os.path.abspath(pid_file)
        if log_file:
            log_file = os.path.abspath(log_file)
        
        if pid_file and os.path.exists(pid_file):
            pid = get_daemon_pid(pid_file)
            if pid:
                raise FileExistsError("")
            logger.warning("Removing left over PID file")
            os.remove(pid_file)
        
        # First fork
        try:
            pid = os.fork()

            if pid > 0:
                # We are the parent so go back
                return
        except OSError as e:
            logger.error(f"Unable to fork (error code: {e.errno}): {e}")
            sys.exit(1)
        
        os.chdir(working_directory)
        os.setsid()
        os.umask(0o22)

        # second fork
        try:
            pid = os.fork()
            if pid > 0:
                # Parent of future daemon process. Kill so daemon gets
                # adopted by init process
                sys.exit(0)
        except OSError as e:
            logger.error(f"Unable to fork (error code: {e.errno}): {e}")
            sys.exit(1)
        
        # Redirect standard out and standard error
        if hasattr(os, "devnull"):
            devnull = os.devnull
        else:
            devnull = "/dev/null"
        
        devnull_fd = os.open(devnull, os.O_RDWR)
        log_fd = (
            os.open(log_file, os.O_CREAT | os.O_RDWR | os.O_APPEND)
            if log_file
            else None
        )
        out_fd = log_fd or devnull_fd

        os.dup2(devnull_fd, sys.stdin.fileno())
        os.dup2(out_fd, sys.stdout.fileno())
        os.dup2(out_fd, sys.stderr.fileno())

        if pid_file:
            # Write PID file
            with open(pid_file, "w+") as fp:
                fp.write(f"{os.getpid()}\n")
        
        def cleanup() -> None:
            """Clean up daemon"""
            kill_children()
            if pid_file and os.path.exists(pid_file):
                os.remove(pid_file)
            
        def sighandle(signum: int, frame: Optional[types.FrameType]) -> None:
            """Daemon signal handler"""
            cleanup()
        
        signal.signal(signal.SIGTERM, sighandle)
        signal.signal(signal.SIGINT, sighandle)
        atexit.register(cleanup)

        daemon_function(*args, **kwargs)
        sys.exit(0)
        
