import os


def process_bool_env_var(var: str, default: bool = False) -> bool:
    """process_bool_env_car converts env var to boolean.
    
    Args
        var: the variable to grab from the environment
        default: 
    """
    value = os.getenv(var)
    if value in {"1", "True", "true"}:
        return True
    else:
        return default

# Global
APP_NAME = "CoalescenceML"

# Env variables
ENV_COML_DEBUG = "COML_DEBUG"
ENV_COML_LOGGING_VERBOSITY = "COML_LOGGING_VERBOSITY"
ENV_COML_CONFIG_PATH = "COML_CONFIG_PATH"
ENV_COML_DEFAULT_STORE_TYPE = "COML_DEFAULT_STORE_TYPE"
ENV_COML_PREVENT_PIPELINE_EXECUTION = "COML_PREVENT_PIPELINE_EXECUTION"
ENV_COML_PROFILE_NAME = "COML_PROFILE_NAME"
ENV_COML_DIRECTORY_PATH = "COML_DIRECTORY_PATH"

# Logging variables
IS_DEBUG_ENV: bool = handle_bool_env_var(ENV_COML_DEBUG, default=False)

COML_LOGGING_VERBOSITY: str = "INFO"

if IS_DEBUG_ENV:
    COML_LOGGING_VERBOSITY = os.getenv(
        ENV_COML_LOGGING_VERBOSITY,
        default="DEBUG"
    ).upper()
else:
    COML_LOGGING_VERBOSITY = os.getenv(
        ENV_COML_LOGGING_VERBOSITY,
        default="INFO"
    ).upper()

REMOTE_FS_PREFIX = ["gs://", "s3://", "az://"]


# Container utils
SHOULD_PREVENT_PIPELINE_EXECUTION = handle_bool_env_var(
    ENV_COML_PREVENT_PIPELINE_EXECUTION
)

# DIRECTORY and local store directory paths:
DIRECTORY_DIRECTORY_NAME = ".coml"
LOCAL_STORES_DIRECTORY_NAME = "local_stores"