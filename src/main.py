import coloredlogs, logging

coloredlogs.install()
log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=log_fmt)

from rich.traceback import install

install()

import os
import json
from src.pipeline import Pipeline
from dotenv import load_dotenv
from pathlib import Path
from typing import Dict


def load_env_variables(project_dir) -> Dict[str, str]:
    """Loads enviromental variables in the .env file."""
    dotenv_path = os.path.join(project_dir, ".env")
    load_dotenv(dotenv_path)
    return {
        "root_path": os.environ.get("ROOT_DATA"),
        "api_key": os.environ.get("API_KEY"),
    }


def load_json(params_path):
    """Load json file."""
    with open(params_path) as f:
        return json.load(f)


if __name__ == "__main__":
    # Project path
    project_dir = str(Path(__file__).resolve().parents[1])
    # Load enviromental variables
    env_var = load_env_variables(project_dir)
    # Load paramenters
    params = load_json(os.path.join(project_dir, "parameters", "parameters.json"))
    params["global"]["root_path"] = env_var["root_path"]
    params["locations"]["api_key"] = env_var["api_key"]
    # Load switchers
    switchers = load_json(os.path.join(project_dir, "parameters", "switchers.json"))
    # Creates the processing pipeline
    pipeline = Pipeline("locations", params, switchers["locations"])
    pipeline.run()
