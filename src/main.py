"""Main script"""
import os
import json
import logging
from pathlib import Path
from typing import Dict
from dotenv import load_dotenv
from coloredlogs import install as coloredlogs_install
from rich.traceback import install as rich_install
from src.pipeline import Pipeline


def initialize_coloredlog():
    """Install the colored log package."""
    coloredlogs_install()


def initialize_logging():
    """Initialize the format of the logger messages"""
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_fmt)


def initialize_rich():
    """Install the rich tracerback"""
    rich_install()


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
    with open(params_path) as file:
        return json.load(file)


def main():
    """Main function"""
    initialize_coloredlog()
    initialize_rich()
    initialize_logging()
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
    # Creates and run the location processing pipeline
    pipeline_locations = Pipeline("locations", params, switchers["locations"])
    pipeline_locations.run()
    # Creates and run the results processing pipeline
    pipeline_results = Pipeline("results", params, switchers["results"])
    pipeline_results.run()


if __name__ == "__main__":
    main()
