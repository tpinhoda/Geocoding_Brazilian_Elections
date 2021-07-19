# -*- coding: utf-8 -*-
import warnings
import pandas as pd
import logging
from os import listdir, environ, remove
from os.path import isfile, join
from tqdm import tqdm
from dotenv import load_dotenv, find_dotenv
from pandas_profiling import ProfileReport

warnings.filterwarnings("ignore")


def merge_data(election_results, polling_places, output_filepath):
    """Runs scripts to turn merge interim election data data from (../interim/election_results) with
    polling places processed data from (../processed/polling_places).

    The results of this script is saved in ../interim/election_results
    """
    logger = logging.getLogger(__name__)
    logger.info("1 - Merging data from:\n" + election_results + "\n" + polling_places)
    # Load data with geocode information from polling places
    polling_places = pd.read_csv(polling_places)
    # Generate an id to facilitate merging
    polling_places["id_polling_place"] = (
        polling_places["SGL_UF"]
        + polling_places["LOCALIDADE_LOCAL_VOTACAO"]
        + polling_places["ZONA"].astype(str)
        + polling_places["NUM_LOCAL"].astype(str)
    )

    # List interim data
    filenames = [
        filename
        for filename in listdir(election_results)
        if isfile(join(election_results, filename))
    ]
    list_state_df = []
    # Merge each file in filenames
    for filename in tqdm(filenames):
        # Load interim data
        filepath = election_results + filename
        results = pd.read_csv(filepath)
        # Delete interim file
        remove(filepath)
        # Convert columns to string type
        results["NR_ZONA"] = results["NR_ZONA"].astype(str)
        results["NR_LOCAL_VOTACAO"] = results["NR_LOCAL_VOTACAO"].astype(str)
        results["NR_SECAO"] = results["NR_SECAO"].astype(str)
        # Generate an ids to facilitate merging
        id_pl = (
            results["SG_ UF"]
            + results["NM_MUNICIPIO"]
            + results["NR_ZONA"]
            + results["NR_LOCAL_VOTACAO"]
        )
        id_sec = (
            results["SG_ UF"]
            + results["NM_MUNICIPIO"]
            + results["NR_ZONA"]
            + results["NR_SECAO"]
        )
        id_zone = results["SG_ UF"] + results["NM_MUNICIPIO"] + results["NR_ZONA"]
        id_city = results["SG_ UF"] + results["NM_MUNICIPIO"]
        # Input columns ids
        results["id_polling_place"] = id_pl
        results["id_section"] = id_sec
        results["id_zone"] = id_zone
        results["id_city"] = id_city
        # Merge datasets
        merged = results.merge(
            polling_places[
                [
                    "id_polling_place",
                    "COD_LOCALIDADE_IBGE",
                    "local_unico",
                    "lat",
                    "lon",
                    "geometry",
                    "rural",
                    "capital",
                    "precision",
                    "lev_dist",
                    "city_limits",
                ]
            ],
            on="id_polling_place",
            how="left",
        )
        # Save the data as csv (Does not make sense have redundant data)
        # merged.to_csv(states_output_filepath + filename, index=False)

        # Append to list
        list_state_df.append(merged)

    # Save final dataset
    output_filepath = output_filepath + "data.csv"
    logger.info("2 - Saving final dataset in:\n" + output_filepath)
    final_df = pd.concat(list_state_df)
    final_df.to_csv(output_filepath, index=False)
    logger.info("Done!")
    return final_df


def generate_data_statistics(data, output_path):
    prof = ProfileReport(data)
    prof.to_file(output_file=output_path + "/summary.html")


def run(region, year, office_folder, turn):
    # Find data.env automatically by walking up directories until it's found
    dotenv_path = find_dotenv(filename="data.env")
    # Load up the entries as environment variables
    load_dotenv(dotenv_path)
    load_dotenv(dotenv_path)
    # Get data root path
    data_dir = environ.get("ROOT_DATA")
    # Get election results path
    election_results_path = data_dir + environ.get("ELECTION_RESULTS")
    polling_places_path = data_dir + environ.get("POLLING_PLACES")
    # Generate input output paths
    election_results_interim_path = election_results_path.format(
        region, year, "interim"
    )
    polling_places_processed_path = polling_places_path.format(
        region, year, "processed"
    )
    # Set paths
    election_results_path = election_results_interim_path + "/{}/turn_{}/".format(
        office_folder, turn
    )
    polling_places_path = polling_places_processed_path + "/polling_places.csv"
    # Log text to show on screen
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_fmt)
    # Print parameters
    # print('======Parameters========')
    # print('Election year: {}'.format(year))
    # print('Office: {}'.format(office_folder))
    # print('Turn: {}'.format(turn))

    data = merge_data(election_results_path, polling_places_path, election_results_path)
    generate_data_statistics(data, election_results_path)
