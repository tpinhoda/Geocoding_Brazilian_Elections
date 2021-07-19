# -*- coding: utf-8 -*-
import warnings
import logging
import pandas as pd
import geopandas as geopd
from os import listdir
from os.path import isfile, join
from pathlib import Path
from tqdm import tqdm

warnings.filterwarnings("ignore")


def structure_data(
    input_filepath,
    reference_filepath,
    digital_mesh_filepath,
    output_filepath,
    output_digital_mesh,
):
    """Runs data processing scripts to turn raw census data data from (../raw) into
    structured data filtered by political office (saved in ../interim).
    """
    logger = logging.getLogger(__name__)

    # Reading reference file
    reference = pd.read_csv(reference_filepath)
    reference["Cod_setor"] = reference["Cod_setor"].astype(str)
    # Reading digital mesh
    digital_mesh = geopd.read_file(digital_mesh_filepath, encoding="latin")
    digital_mesh.rename(columns={"CD_GEOCODI": "Cod_setor"}, inplace=True)
    digital_mesh["Cod_setor"] = digital_mesh["Cod_setor"].astype(str)
    digital_mesh = digital_mesh.merge(reference, on="Cod_setor", how="left")
    with open(output_digital_mesh, "w") as f:
        f.write(digital_mesh.to_json())

    # Listing raw data
    filenames = [
        filename
        for filename in listdir(input_filepath)
        if isfile(join(input_filepath, filename))
    ]
    # Remove Basico file
    filenames.remove("Basico.csv")
    logger.info("Structuring raw data")
    for filename in tqdm(filenames, leave=False):
        # Loading raw data
        filepath = input_filepath + filename
        raw_data = pd.read_csv(filepath, sep=";", encoding="utf-8")
        raw_data["Cod_setor"] = raw_data["Cod_setor"].astype(str)
        # Add code of weightening area

        # Add polygon of census tract
        structured_data = digital_mesh.merge(raw_data, on="Cod_setor", how="right")
        # Save the data as csv
        filename = filename.split(".")[0]
        # structured_data.to_file(output_filepath+filename+'.shp', index=False)
        # structured_data.to_file(output_digital_mesh+filename+'.gpkg', driver="GPKG")
        structured_data.to_csv(
            output_filepath + filename + ".csv", index=False, sep=";"
        )

    logger.info("Done!")


if __name__ == "__main__":
    # Project path
    project_dir = str(Path(__file__).resolve().parents[4])
    # Set data parammeters
    country = "Brazil"
    census_year = "2010"
    state = "RS"
    # Set paths
    input_filepath = (
        project_dir
        + "/data/raw/{}/IBGE/census_data/{}/census_tract/universal_results/{}/".format(
            country, census_year, state
        )
    )
    reference_filepath = (
        project_dir
        + "/data/raw/{}/IBGE/census_data/{}/weightening_area/weightening_area_by_census_tract.csv".format(
            country, census_year, state
        )
    )
    digital_mesh_filepath = (
        project_dir
        + "/data/raw/{}/IBGE/digital_mesh/census_tract/{}/43SEE250GC_SIR.shp".format(
            country, census_year, state
        )
    )

    output_filepath = (
        project_dir
        + "/data/interim/{}/IBGE/census_data/{}/census_tract/universal_results/states/{}/".format(
            country, census_year, state
        )
    )
    output_digital_mesh = (
        project_dir
        + "/data/interim/{}/IBGE/census_data/{}/census_tract/digital_mesh/{}.geojson".format(
            country, census_year, state
        )
    )

    # Log text to show on screen
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # Print parameters
    print("======Parameters========")
    print("Country: {}".format(country))
    print("Census year: {}".format(census_year))
    print("State: {}".format(state))

    structure_data(
        input_filepath,
        reference_filepath,
        digital_mesh_filepath,
        output_filepath,
        output_digital_mesh,
    )
