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


def disolve_digital_mesh(digital_mesh_filepath, output_digital_mesh, attr):
    logger = logging.getLogger(__name__)

    logger.info("Disolving digital mesh")

    digital_mesh = geopd.read_file(digital_mesh_filepath, driver="GeoJSON")
    data_aggr = digital_mesh.dissolve(by=attr, aggfunc="first", as_index=False)
    logger.info("Saving digital mesh")
    with open(output_digital_mesh, "w") as f:
        f.write(data_aggr[["Cod_ap", "CD_GEOCODM", "NM_MUNICIP", "geometry"]].to_json())

    logger.info("Done!")


def structure_data(input_filepath, output_filepath, attr):
    """Runs data processing scripts to turn raw census data data from (../raw) into
    structured data filtered by political office (saved in ../interim).
    """
    logger = logging.getLogger(__name__)

    # Listing raw data
    filenames = [
        filename
        for filename in listdir(input_filepath)
        if isfile(join(input_filepath, filename))
    ]

    logger.info("Aggregating data")
    for filename in tqdm(filenames, leave=False):
        print(filename)
        # Loading raw data
        filepath = input_filepath + filename
        data = pd.read_csv(filepath, sep=";", encoding="utf-8")
        data.replace("X", 0, inplace=True)

        attr_cols = data.columns.values.tolist()
        not_to_sum = [
            "Cod_setor",
            "Cod_ap",
            "geometry",
            "Situacao_setor",
            "CD_GEOCODB",
            "NM_BAIRRO",
            "CD_GEOCODS",
            "NM_SUBDIST",
            "CD_GEOCODD",
            "NM_DISTRIT",
            "CD_GEOCODM",
            "NM_MUNICIP",
            "NM_MICRO",
            "NM_MESO",
            "ID",
            "TIPO",
        ]

        for col_name in not_to_sum:
            attr_cols.remove(col_name)

        for col_name in attr_cols:
            data[col_name] = pd.to_numeric(data[col_name], errors="coerce")

        geo_cols = attr + ["CD_GEOCODM", "NM_MUNICIP"]
        attr_cols = geo_cols + attr_cols

        aggr_map_func = {col_name: "sum" for col_name in attr_cols}
        aggr_map_func["CD_GEOCODM"] = "first"
        aggr_map_func["NM_MUNICIP"] = "first"
        del aggr_map_func["Cod_ap"]

        data_aggr = data[attr_cols].groupby(by=attr, as_index=False).agg(aggr_map_func)
        # data_aggr = data[attr_cols].dissolve(by=attr, aggfunc=aggr_map_func).reset_index(drop=True)
        # Save the data as csv
        filename = filename.split(".")[0]

        # digital_mesh = data_aggr[geo_cols]
        # print(data_aggr.head())

        # digital_mesh.to_file(output_digital_mesh+filename+'.geojson', driver='GeoJson')

        # data_aggr.drop('geometry', axis=1, inplace=True)

        data_aggr.to_csv(output_filepath + filename + ".csv", index=False)

    logger.info("Done!")


if __name__ == "__main__":
    # Project path
    project_dir = str(Path(__file__).resolve().parents[4])
    print(project_dir)

    # Set data parammeters
    country = "Brazil"
    census_year = "2010"
    state = "RS"

    # Set paths
    input_filepath = (
        project_dir
        + "/data/interim/{}/IBGE/census_data/{}/census_tract/universal_results/states/{}/".format(
            country, census_year, state
        )
    )
    output_filepath = (
        project_dir
        + "/data/interim/{}/IBGE/census_data/{}/weightening_area/universal_results/states/{}/".format(
            country, census_year, state
        )
    )

    digital_mesh_filepath = (
        project_dir
        + "/data/interim/{}/IBGE/digital_mesh/census_tract/{}.geojson".format(
            country, census_year, state
        )
    )
    output_digital_mesh = (
        project_dir
        + "/data/interim/{}/IBGE/digital_mesh/weighting_area/{}.geojson".format(
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

    disolve_digital_mesh(digital_mesh_filepath, output_digital_mesh, attr="Cod_ap")
    structure_data(input_filepath, output_filepath, attr=["Cod_ap"])
