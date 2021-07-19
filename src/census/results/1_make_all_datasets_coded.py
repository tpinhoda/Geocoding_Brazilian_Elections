# -*- coding: utf-8 -*-
import warnings
import logging
import pandas as pd
from os import listdir, environ, mkdir
from os.path import isfile, join
from tqdm import tqdm
from dotenv import load_dotenv, find_dotenv

warnings.filterwarnings("ignore")


def code_data(input_path, output_path, wa_path, aggr):
    """Runs data processing scripts to turn raw census data from (../raw) into
    coded data (saved in ../interim).
    """
    logger = logging.getLogger(__name__)
    # List raw data
    folders = [folder for folder in listdir(input_path)]
    for folder in folders:
        logger.info("Associating codes to data from: " + folder)
        # Create state folder
        exist, output_state_path = create_folder(output_path, folder)
        input_state_path = join(input_path, folder)
        filenames = [
            filename
            for filename in listdir(input_state_path)
            if isfile(join(input_state_path, filename))
        ]

        # Delete the reference file from the list
        # filenames.remove('Basico_{}.csv'.format(folder))
        # Read the reference file
        ref_data = pd.read_csv(
            join(input_state_path, "Basico_{}.csv".format(folder)),
            sep=";",
            encoding="latin",
        )
        # Get only the columns regarding the codes
        attr_interest = ref_data.columns[0:19]
        ref_data = ref_data[attr_interest].copy()
        # Rename columns to standardize
        ref_data.columns = [
            "Cod_Setor",
            "Cod_Grande_Regiao",
            "Nome_Grande_Regiao",
            "Cod_UF",
            "Nome_UF",
            "Cod_Meso",
            "Nome_Meso",
            "Cod_Micro",
            "Nome_Micro",
            "Cod_RM",
            "Nome_RM",
            "Cod_Municipio",
            "Nome_Municipio",
            "Cod_Distrito",
            "Nome_Distrito",
            "Cod_Subdistrito",
            "Nome_Subdistrito",
            "Cod_Bairro",
            "Nome_Bairro",
        ]
        ref_data["Cod_Setor"] = ref_data["Cod_Setor"].astype("int64")
        # Associate the codes to each file in filenames
        for filename in tqdm(filenames, leave=False):
            # Load raw data
            filepath = join(input_state_path, filename)
            raw_data = pd.read_csv(filepath, sep=";", encoding="latin")
            raw_data.replace({",": "."}, regex=True, inplace=True)
            cols_to_delete = [
                col for col in raw_data.columns if "V" not in col and col != "Cod_setor"
            ]
            raw_data.drop(cols_to_delete, axis=1, inplace=True)
            if len(raw_data.columns) == 1:
                raw_data = pd.read_csv(filepath, sep=",")
            raw_data.replace("X", 0, inplace=True)
            raw_data.fillna(0, inplace=True)
            for col in raw_data.columns:
                raw_data[col] = pd.to_numeric(raw_data[col], errors="ignore")
            first_col = raw_data.columns.values[0]
            raw_data.rename(columns={first_col: "Cod_Setor"}, inplace=True)
            try:
                raw_data.drop(["Situacao_setor"], axis=1, inplace=True)
            except:
                nothing = 1  # HueHue
            raw_data.dropna(axis=1, how="all", inplace=True)
            # Associate codes to data set
            # if len(ref_data) == len(ref_data):
            #    print('entrou')
            #    exit()
            #    raw_data.drop(['Cod_Setor'], axis=1, inplace=True)
            #    merged_data = pd.concat([ref_data, raw_data], axis=1)
            # else:
            raw_data["Cod_Setor"] = raw_data["Cod_Setor"].astype("int64")
            merged_data = ref_data.merge(raw_data, on="Cod_Setor", how="left")

            # Replace 'X' by Nan
            merged_data.replace("X", pd.NA, inplace=True)

            # New filename
            f_name = filename.split("_")[0] + ".csv"
            # Save the data as csv

            merged_data = add_weighting_area_code(merged_data, wa_path)
            merged_data = aggregate_data(merged_data, aggr)
            merged_data = drop_unnamed_cols(merged_data)
            merged_data.to_csv(
                join(output_state_path, f_name), index=False, encoding="utf-8"
            )
    logger.info("Data saved in:\n" + output_state_path)
    logger.info("Done!")


def add_weighting_area_code(data, path):
    ref_data = pd.read_csv(path)
    ref_data.rename(columns={"Cod_setor": "Cod_Setor"}, inplace=True)
    data = data.merge(ref_data, on="Cod_Setor", how="left")
    return data


def aggregate_data(data, aggr):
    map_func = {col: "sum" for col in data.columns}
    code_cols = [
        "Cod_Setor",
        "Cod_ap",
        "Cod_Grande_Regiao",
        "Nome_Grande_Regiao",
        "Cod_UF",
        "Nome_UF",
        "Cod_Meso",
        "Nome_Meso",
        "Cod_Micro",
        "Nome_Micro",
        "Cod_RM",
        "Nome_RM",
        "Cod_Municipio",
        "Nome_Municipio",
        "Cod_Distrito",
        "Nome_Distrito",
        "Cod_Subdistrito",
        "Nome_Subdistrito",
        "Cod_Bairro",
        "Nome_Bairro",
    ]
    for col in code_cols:
        map_func[col] = "first"

    if aggr == "weighting_area":
        aggr_attr = "Cod_ap"
        del map_func["Cod_ap"]
        data = data.groupby(by=aggr_attr, as_index=False).agg(map_func)
    elif aggr == "sub_dist":
        aggr_attr = "Cod_Subdistrito"
        del map_func["Cod_Subdistrito"]
        data = data.groupby(by=aggr_attr, as_index=False).agg(map_func)
    elif aggr == "city":
        aggr_attr = "Cod_Municipio"
        del map_func["Cod_Municipio"]
        data = data.groupby(by=aggr_attr, as_index=False).agg(map_func)

    return data


def drop_unnamed_cols(data):
    data = data.loc[:, ~data.columns.str.contains("^Unnamed")]
    return data


def create_folder(path, folder_name):
    logger = logging.getLogger(__name__)
    path = join(path, folder_name)
    exist = 0
    try:
        mkdir(path)
    except FileExistsError:
        logger.info("Folder already exist.")
        exist = 1
    return exist, path


def run(region, year, aggr):
    # Find data.env automatically by walking up directories until it's found
    dotenv_path = find_dotenv(filename="data.env")
    # Load up the entries as environment variables
    load_dotenv(dotenv_path)
    # Get data root path
    data_dir = environ.get("ROOT_DATA")
    # Get census results path
    path = data_dir + environ.get("CENSUS_DATA")
    # Generate input output paths
    raw_path = path.format(region, year, "raw")
    interim_path = path.format(region, year, "interim")
    external_path = path.format(region, year, "external")
    # Set paths
    input_filepath = raw_path
    output_filepath = interim_path
    wa_filepath = join(external_path, "census_tract_x_weighting_area.csv")
    # Log text to show on screen
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_fmt)
    # Print parameters
    print("======Parameters========")
    print("Census year: {}".format(year))

    code_data(input_filepath, output_filepath, wa_filepath, aggr)
