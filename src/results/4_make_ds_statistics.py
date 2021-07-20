# -*- coding: utf-8 -*-
import warnings
import logging
import pandas as pd
from os import environ
from dotenv import load_dotenv, find_dotenv
from pandas_profiling import ProfileReport

warnings.filterwarnings("ignore")


def calculate_integrity(data):
    tse_score = sum(data["precision"] == "TSE") * 1
    rooftop_score = sum(data["precision"] == "ROOFTOP") * 0.8
    ri_score = sum(data["precision"] == "RANGE_INTERPOLATED") * 0.6
    gc_score = sum(data["precision"] == "GEOMETRIC_CENTER") * 0.4
    approximate_score = sum(data["precision"] == "APPROXIMATE") * 0.2
    nv_score = sum(data["precision"] == "NO_VALUE") * 0.0

    precision_score = (
        tse_score + rooftop_score + ri_score + gc_score + approximate_score + nv_score
    ) / len(data)

    in_score = sum(data["city_limits"] == "in") * 1

    # Add manually the others
    boundary01_score = sum(data["city_limits"] == "boundary_0.01") * 0.99
    boundary02_score = sum(data["city_limits"] == "boundary_0.02") * 0.98
    boundary03_score = sum(data["city_limits"] == "boundary_0.03") * 0.97

    out_score = sum(data["city_limits"] == "out") * 0

    city_limits_score = (
        in_score + out_score + boundary01_score + boundary02_score + boundary03_score
    ) / len(data)

    levenstein_score = sum(data["lev_dist"]) / len(data)

    final_score = (precision_score + city_limits_score + levenstein_score) / 3

    return final_score


def n_cities(original, cleaned):
    cities = len(cleaned.groupby("CD_MUNICIPIO"))
    cities_perc = 100 * cities / len(original.groupby("CD_MUNICIPIO"))
    cities = str(cities) + " (" + str(round(cities_perc, 2)) + "%)"

    left_cities = len(original.groupby("CD_MUNICIPIO")) - len(
        cleaned.groupby("CD_MUNICIPIO")
    )
    left_cities_perc = 100 - cities_perc
    left_cities = str(left_cities) + " (" + str(round(left_cities_perc, 2)) + "%)"
    return {"represented": cities, "left": left_cities}


def n_aggr_level(original, cleaned, aggr):
    if aggr == "Polling place":
        aggr_attr = "id_polling_place"
    elif aggr == "City":
        aggr_attr = "id_city"
    elif aggr == "Section":
        aggr_attr = "id_section"
    elif aggr == "Zone":
        aggr_attr = "id_zone"
    original = original.groupby(aggr_attr).agg("first")
    rows = len(cleaned)
    rows_perc = 100 * rows / len(original)
    rows = str(rows) + " (" + str(round(rows_perc, 2)) + "%)"

    left_rows = len(original) - len(cleaned)
    left_rows_perc = 100 * left_rows / len(original)
    left_rows = str(left_rows) + " (" + str(round(left_rows_perc, 2)) + "%)"
    return {"represented": rows, "left": left_rows}


def n_attribute(original, cleaned, attr):
    attr_size = sum(cleaned[attr])
    attr_size_perc = 100 * attr_size / sum(original[attr])
    attr_size = str(attr_size) + " (" + str(round(attr_size_perc, 2)) + "%)"

    left_attr_size = sum(original[attr]) - sum(cleaned[attr])
    left_attr_size_perc = 100 * left_attr_size / sum(original[attr])
    left_attr_size = (
        str(left_attr_size) + " (" + str(round(left_attr_size_perc, 2)) + "%)"
    )
    return {"represented": attr_size, "left": left_attr_size}


def generate_parameters_report(aggr_level, city_lim, precisions, lev):
    parameters_report = {
        "Aggregate level": aggr_level,
        "City limits:": [city_lim],
        "Precision categories": [precisions],
        "Levenshtein threshold": lev,
    }
    report_df = pd.DataFrame(parameters_report)
    parameters_markdown = "## Filter Parameters \n" + report_df.to_markdown(
        showindex=False
    )
    return parameters_markdown


def generate_precisions_report(cleaned_data):
    tse = sum(cleaned_data["precision"] == "TSE")
    rooftop = sum(cleaned_data["precision"] == "ROOFTOP")
    r_interpolated = sum(cleaned_data["precision"] == "RANGE_INTERPOLATED")
    g_center = sum(cleaned_data["precision"] == "GEOMETRIC_CENTER")
    approximate = sum(cleaned_data["precision"] == "APPROXIMATE")
    n_value = sum(cleaned_data["precision"] == "NO_VALUE")

    precision_report = {
        "TSE": tse,
        "Rooftop": rooftop,
        "Range interpolated": r_interpolated,
        "Geometric center": g_center,
        "Approximate": approximate,
        "No value": n_value,
    }
    report_df = pd.DataFrame(precision_report, index=[0])
    precisions_markdown = "\n ## Precisions \n" + report_df.to_markdown(showindex=False)
    return precisions_markdown


def generate_votes_report(original_data, cleaned_data, candidates):
    null = n_attribute(original_data, cleaned_data, "NULO")
    blank = n_attribute(original_data, cleaned_data, "BRANCO")
    votes_report = {"Null": null["represented"], "Branco": blank["represented"]}
    for c in candidates:
        c_votes = n_attribute(original_data, cleaned_data, c.upper())
        votes_report[c] = c_votes["represented"]

    report_df = pd.DataFrame(votes_report, index=[0])
    votes_markdown = "\n ## Votes \n" + report_df.to_markdown(showindex=False)
    return votes_markdown


def generate_summary_report(original_data, cleaned_data, aggregate_level):
    cities = n_cities(original_data, cleaned_data)
    aggr_places = n_aggr_level(original_data, cleaned_data, aggregate_level)
    electorate = n_attribute(original_data, cleaned_data, "QT_APTOS")
    turnout = n_attribute(original_data, cleaned_data, "QT_COMPARECIMENTO")

    summary_report = {
        "Cities": cities["represented"],
        "(Not Included) Cities": cities["left"],
        aggregate_level: aggr_places["represented"],
        "(Not Included) " + aggregate_level: aggr_places["left"],
        "Electorate": electorate["represented"],
        "(Not Included) Electorate": electorate["left"],
        "Turnout": turnout["represented"],
        "(Not Included) Turnout": turnout["left"],
    }

    report_df = pd.DataFrame(summary_report, index=[0])
    summary_markdown = "\n ## Summary \n" + report_df.to_markdown(showindex=False)
    return summary_markdown


def generate_data_statistics(data, output_path):
    prof = ProfileReport(data)
    prof.to_file(output_file=output_path)


def run(
    region,
    year,
    office_folder,
    turn,
    candidates,
    city_limits,
    levenshtein_threshold,
    precision_categories,
    aggregate_level,
    per,
):
    # Find data.env automatically by walking up directories until it's found
    dotenv_path = find_dotenv(filename="data.env")
    # Load up the entries as environment variables
    load_dotenv(dotenv_path)
    # Get data root path
    data_dir = environ.get("ROOT_DATA")
    # Get election results path
    path = data_dir + environ.get("ELECTION_RESULTS")
    # Generate input output paths
    interim_path = path.format(region, year, "interim")
    processed_path = path.format(region, year, "processed")
    # Set path
    aggr_folder = "aggr_by_" + aggregate_level.lower().replace(" ", "_")
    processed_data = processed_path + "/{}/turn_{}/{}/PER_{}/".format(
        office_folder, turn, aggr_folder, per
    )
    interim_data = interim_path + "/{}/turn_{}/".format(office_folder, turn)
    # Log text to show on screen
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_fmt)
    # Print parameters
    # print('======Global parameters========')
    # print('Election year: {}'.format(year))
    # print('Office: {}'.format(office_folder))
    # print('Turn: {}'.format(turn))
    # print('PER: {}'.format(per))
    # Load original and cleaned data
    original_df = pd.read_csv(interim_data + "data.csv")
    cleaned_df = pd.read_csv(processed_data + "data.csv")

    # Generate the reports
    parameters_report = generate_parameters_report(
        aggregate_level, city_limits, precision_categories, levenshtein_threshold
    )
    summary_report = generate_summary_report(original_df, cleaned_df, aggregate_level)
    precisions_report = generate_precisions_report(cleaned_df)
    votes_report = generate_votes_report(original_df, cleaned_df, candidates)

    # Save the report
    final_report = (
        "# Data set: PER_{} \n".format(per)
        + parameters_report
        + "\n"
        + summary_report
        + "\n"
        + precisions_report
        + "\n"
        + votes_report
    )

    print(final_report, file=open(processed_data + "summary.md", "w"))
    for c in candidates:
        cleaned_df[c] = cleaned_df[c].astype("int64")

    generate_data_statistics(cleaned_df, processed_data + "profiling.html")
    is_score = calculate_integrity(cleaned_df)
    return is_score
