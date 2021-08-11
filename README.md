# Geocoding Brazilian Elections

This project pre-process and geocode Brazilian electoral data. Each row of the final dataset will represent a location (i.e, polling place, city, state, etc), and each location will be desbribed by the electoral results they presented. Moreover, since no geocoding is perfect we provide manners to filter locations based on the level of precision needed.

> ## Setup

1. Create a **.env** file, insert an fill the following envirommental variables:

    ```` env
        ROOT_DATA= <path to save the data>
        API_KEY= <google geocoding api key>
    ````

    :warning: Even though you wont use google api the variable needs to be created, just fill with random characteres.

2. Install all packages in requiments.txt.

    ```` bash
        conda install --file requirements.txt
    ````

3. Install the project as package.

    ```` bash
        pip install .
    ````

> ## Usage

1. Configure the parameters in the files:

    ```` bash
    ├── data
        ├── parameters.json
        ├── switchers.json
    ````

2. Run src/main.py

    ```` bash
        python src/main.py
    ````

> ## Parameters description

Description of the parameters needed to execute the code.

>>### parameters.json

* **global**: General parameters
  * **region**: The name of the region (Ex: Brazil)
  * **org**: The name of the federal agency
  * **year**: The election year
  * **round**: The election round
  * **aggregation_level**: Geographical level of data aggregation
  * **geocoding_api**: The name of the geocoding api used
* **locations**: parameters for locations to be geocoded
  * **data_name**: The name of the data (Ex: locations)
  * **url_data**: The url to download the locations containg addresses
  * **data_filename**: The name of the donwloaded data
  * **url_meshblock**:  The url to download the Brazilian cities meshblocks
  * **meshblocks_filename**: The name of the meshblock file downloaded
  * **save_at**: Number of geocoded address until save the progress
  * **meshblock_crs**: Meshblock coordinate system
  * **meshblock_id**: Meshblock id column
  * **city_buffers**: List of buffering to increase cities boundaries
* **results**: parameters regarding electoral results
  * **data_name** The name of the data (Ex: results)
  * **url_data** The url to download the election results
  * **candidacy_pos** The candidacy position to be filtered
  * **candidates** The candidades ids to be filtered
  * **levenshtein_threshild**: The levenshtein similarity threshold to filther the locations
  * **precision filter** The precision to filter the dataset
  * **city_limits_filter** The city limits allowed consider right geocoding

>> ### switchers.json

* **locations**: switchers regarding the locations pipeline
  * **raw**: switch to run the raw process (0 or 1)
  * **interim**: switch to run the interim process (0 or 1)
  * **processed**: switch to run the processed process (0 or 1)
* **results**: switchers regarding the electoral results pipeline
  * **raw**: switch to run the raw process (0 or 1)
  * **interim**: switch to run the interim process (0 or 1)
  * **processed**: switch to run the processed process (0 or 1)

:warning: The switchers turn on and off the processes of the pipeline, by default let them all turned on (**filled with 1**), so the entire pipeline can be executed.

>## Geocoding api supported

At the moment we only support:

* Google api: parameter value = **GMAP**
* Open Stree Maps: parameter value = **OSM**

We also provide a "geocoding" for units of the federation level of aggregation based on the IBGE meshblocks by considering their centroid. The parameter value for this options is **IBGE**
>## Final dataset sample

| [GEO]_ID_TSE_CITY | [GEO]_ID_POLLING_ZONE | [GEO]_ID_POLLING_PLACE | [GEO]_ID_POLLING_SECTION | [GEO]_UF | [GEO]_CITY | [ELECTION]_ELECTORATE | [ELECTION]_TURNOUT | [ELECTION]_ABSTENTIONS | [ELECTION]_ELECTORATE_BIOMETRIA | [ELECTION]_CANDIDATE_13 | [ELECTION]_CANDIDATE_17 | [ELECTION]_BLANK | [ELECTION]_NULL | [GEO]_ID_IBGE_CITY | [GEO]_POLLING_ZONE | [GEO]_POLLING_PLACE | [GEO]_POLLING_PLACE_NEIGHBORHOOD | [GEO]_POLLING_PLACE_ADDRESS | [GEO]_CEP_CODE | [GEO]_LATITUDE | [GEO]_LONGITUDE | [GEO]_CLEAN_ADDRESS | [GEO]_PRECISION | [GEO]_FETCHED_ADDRESS | [GEO]_QUERY_ADDRESS | geometry | [GEO]_CITY_LIMITS | [GEO]_LEVENSHTEIN_SIMILARITY | [GEO]_RURAL_MARKS | [GEO]_CAPITAL_MARKS |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1120 | 8 | 1074 | 101 | AC | ACRELÂNDIA | 8808 | 6697 | 2111 | 1178 | 36 | 153 | 65 | 167 | 1200013 | CENTRO | ESCOLA ALTINA MAGALHAES | ZONA RURAL | BR 364 - KM 114 S/N | 69945000 | -9.93365116571054 | -66.9504957459741 | BR 364 - KM 114 S/N | IBGE | BR 364 - KM 114 S/N | BR 364 - KM 114 S/N | POINT (-66.95049574597411 -9.933651165710543) | in | 1 | True | False |
| 1570 | 6 | 1309 | 104 | AC | ASSIS BRASIL | 5727 | 4137 | 1590 | 477 | 129 | 30 | 42 | 102 | 1200054 | CENTRO | ESCOLA SIMON BOLIVAR | CENTRO | RUA DOM GIOCONDO MARIA GROTT N. 019 | 69935000 | -10.7789593441465 | -70.0058200047204 | RUA DOM GIOCONDO MARIA GROTT N. 019 | IBGE | RUA DOM GIOCONDO MARIA GROTT N. 019 | RUA DOM GIOCONDO MARIA GROTT N. 019 | POINT (-70.00582000472043 -10.77895934414647) | in | 1 | False | False |
| 1058 | 6 | 2151 | 100 | AC | BRASILÉIA | 16027 | 12317 | 3710 | 1443 | 88 | 100 | 142 | 256 | 1200104 | CENTRO | CENTRO DE REFERENCIA PARA MULHERES | CENTRO | RUA JOSE KAIRALA N. 0042 | 69932000 | -10.7466460576254 | -69.2076799476512 | RUA JOSE KAIRALA N. 0042 | IBGE | RUA JOSE KAIRALA N. 0042 | RUA JOSE KAIRALA N. 0042 | POINT (-69.2076799476512 -10.74664605762539) | in | 1 | False | False |

## Project Organization

    ├── LICENSE
    ├── README.md          <- The top-level README for developers using this project.
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │
    ├── setup.py           <- makes project pip installable (pip install -e .) so src can be imported
    ├── src                <- Source code for use in this project.
    │   ├── __init__.py    <- Makes src a Python module
    │   ├── election.py    <- Election abstract class
    │   ├── pipeline.py    <- Pipeline class
    │   ├── main.py    <- Main function
    │   │
    │   ├── results           <- Scripts to process electoral results data
    │   │   └── raw.py
    │   │   └── interim.py
    │   │   └── preocessed.py
    │   │
    │   ├── locations           <- Scripts to process locations data
    │   │   └── raw.py
    │   │   └── interim.py
    │   │   └── preocessed.py
--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
