# Geocoding Brazilian Elections

This project pre-process and geocode Brazilian electoral data. Each row of the final dataset will represent a location (i.e, polling place, city, state, etc), and each location will be desbribed by the electoral results they presented. Moreover, since no geocoding is perfect we provide manners to filter locations based on the level of precision needed.

> ## Setup

1. Create a **.env** file, insert an fill the following envirommental variables:

    ```` env
        ROOT_DATA= <path to save the data>
        API_KEY= <google geocoding api key>
    ````

    :warning: Even though you wont use google api the variable needs to be created, just fill with random characteres.

2. Create a  vitual enviroment and install all packages in requiments.txt.

    ```` bash
        conda create --name <env> --file requirements.txt
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

| [GEO]_ID_TSE_CITY | [GEO]_ID_POLLING_ZONE | [GEO]_ID_POLLING_PLACE | [GEO]_ID_POLLING_SECTION | [GEO]_UF | [GEO]_CITY | [ELECTION]_ELECTORATE | [ELECTION]_TURNOUT | [ELECTION]_ABSTENTIONS | [ELECTION]_ELECTORATE_BIOMETRIA | [ELECTION]_CANDIDATE_13 | [ELECTION]_CANDIDATE_17 | [ELECTION]_NULL | [ELECTION]_BLANK | [ELECTION]_CANDIDATE_13_(%) | [ELECTION]_CANDIDATE_17_(%) | [ELECTION]_NULL_(%) | [ELECTION]_BLANK_(%) | [ELECTION]_TURNOUT_(%) | [ELECTION]_ABSTENTIONS_(%) | [GEO]_LATITUDE | [GEO]_LONGITUDE | [GEO]_FETCHED_ADDRESS | [GEO]_PRECISION | [GEO]_POLLING_PLACE | [GEO]_POLLING_PLACE_ADDRESS | [GEO]_CEP_CODE | [GEO]_ID_IBGE_CITY | [GEO]_POLLING_ZONE | [GEO]_POLLING_PLACE_NEIGHBORHOOD | [GEO]_CLEAN_ADDRESS | [GEO]_QUERY_ADDRESS | geometry | [GEO]_CITY_LIMITS | [GEO]_LEVENSHTEIN_SIMILARITY | [GEO]_RURAL_MARKS | [GEO]_CAPITAL_MARKS |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1120 | 8 | 1015 | 143 | AC | ACRELÂNDIA | 924 | 683 | 241 | 115 | 156 | 513 | 7 | 7 | 22.8404099560761 | 75.1098096632504 | 1.02489019033675 | 1.02489019033675 | 73.9177489177489 | 26.0822510822511 | -9.948481 | -66.979777 | Br 364 Km 114 S/N, Acrelândia - AC, 69945-000, Brasil | GEOMETRIC_CENTER | ESCOLA ALTINA MAGALHAES | BR 364 - KM 114 S/N | 69945000 | 1200013 | CENTRO | ZONA RURAL | BR 364 - KM 114 S/N | BR 364 - KM 114 S/N, ACRELÂNDIA, AC | POINT (-66.979777 -9.948480999999999) | in | 0.727272727272727 | True | False |
| 1120 | 8 | 1023 | 224 | AC | ACRELÂNDIA | 1017 | 769 | 248 | 204 | 157 | 584 | 18 | 10 | 20.4161248374512 | 75.9427828348504 | 2.3407022106632 | 1.30039011703511 | 75.6145526057031 | 24.3854473942969 | -10.0785588 | -67.0559827 | 562, - Av. Paraná, 454, Acrelândia - AC, 69945-000, Brasil | ROOFTOP | ESCOLA  DE 1 GRAU PROF PEDRO DE CASTRO MEIRELES | AV PARANA COM RUA DOS PINHEIROS | 69945000 | 1200013 | CENTRO | CENTRO | AV PARANA COM RUA DOS PINHEIROS | AV PARANA COM RUA DOS PINHEIROS, ACRELÂNDIA, AC | POINT (-67.0559827 -10.0785588) | in | 0.457142857142857 | False | False |
| 1120 | 8 | 1031 | 160 | AC | ACRELÂNDIA | 550 | 416 | 134 | 58 | 84 | 323 | 4 | 5 | 20.1923076923077 | 77.6442307692308 | 0.961538461538462 | 1.20192307692308 | 75.6363636363636 | 24.3636363636364 | -16.7219947 | -49.2461329 | Vila Redencao, Goiânia - GO, Brasil | APPROXIMATE | ESCOLA DE 1 GRAU MARIA DE JESUS RIBEIRO | AC 475 VILA REDENCAO KM 15 RUA TEREZA DE JESUS PINTO N 298 | 69945000 | 1200013 | CENTRO | ZONA RURAL | AC 475 VILA REDENCAO KM 15 RUA TEREZA DE JESUS PINTO N 298 | AC 475 VILA REDENCAO KM 15 RUA TEREZA DE JESUS PINTO N 298, ACRELÂNDIA, AC | POINT (-49.2461329 -16.7219947) | out | 0.403669724770642 | True | False |

## Project Organization

```` text
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
    ├────
````

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
