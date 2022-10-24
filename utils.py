import csv
#from difflib import SequenceMatcher
import json
import pandas as pd
from pathlib import Path
import re
import xlrd


def make_dir(path=None):
    """Create a new directory at this given path. 

    path = None
    """
    assert isinstance(path, str), (
        f"Path must be a string; you passed a {type(path)}"
    )
    
    # parents = True creates missing parent paths
    # exist_ok = True igores FileExistsError exceptions
    # these settings mimick the POSIX mkdir -p command
    Path(path).mkdir(parents=True, exist_ok=True)
    
def read_json(fl):
    
    assert isinstance(fl, str), (
        f"fl must be a string; you passed a {type(fl)}"
    )
    
    # Opening JSON file
    with open(fl) as json_file:
        data = json.load(json_file)
        return data

def get_fieldnames(tableName=None, schema_json=None):
    """switcher to get field names for each table
    
    schema_json is a json file containing the openClimate schema
        {table_name : list_of_table_columns}
    """
    
    if schema_json is None:
        schema_json = './openClimate_schema.json'
    
    assert isinstance(schema_json, str), (
        f"schema_json must be a string; not a {type(schema_json)}"
    )
    
    # switcher stuff needs to be a JSON
    switcher = read_json(fl='./openClimate_schema.json')
    return switcher.get(tableName.lower(), f"{tableName} not in {list(switcher.keys())}")


def write_to_csv(outputDir=None, 
                 tableName=None, 
                 dataDict=None, 
                 mode=None):
    
    # set default values 
    outputDir = '.' if outputDir is None else outputDir
    tableName = 'Output' if tableName is None else tableName
    dataDict = {} if dataDict is None else dataDict
    mode = 'w' if mode is None else mode
        
    # ensure correct type
    assert isinstance(outputDir, str), f"outputDir must a be string"
    assert isinstance(tableName, str), f"tableName must be a string"
    assert isinstance(dataDict, dict), f"dataDict must be a dictionary"
    acceptableModes = ['r', 'r+', 'w', 'w+', 'a', 'a+', 'x']
    assert mode in acceptableModes, f"mode {mode} not in {acceptableModes}"
    
    # test that dataDict has all the necessary fields
    fieldnames_in_dict = [key in dataDict for key in get_fieldnames(tableName)]
    assert all(fieldnames_in_dict), f"Key mismatch: {tuple((dataDict.keys()))} != {get_fieldnames(tableName)}"
    
    # remove a trailing "/" in the path
    out_dir = Path(outputDir).as_posix()

    # create out_dir if does not exist
    make_dir(path=out_dir)
    
    # write to file 
    with open(f'{out_dir}/{tableName}.csv', mode) as f:
        w = csv.DictWriter(f, fieldnames=get_fieldnames(tableName))
        
        # only write header once
        # this helped (https://9to5answer.com/python-csv-writing-headers-only-once)
        if f.tell() == 0:
            w.writeheader()
        
        w.writerow(dataDict)
        
        
def df_to_csv(df=None, 
              outputDir=None, 
              tableName=None):
    
    # set default values 
    outputDir = '.' if outputDir is None else outputDir
    tableName = 'Output' if tableName is None else tableName
    
    # ensure correct type
    assert isinstance(df, pd.core.frame.DataFrame), f"df must be a DataFrame"
    assert isinstance(outputDir, str), f"outputDir must a be string"
    assert isinstance(tableName, str), f"tableName must be a string"
    
    # remove a trailing "/" in the path
    out_dir = Path(outputDir).as_posix()
    
    # create out_dir if does not exist
    make_dir(path=out_dir)
    
    df.to_csv(f'{out_dir}/{tableName}.csv', index=False)

def df_wide_to_long(df=None, 
                    value_name=None, 
                    var_name=None):
    
    # set default values (new column names)
    var_name = "year" if var_name is None else var_name          # new column name with {value_vars}
    value_name = "values" if value_name is None else value_name  # new column name with values
    
    # ensure correct type
    assert isinstance(df, pd.core.frame.DataFrame), f"df must be a DataFrame"
    assert isinstance(var_name, str), f"var_name must a be string"
    assert isinstance(value_name, str), f"value_name must be a string"
    
    # ensure column names are strings
    df.columns = df.columns.astype(str)
    
    # columns to use as identifiers (columns that are not number)
    id_vars = [val for val in list(df.columns) if not val.isdigit()]

    # columns to unpivot (columns that are numbers)
    value_vars = [val for val in list(df.columns) if val.isdigit()]

    # Unpivot (melt) a DataFrame from wide to long format
    df_long = df.melt(id_vars = id_vars, 
                            value_vars = value_vars,
                            var_name = var_name, 
                            value_name = value_name)
    
    # convert var_name column to int
    df_long[var_name] = df_long[var_name].astype(int)
    
    return df_long

def find_regex_in_csv(fl=None, 
                      regex=None):
    """
    example:
    find_regex_in_csv(regex='Swaziland')
    """
    if fl is None:
        fl = '/Users/luke/Documents/work/data/ClimActor/country_dict_August2020.csv'

    # ensure correct type
    assert isinstance(fl, str), f"fl must a be string"
    assert isinstance(regex, str), f"regex must be a string"

    # return first first element with name
    with open(fl, "r") as file:
        reader = csv.reader(file)
        for line in reader:
            if re.search(regex, ','.join(line)):
                matched_line = line
                return line
                break
                

# TODO: separate these into conversion file

def gigagram_to_metric_ton(val):
    ''' 1 gigagram = 1000 tonnes  '''
    return val * 1000


# TODO: separate the functions below into a separate file specifically for data readers

def read_iso_codes(fl=None):
    ''' read iso codes
    this reads iso codes from web into dataframe
    with columns ['country', 'country_french', 'iso2', 'iso3']

    input
    -----
    fl: path to file

    output
    ------
    df: dataframe with iso codes

    source:
    -----
    https://raw.githubusercontent.com/Open-Earth-Foundation/OpenClimate-ISO-3166/main/ISO-3166-1.csv
    '''

    # TODO provide name harmonized name
    
    if fl is None:
        fl = 'https://raw.githubusercontent.com/Open-Earth-Foundation/OpenClimate-ISO-3166/main/ISO-3166-1.csv'

    # keep_deault_na=False is required so the Alpha-2 code "NA"
    # is parsed as a string and not converted to NaN
    df = pd.read_csv(fl, keep_default_na=False)

    # rename columns
    df = df.rename(columns={'English short name':'country', 
                        'French short name':'country_french',
                        'Alpha-2 code':'iso2',
                        'Alpha-3 code': 'iso3'})

    # only keep needed columns
    df = df[['country', 'country_french', 'iso2', 'iso3']]

    return df


def get_climactor_country():
    # open climactor
    #fl = 'https://raw.githubusercontent.com/datadrivenenvirolab/ClimActor/master/data-raw/country_dict_August2020.csv'
    fl = '/Users/luke/Documents/work/data/ClimActor/country_dict_updated.csv'
    df = pd.read_csv(fl)
    df['right'] = df['right'].str.strip()
    df['wrong'] = df['wrong'].str.strip()
    return df



def check_all_names_match(df=None, column=None):
    
    # default value
    column = 'country' if column is None else column
    
    # get climActor names
    df_climactor = get_climactor_country()

    # sanity check
    filt = df[column].isin(list(df_climactor['right']))
    assert len(df.loc[filt]) == len(df)
    
    
    
def name_harmonize_iso():
    # name harmonize
    from utils import read_iso_codes
    #df_iso = read_iso_codes()
    # keep_default_na=False ensure ISO code NA is parsed
    df_iso = pd.read_csv('/Users/luke/Documents/work/projects/OpenClimate-ISO-3166/ISO-3166-1/Actor.csv',
                         keep_default_na=False)

    df_climactor = get_climactor_country()

    #len(df_iso)

    #column = 'name'
    #filt = df_iso[column].isin(list(df_climactor['right']))
    #len(df_iso.loc[filt])

    #set(df_iso['name']) - set(df_iso.loc[filt, 'name'])

    # name harmonize country column
    df_iso['name'] = df_iso['name'].replace(to_replace = list(df_climactor['wrong']), 
                                            value = list(df_climactor['right']))

    #column = 'name'
    #filt = df_iso[column].isin(list(df_climactor['right']))
    #len(df_iso.loc[filt])

    #set(df_iso['name']) - set(df_iso.loc[filt, 'name'])
    
    return df_iso




def read_primap(fl=None):
    ''' read primap from web

    this reads the PRIMAP data from the zenoodo server.

    input
    ------
    fl: path to file

    output
    ------
    pandas dataframe

    source:
    -------
    https://zenodo.org/record/5494497

    Datasets on zenodo server
    - https://zenodo.org/record/5494497/files/Guetschow-et-al-2021-PRIMAP-hist_v2.3.1_20-Sep_2021.csv
    - https://zenodo.org/record/5494497/files/Guetschow-et-al-2021-PRIMAP-hist_v2.3.1_no_extrap_20-Sep_2021.csv
    - https://zenodo.org/record/5494497/files/Guetschow-et-al-2021-PRIMAP-hist_v2.3.1_no_extrap_no_rounding_20-Sep_2021.csv
    - https://zenodo.org/record/5494497/files/Guetschow-et-al-2021-PRIMAP-hist_v2.3.1_no_rounding_20-Sep_2021.csv
    - https://zenodo.org/record/5494497/files/PRIMAP-hist_v2.3.1_data-description.pdf
    - https://zenodo.org/record/5494497/files/PRIMAP-hist_v2.3.1_updated_figures.pdf

    '''
    # set default path
    if fl is None:
        fl = "https://zenodo.org/record/5494497/files/Guetschow-et-al-2021-PRIMAP-hist_v2.3.1_no_extrap_20-Sep_2021.csv"

    # read as pandas dataframe
    df = pd.read_csv(fl)

    return df

# formerly filter primap
def subset_primap(df=None, entity=None, category=None, scenario=None):
    '''filter primap dataset

    input
    -----
    df: primap dataframe
    entity: Gas categories using global warming potentials (GWP)
            from either Second Assessment Report (SAR) or Fourth Assessment Report (AR4).
            (see table in notes) [default: CO2]
    category: IPCC (Intergovernmental Panel on Climate Change) 2006 categories for emissions.
            (see table in notes) [default: M.0.EL]
    scenario: HISTCR or HISTTP [default: HISTCR]
        - HISTCR: In this scenario country-reported data (CRF, BUR, UNFCCC)
                  is prioritized over third-party data (CDIAC, FAO, Andrew, EDGAR, BP).
        - HISTTP: In this scenario third-party data (CDIAC, FAO, Andrew, EDGAR, BP)
                  is prioritized over country-reported data (CRF, BUR, UNFCCC)

    output
    ------
    filtered dataframe

    notes:
    -----

    -----------------------------------------------------------------------
    Entity_code            Description
    -----------------      ------------------------------------------------
    CH4                    Methane
    CO2                    Carbon Dioxide
    N2O                    Nitrous Oxide
    HFCS (SARGWP100)       Hydrofluorocarbons (SAR)
    HFCS (AR4GWP100)       Hydrofluorocarbons (AR4)
    PFCS SARGWP100         Perfluorocarbons (SAR)
    PFCS (AR4GWP100)       Perfluorocarbons (AR4)
    SF6                    Sulfur Hexafluoride
    NF3                    Nitrogen Trifluoride
    FGASES SARGWP100       Fluorinated Gases (SAR): HFCs, PFCs, SF$_6$, NF$_3$
    FGASES (AR4GWP100)     Fluorinated Gases (AR4): HFCs, PFCs, SF$_6$, NF$_3$
    KYOTOGHG SARGWP100     Kyoto greenhouse gases (SAR)
    KYOTOGHG (AR4GWP100)   Kyoto greenhouse gases (AR4)


    -----------------------------------------------------------------------
    Category_code Description
    ---------     -----------------------------------------------------------
    M.0.EL        National Total excluding LULUCF
    1             Energy
    1.A           Fuel Combustion Activities
    1.B           Fugitive Emissions from Fuels
    1.B.1         Solid Fuels
    1.B.2         Oil and Natural Gas
    1.B.3         Other Emissions from Energy Production
    1.C           Carbon Dioxide Transport and Storage
                  (currently no data available)
    2             Industrial Processes and Product Use (IPPU)
    2.  A         Mineral Industry
    2.B           Chemical Industry
    2.C           Metal Industry
    2.D           Non-Energy Products from Fuels and Solvent Use
    2.E           Electronics Industry
                  (no data available as the category is only used for
                  fluorinated gases which are only resolved at the level
                  of category IPC2)
    2 F           Product uses as Substitutes for Ozone Depleting Substances
                  (no data available as the category is only used for
                  fluorinated gases which are only resolved at the level
                  of category IPC2)
    2.G           Other Product Manufacture and Use
    2.H           Other
    M.AG          Agriculture, sum of IPC3A and IPCMAGELV
    3.A           Livestock
    M.AG.ELV      Agriculture excluding Livestock
    4             Waste
    5             Other
    -----------------------------------------------------------------------


    ----------------------------------------------------------------------
    code       Region_Description
    ---------  -----------------------------------------------------------
    EARTH      Aggregated emissions for all countries.
    ANNEXI     Annex I Parties to the Convention
    NONANNEXI  Non-Annex I Parties to the Convention
    AOSIS      Alliance of Small Island States
    BASIC      BASIC countries (Brazil, South Africa, India and China)
    EU27BX     European Union post Brexit
    LDC        Least Developed Countries
    UMBRELLA   Umbrella Group
    '''

    # set default values
    entity = 'CO2' if entity is None else entity
    category = 'M.0.EL' if category is None else category
    scenario = 'HISTCR' if scenario is None else scenario

    # filtering criteria
    filt = (
        (df['entity'] == entity) &
        (df['category (IPCC2006_PRIMAP)'] == category) &
        (df['scenario (PRIMAP-hist)'] == scenario)
    )

    # filtered dataset
    return df.loc[filt]


def filter_primap(df=None, identifier=None, emissions=None):

    identifier = 'identifier' if identifier is None else identifier
    emissions = 'emissions' if emissions is None else emissions
    
    # drop PRIMAP specific ISO codes and ANT
    # ANT = The Netherlands Antilles
    # which dissolved on October 10, 2010
    isoCodesToDrop = [
        'EARTH',
        'ANNEXI',
        'NONANNEXI',
        'AOSIS',
        'BASIC',
        'EU27BX',
        'LDC',
        'UMBRELLA',
        'ANT',  
    ]

    # filtered dataset
    filt = ~ df[identifier].isin(isoCodesToDrop)
    df = df.loc[filt]

    # filter where total emissions NaN
    filt = ~df[emissions].isna()
    df = df.loc[filt]
    return df


def remove_country_groups(df, column=None):
    """This needs to happen after you remove whitespace from names"""

    # set column name
    column = 'country' if column is None else column

    country_groups = [
        'ASEAN-5',
        'Australia and New Zealand',
        'Advanced economies',
        'Africa (Region)',
        'Asia and Pacific',
        'Caribbean',
        'Central America',
        'Central Asia and the Caucasus',
        'East Asia',
        'Eastern Europe',
        'Emerging and Developing Asia',
        'Emerging and Developing Europe',
        'Emerging market and developing economies',
        'Euro area',
        'Europe',
        'European Union',
        'Latin America and the Caribbean',
        'Major advanced economies (G7)',
        'Middle East (Region)',
        'Middle East and Central Asia',
        'North Africa',
        'North America',
        'Other advanced economies',
        'Pacific Islands',
        'South America',
        'South Asia',
        'Southeast Asia',
        'Sub-Saharan Africa',
        'Sub-Saharan Africa (Region)',
        'West Bank and Gaza',
        'Western Europe',
        'Western Hemisphere (Region)',
        'World',
    ]

    # remove country_groups
    filt = ~df[column].isin(country_groups) 
    df = df.loc[filt]
    return df

    
# TODO: separate into primap specific file (?)
def harmonize_primap_emissions(outputDir=None, 
                               tableName=None,
                               methodologyDict=None, 
                               datasourceDict=None):
    '''harmonize primap dataset

    haramonize primap to conform to open cliamte schema

    input
    ------
    outputDir: directory where table will be created
    tableName: name of the table to create
    methodologyDict: dictionary with methodology info
    datasourceDict: dictionary with datasource info

    output
    -------
    df: final dataframe with emissions info
    '''
    
    # ensure input types are correct
    assert isinstance(outputDir, str), f"outputDir must a be string"
    assert isinstance(tableName, str), f"tableName must be a string"
    assert isinstance(methodologyDict, dict), f"methodologyDict must be a dictionary"
    assert isinstance(datasourceDict, dict), f"datasourceDict must be a dictionary"
    
    # TODO add section to ensure methodologyDict and datasourceDict have correct keys
    
    # output directory
    out_dir = Path(outputDir).as_posix()
    
    # create out_dir if does not exist
    make_dir(path=out_dir)
    
    # read iso
    df_iso = read_iso_codes()
    
    # read subset of primap
    df_pri_tmp = read_primap()
    df_pri = subset_primap(df_pri_tmp)

    # merge datasets
    df_merged = pd.merge(df_pri, df_iso, 
                         left_on=['area (ISO3)'], 
                         right_on=["iso3"], 
                         how="left")
    
    # convert from wide to long dataframe
    df_long = df_wide_to_long(df=df_merged,
                              value_name="emissions",
                              var_name="year")

    # filter un-necessary ISO codes and where emissions ana (removes 251 records)
    df = filter_primap(df=df_long, identifier="iso3", emissions="emissions")
    
    # rename columns
    df = df.rename(columns={'iso3': 'actor_id'})

    def gigagram_to_metric_ton(val):
        ''' 1 gigagram = 1000 tonnes  '''
        return val * 1000
    
    # create id columns
    df['datasource_id'] = datasourceDict['datasource_id']
    df['methodology_id'] = methodologyDict['methodology_id']
    df['emissions_id'] = df.apply(lambda row: 
                                  f"{row['source']}:{row['actor_id']}:{row['year']}", 
                                  axis=1)

    # convert emissions to metric tons
    df['total_emissions'] = df['emissions'].apply(gigagram_to_metric_ton)

    # Create EmissionsAgg table
    emissionsAggColumns = ["emissions_id",
                           "actor_id",
                           "year",
                           "total_emissions",
                           "methodology_id",
                           "datasource_id"]

    df_emissionsAgg = df[emissionsAggColumns]

    # ensure columns have correct types
    df_emissionsAgg = df_emissionsAgg.astype({'emissions_id': str,
                                              'actor_id': str,
                                              'year': int,
                                              'total_emissions': int,
                                              'methodology_id': str,
                                              'datasource_id': str})

    # sort by actor_id and year
    df_emissionsAgg = df_emissionsAgg.sort_values(by=['actor_id', 'year'])

    # convert to csv
    df_emissionsAgg.to_csv(f'{out_dir}/{tableName}.csv', index=False)

    return df


def harmonize_imf_gdp(outputDir=None, 
                      tableName=None,
                      datasourceDict=None):
    
    # ensure input types are correct
    assert isinstance(outputDir, str), f"outputDir must a be string"
    assert isinstance(tableName, str), f"tableName must be a string"
    assert isinstance(datasourceDict, dict), f"datasourceDict must be a dictionary"
    
    # TODO add section to ensure methodologyDict and datasourceDict have correct keys
    
    # output directory
    out_dir = Path(outputDir).as_posix()
    
    # create out_dir if does not exist
    make_dir(path=out_dir)
    
    # read dataset
    workbook = xlrd.open_workbook_xls('/Users/luke/Documents/work/data/GDP/country/imf-dm-export-20221017.xls', 
                                      ignore_workbook_corruption=True)  
    df_gdp_tmp = pd.read_excel(workbook)

    # open climactor and isocode dataset 
    df_climactor = get_climactor_country()
    df_iso = name_harmonize_iso()

    # rename column
    df_gdp_tmp = df_gdp_tmp.rename(columns={"GDP, current prices (Billions of U.S. dollars)":"country"})

    # filter out NaN country names and IMF lines
    filt = (
        ~(df_gdp_tmp['country'].isna()) &
        ~(df_gdp_tmp['country'] == '©IMF, 2022')
    )

    df_gdp_tmp = df_gdp_tmp.loc[filt]

    # avoids a SettingWithCopyWarning
    df_tmp = df_gdp_tmp.copy()

    # remove trailing white space
    df_tmp['country'] = df_tmp['country'].str.strip()
    df_out = df_tmp.copy()
    df_out = remove_country_groups(df_out, column='country')

    # name harmonize country column
    df_out['country_harmonized'] = (
        df_out['country']
        .replace(to_replace = list(df_climactor['wrong']),
                 value = list(df_climactor['right']))
    )

    # sanity check that names match
    check_all_names_match(df_out, 'country_harmonized')

    # unpivot the dataset wide to long
    df_long = df_wide_to_long(df=df_out, value_name='GDP')

    # remove any records with no GDP data
    filt = ~(df_long['GDP'] == 'no data')
    df_long = df_long.loc[filt]

    # convert to float
    df_long['GDP'] = df_long['GDP'].astype(float)

    # convert GDP to USD instead of billion USD
    df_long['GDP'] = df_long['GDP'] * 10**9

    # filter years
    filt = (df_long['year'] <= 2021)
    df_long = df_long.loc[filt]

    # change type
    df_long['GDP'] = df_long['GDP'].astype(int)

    # merge ISO codes into dataframe to get actor_id
    df_out = pd.merge(df_long, df_iso, left_on=["country_harmonized"], right_on=["name"], how="left")

    # filter out Kosovo (not in our emission or pledge databases)
    filt = (df_out['country_harmonized'] != 'Kosovo')
    df_out = df_out.loc[filt]

    # rename GDP to lowercase 
    df_out = df_out.rename(columns={'GDP':'gdp'})

    # set datasource ID
    df_out['datasource_id'] = datasourceDict['datasource_id']

    # create final dataframe
    columns = ['actor_id', 'gdp', 'year', 'datasource_id']
    df_out = df_out[columns]

    # ensure types are correct
    df_out = df_out.astype({
        'actor_id': str,
        'gdp': int,
        'year': int,
        'datasource_id': str
    })

    # sort dataframe and save
    df_out = df_out.sort_values(by=['actor_id', 'year'])

    # convert to csv
    df_out.to_csv(f'{out_dir}/{tableName}.csv', index=False)
    
    return df_out


