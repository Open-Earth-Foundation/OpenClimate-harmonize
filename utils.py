import pandas as pd
import csv


def read_iso_codes(fl=None):
    ''' read iso codes 
    this reads iso codes from web into dataframe

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

    if fl is None:
        fl = 'https://raw.githubusercontent.com/Open-Earth-Foundation/OpenClimate-ISO-3166/main/ISO-3166-1.csv'

    # keep_deault_na=False is required so the Alpha-2 code "NA"
    # is parsed as a string and not converted to NaN
    df = pd.read_csv(fl, keep_default_na=False)

    return df


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
        fl = "https://zenodo.org/record/5494497/files/Guetschow-et-al-2021-PRIMAP-hist_v2.3.1_20-Sep_2021.csv"

    # read as pandas dataframe
    df = pd.read_csv(fl)

    return df


def filter_primap(df=None, entity=None, category=None, scenario=None):
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


# TODO: this needs to be generalized

def create_publisher_csv():
    ''' create publisher csv for primap '''
    publisherDict = {
        'id': 'PRIMAP',
        'name': 'Potsdam Realtime Integrated Model for probabilistic Assessment of emissions Path',
        'URL': 'https://zenodo.org/record/5494497'
    }

    with open('./Publisher.csv', 'w') as f:  # You will need 'wb' mode in Python 2.x
        w = csv.DictWriter(f, publisherDict.keys())
        w.writeheader()
        w.writerow(publisherDict)


# TODO: this needs to be generalized

def create_methodology_csv():
    ''' create methodology csv for primap '''
    publisher = 'PRIMAP'
    doi = '10.5281/zenodo.5494497'
    version = 'v2.3.1'

    methodologyDict = {
        "methodology_id": f'{publisher}:{doi}:{version}:methodology',
        "name": 'PRIMAP methodology based on a compliation of multiple publicly available data sources',
        "methodology_link": 'https://essd.copernicus.org/articles/8/571/2016/'
    }

    with open('./Methodology.csv', 'w') as f:  # You will need 'wb' mode in Python 2.x
        w = csv.DictWriter(f, methodologyDict.keys())
        w.writeheader()
        w.writerow(methodologyDict)


def create_datasource_csv():
    ''' create datasource csv for primap '''
    datasourceDict = {
        'datasource_id': 'PRIMAP:10.5281/zenodo.5494497:v2.3.1',
        'name': 'PRIMAP-hist_v2.3.1_ne (scenario=HISTCR)',
        'publisher': 'PRIMAP',
        'published': '2021-09-22',
        'URL': 'https://zenodo.org/record/5494497'
    }

    with open('./DataSource.csv', 'w') as f:  # You will need 'wb' mode in Python 2.x
        w = csv.DictWriter(f, datasourceDict.keys())
        w.writeheader()
        w.writerow(datasourceDict)


def harmonize_primap():
    '''harmonize primap dataset

    haramonize primap to conform to open cliamte schema

    input
    ------
    None ... yet

    output
    -------
    merged dataframe 

    '''
    # read datasets
    df_iso = read_iso_codes()
    df_pri_tmp = read_primap()

    # filter premap
    df_pri = filter_primap(df_pri_tmp)

    # rename ISO3 name in PRIMAP so same as ISO dataset
    df_pri = df_pri.rename(columns={"area (ISO3)": "Alpha-3 code"})

    # merge datasets (wide, each year is a column)
    df_wide = pd.merge(df_pri, df_iso, on=["Alpha-3 code"], how="left")

    # --------------------------------------------------------
    # This section unpivots the dataframe from wide to long
    # so each year is not a separate column
    # --------------------------------------------------------
    # columns to use as identifiers
    id_vars = [val for val in list(df_wide.columns) if not val.isdigit()]

    # columns to unpivot
    value_vars = [val for val in list(df_wide.columns) if val.isdigit()]
    var_name = "year"     # new column name with {value_vars}
    value_name = "emissions"  # new column name with values

    # Unpivot (melt) a DataFrame from wide to long format
    df_merged_long = df_wide.melt(id_vars=id_vars,
                                  value_vars=value_vars,
                                  var_name=var_name,
                                  value_name=value_name)

    # rename columns
    df = df_merged_long.rename(columns={'scenario (PRIMAP-hist)': 'scenario',
                                        'Alpha-3 code': 'identifier',
                                        'category (IPCC2006_PRIMAP)': 'category',
                                        'English short name': 'name',
                                        'French short name': 'name_french',
                                        'Alpha-2 code': 'actor_id'})

    # convert year to int
    df['year'] = df['year'].astype('int16')

    # trim time
    filt = (df['year'] >= 1850)
    df = df.loc[filt]

    # drop PRIMAP specific ISO codes
    # drop ANT ISO code as it's not in our database
    isoCodesToDrop = [
        'EARTH',
        'ANNEXI',
        'NONANNEXI',
        'AOSIS',
        'BASIC',
        'EU27BX',
        'LDC',
        'UMBRELLA',
        'ANT',  # this ISO code is not in our database
    ]

    # filtered dataset
    filt = ~ df['identifier'].isin(isoCodesToDrop)
    df = df.loc[filt]

    # filter where total emissions NaN
    filt = ~df['emissions'].isna()
    df = df.loc[filt]

    # TODO: make these functions not dependdent on particular columns and try not to use .apply()
    # and move them out from inside this function

    def create_emission_id(row):
        return f"{row['source']}:{row['actor_id']}:{row['year']}"

    def create_datasource_id(row, publisher, doi, version):
        return f"{publisher}:{doi}:{version}"

    def create_methodology_id(row, publisher, doi, version):
        datasource_id = create_datasource_id(row, publisher, doi, version)
        return f"{datasource_id}:methodology"

    def gigagram_to_metric_ton(val):
        ''' 1 gigagram = 1000 tonnes  '''
        return val * 1000

    df['emissions_id'] = df.apply(lambda row: create_emission_id(row), axis=1)

    df['datasource_id'] = df.apply(lambda row: create_datasource_id(row,
                                                                    'PRIMAP',
                                                                    '10.5281/zenodo.5494497',
                                                                    'v2.3.1'), axis=1)

    df['methodology_id'] = df.apply(lambda row: create_methodology_id(row,
                                                                      'PRIMAP',
                                                                      '10.5281/zenodo.5494497',
                                                                      'v2.3.1'), axis=1)

    df['total_emissions'] = df['emissions'].apply(gigagram_to_metric_ton)

    # Create EmissionsAgg table
    emissionsAggColumns = ["emissions_id",
                           "actor_id",
                           "year",
                           "total_emissions",
                           "methodology_id",
                           "datasource_id"]

    df_emissionsAgg = df[emissionsAggColumns]

    # ensure data has correct types
    df_emissionsAgg = df_emissionsAgg.astype({'emissions_id': str,
                                             'actor_id': str,
                                              'year': int,
                                              'total_emissions': int,
                                              'methodology_id': str,
                                              'datasource_id': str})

    # sort colmns
    df_emissionsAgg = df_emissionsAgg.sort_values(by=['actor_id', 'year'])

    # convert to csv
    df_emissionsAgg.to_csv('./EmissionsAgg.csv', index=False)

    # creates methodology and publisher and datasource csv files
    create_methodology_csv()
    create_publisher_csv()
    create_datasource_csv()

    return df
