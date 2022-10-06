if __name__ == "__main__":
    from utils import harmonize_primap
    df = harmonize_primap()
    df.to_csv('./primap_country_aggEmissions_harmonized_TEST.csv')
