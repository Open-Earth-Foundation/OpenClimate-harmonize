# OpenClimate-harmonize

this is the start of a package to harmonize DIGS datasources for ingestion into the OpenClimate schema

## Harmonize PRIMAP

`harmonize_primap()` is a top-level function that loads PRIMAP and ISO code data,
harmonizes it with the OpenClimate schema, and creates three files in the working directory 
(`DataSource.csv`, `EmissionsAgg.csv`,  `Methodology.csv`, and `Publisher.csv`). EmissionsAgg is country total emissions in metric tons. 

```python
if __name__ == "__main__":
    from utils import harmonize_primap 
    df = harmonize_primap()
```

Harmonized data for each data source is in the `/data_harmonized` directory