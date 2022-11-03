# DDL - Zhi Yi Yeo
# Script to remap raw CDP targets data to data model format
library(ClimActor)
library(tidyverse)


# Read in raw data 
x <- read.csv("Raw/2021_Cities_Emissions_Reduction_Targets_clean_names.csv",
              stringsAsFactors = F, encoding = "UTF-8")
# Map UN Locodes - use the ingested version in OC github
locodes <- read.csv("../../../../OpenClimate-UNLOCODE/UNLOCODE/ActorName.csv",
                    stringsAsFactors = F, fileEncoding = "UTF-8")
kd_lc <- read.csv("../../../key_dict_LOCODE_to_climactor.csv",
                  stringsAsFactors = F, fileEncoding = "UTF-8")
# Quick check
length(unique(x$name))
sum(unique(x$name) %in% locodes$name)
sum(unique(x$name) %in% kd_lc$right)
sum(unique(x$name) %in% kd_lc$right)
sum(unique(paste0(x$name, x$iso)) %in% paste0(kd_lc$right, kd_lc$iso))
any(is.na(kd_lc$locode))

# Check if we can get more copies by using the key dict
all_name <- key_dict[which(key_dict$right %in% x$name), ]
length(unique(all_name$right))
length(unique(all_name$right[(which(all_name$wrong %in% locodes$name))]))

# Can get about 600+ matches by using key dict. Probably just manually clean the rest?

# Change data to OC schema first and incorporate updates from before

datasource_id <- "CDPCitiesTargets:2021"

ActorIdentifier <- x %>%
  select(Account.Number) %>% 
  rename("identifier" = Account.Number) %>%
  mutate(namespace = "CDP")

Actor <- x %>% 
  select(name, entity_type, iso) %>%
  rename("type" = entity_type)

Actor2 <- Actor %>% left_join(kd_lc %>% select(right, iso, locode), by = c("name" = "right", 
                                  "iso" = "iso"))
# Export NAs for gapfilling 
missing_locode <- Actor2 %>% 
  filter(is.na(locode))
which(paste0(missing_locode$name, missing_locode$iso) %in% paste0(locodes$name, locodes$iso3))

missing_locode$locode <- locodes$actor_id[match(paste0(missing_locode$name, missing_locode$iso), paste0(locodes$name, locodes$iso3))]
# missing_locode %>% filter(is.na(locode)) %>%
  # write.csv("missing_locodes2.csv", row.names = F, fileEncoding = "UTF-8")

# Check for partial matches and focus search on those with partial searches 
# Export for manual checks 
# write.csv(missing_locode, "missing_locode_gapfilling.csv",
#           row.names = F, fileEncoding = "UTF-8")


Target <- x %>%
  select(Type.of.target, Base.year, Percentage.reduction.target, Target.year, Sector, 
         Target.boundary.relative.to.city.boundary,
         Intensity.unit..Emissions.per., Estimated.business.as.usual.absolute.emissions.in.target.year..metric.tonnes.CO2e.,
         Percentage.of.target.achieved.so.far, Last.update) %>%
  rename("target_type" = Type.of.target,
         "baseline_year" = Base.year,
         "target_value" = Percentage.reduction.target,
         "sector_list" = Sector,
         "target_year" = Target.year,
         "target_unit"= "%",
         "target_boundary" = Target.boundary.relative.to.city.boundary,
         "bau_value" = Estimated.business.as.usual.absolute.emissions.in.target.year..metric.tonnes.CO2e.,
         "percent_achieved" = Percentage.of.target.achieved.so.far,
         "last_updated" = Last.update) %>%
  mutate(data_source = datasource_id)

EmissionsAgg <- x %>%
  select(Base.year.emissions..metric.tonnes.CO2e., Base.year, Last.update,
         Target.year.absolute.emissions..metric.tonnes.CO2e., Target.year) %>%
  rename("total_emissions" = Base.year.emissions..metric.tonnes.CO2e.,
         "year" = Base.year, 
         "last_updated" = Last.update) %>%
  mutate(data_source = datasource_id)

Territory <- x %>%
  select(City.Location) %>%
  mutate(lat = gsub("POINT \\((.*) (.*)\\)", "\\2", City.Location), 
         lng = gsub("POINT \\((.*) (.*)\\)", "\\1", City.Location))
  
Population <- x %>%
  select(Population, Population.Year) %>%
  rename("population" = Population,
         "population_year" = Population.Year)

Publisher <- data.frame(id = "CDP", name = "CDP",
                        url = "https://www.cdp.net/en")

DataSource <- data.frame(datasource_id = datasource_id, name = "CDP Cities Targets 2021",
                         publisher = "CDP", published = "2021", 
                         URL = "https://data.cdp.net/Mitigation-Actions/2021-Cities-Emissions-Reduction-Targets/vevx-e5s3")


write.csv(Actor, "Harmonized/Actor.csv", 
          row.names = F, fileEncoding = "UTF-8")
write.csv(ActorIdentifier, "Harmonized/ActorIdentifier.csv", 
          row.names = F, fileEncoding = "UTF-8")
write.csv(EmissionsAgg, "Harmonized/EmissionsAgg.csv", 
          row.names = F, fileEncoding = "UTF-8")
write.csv(Target, "Harmonized/Target.csv", 
          row.names = F, fileEncoding = "UTF-8")
write.csv(TerritoryContext, "Harmonized/TerritoryContext.csv", 
          row.names = F, fileEncoding = "UTF-8")
