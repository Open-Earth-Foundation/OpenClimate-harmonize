# DDL - Zhi Yi Yeo
# Script to remap raw CDP targets data to data model format
library(ClimActor)
library(tidyverse)


# Read in raw data 
x <- read.csv("Raw/2021_Cities_Emissions_Reduction_Targets_clean_names.csv",
              stringsAsFactors = F, encoding = "UTF-8")
ActorIdentifier <- x %>%
  select(Account.Number) %>% 
  rename("identifier" = Account.Number) %>%
  mutate(namespace = "CDP Cities")

Actor <- x %>% 
  select(name, entity_type, iso) %>%
  rename("type" = entity_type)

Target <- x %>%
  select(Type.of.target, Base.year, Percentage.reduction.target, Target.year,
         Intensity.unit..Emissions.per., Estimated.business.as.usual.absolute.emissions.in.target.year..metric.tonnes.CO2e.,
         Percentage.of.target.achieved.so.far, Last.update) %>%
  rename("target_type" = Type.of.target,
         "baseline_year" = Base.year,
         "target_value" = Percentage.reduction.target,
         "target_year" = Target.year,
         "target_unit"= Intensity.unit..Emissions.per.,
         "bau_value" = Estimated.business.as.usual.absolute.emissions.in.target.year..metric.tonnes.CO2e.,
         "percent_achieved" = Percentage.of.target.achieved.so.far,
         "last_updated" = Last.update) %>%
  mutate(data_source = "CDPCities2021Targets")

EmissionsAgg <- x %>%
  select(Base.year.emissions..metric.tonnes.CO2e., Base.year, Last.update) %>%
  rename("total_emissions" = Base.year.emissions..metric.tonnes.CO2e.,
         "year" = Base.year, 
         "last_updated" = Last.update) %>%
  mutate(data_source = "CDPCities2021Targets")

TerritoryContext <- x %>%
  select(Population, Population.Year, City.Location) %>%
  mutate(lat = gsub("POINT \\((.*) (.*)\\)", "\\2", City.Location), 
         lng = gsub("POINT \\((.*) (.*)\\)", "\\1", City.Location)) %>%
  rename("population" = Population,
         "population_year" = Population.Year)

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
