library(tidyverse)
library(DBI)
library(dbplyr)
library(sf)


con <- dbConnect(RSQLite::SQLite(), "database/detroit.sqlite")

assessments2 <- read_csv('../detroit-data-warehouse/Processed Data/joined11_22tent.csv')
dbWriteTable(con, 'assessments', assessments2 %>% select(PARCELNO, propclass, ASSESSEDVALUE, TAXABLEVALUE, year))

sales <- read_csv('../detroit-data-warehouse/Processed Data/property_sales.csv')
dbWriteTable(con, 'sales', sales %>% select(parcel_num, sale_date, sale_price:grantee, sale_terms, ecf, property_c) %>%
               mutate(sale_date = as.character(sale_date)) %>% filter(str_sub(property_c, 1, 1) == '4'), overwrite=TRUE)

parcles21 <- sf::read_sf('../detroit-data-warehouse/Open Data/Parcels_2021.geojson')
coords <- parcles21 %>% st_centroid() %>% st_coordinates()

parcles21mini <- parcles21 %>% as.data.frame() %>% select(-geometry) %>%
  select(-legal_description) %>%
  mutate(X = coords[, 1],
         Y = coords[, 2])


dbWriteTable(con, 'parcels', parcles21mini, overwrite=TRUE)

history <- read_csv('../detroit-data-warehouse/Processed Data/joined09_15.csv')
dbWriteTable(con, 'parcels_historic', history %>% filter(tax_year == 2009))

blight <- read_csv('../../../Downloads/Blight_Violations.csv', col_types = 'cccccccccccccccccccccccccccccccccccc')
dbWriteTable(con, 'blight', blight %>% select(ticket_id, ticket_number, agency_name, violator_name:ticket_issued_time, violation_code:parcelno))

foreclosures <- read_csv('../../../Downloads/foreclosures_2002-2019__2020addresses.csv')
dbWriteTable(con, 'foreclosures', foreclosures)

dbExecute(con, 'vacuum')

dbDisconnect(con)


