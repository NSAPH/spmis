##----- Install Postgres and PostGIS with homebrew
## On OSX
## https://brew.sh/
#/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
# brew install postgres
# brew install postgis

##----- Start Postgres
# pg_ctl -D /usr/local/var/postgres start &
##----- Stop Postgres
# pg_ctl -D /usr/local/var/postgres stop &

library(DBI)
library(RPostgreSQL)
library(dplyr)
library(readr)
library(raster)

##----- 'Global' variables.  Use ABSOLUTE file paths

dbname <- "pm"
host <- "localhost"
port <- "5432"
user <- Sys.getenv("LOGNAME")
gridded_pm        <- "[...]/pm25_2000_ne.csv"
zipcode_shapefile <- "[...]/SDE_ESRIUSZIP_POLY.shp"
pobox_shapefile <- "[...]SDE_ESRIUSZIP_USA.shp"
pobox <- "[...]/PO_boxes.csv"

##----- Load utility GIS functions

source("functions.R")

##----- Create spatial database.  Create ONCE

create_spatial_db() # create "pm" database

##----- Load PM2.5 gridded data to database

pm25 <- read_csv(gridded_pm)
pm25$id <- NULL
names(pm25) <- c("lat", "lng", "pm")
copy_to_db_points(pm25, table_name = "pm25")

##----- Load USPS zip code shapefile to database

copy_zipshapefile_to_db(table_name = "zipcode")

##----- Link data

link_zip_pm(table_name = "link")
link <- get_table(table_name = "link")

##----- Link PO boxes

copy_pobox_to_db(file = pobox)
link_pobox_pm(table_name = "linkpo")
linkpo <- get_table(table_name = "linkpo")

##----- Restrict to New England

zip_ne <- subset(shapefile(zipcode_shapefile), STATE %in% c("ME", "CT", "MA", "NH", "RI", "VT"))
link_ne <- subset(link, zip %in% zip_ne$ZIP)

pobox_ne <- subset(shapefile(pobox_shapefile), STATE %in% c("ME", "CT", "MA", "NH", "RI", "VT"))
linkpo_ne <- subset(linkpo, zip %in% pobox_ne$ZIP)

##----- FINAL: zip areas and PO boxies

link_final_ne <- arrange(rbind(link_ne, linkpo_ne), zip)
write_csv(link_final_ne, "output_link_final_ne.csv")

##----- Clean database tables if needed

# remove_table("pm25")
# remove_table("zipcode")
# remove_table("pobox")
# remove_table("link")
# remove_table("linkpo")

##----- Remove database if needed

# system("dropdb pm")
