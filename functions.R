create_spatial_db <- function() {
  system(paste("createdb", dbname))
  db <- src_postgres(dbname = dbname, host = host, port = port, user = user)
  dbSendQuery(db$con, "CREATE EXTENSION POSTGIS")
  dbDisconnect(db$con)
}

copy_to_db_points <- function(table, table_name = NULL) {
  db <- src_postgres(dbname = dbname, host = host, port = port, user = user)
  if (is.null(table_name))
    table_name <- deparse(substitute(table))
  copy_to(dest = db, df = table, name = table_name, temporary = FALSE)
  dbGetQuery(db$con, paste("ALTER TABLE", table_name, "ADD COLUMN geom geometry(POINT, 4236);"))
  dbGetQuery(db$con, paste("UPDATE", table_name, "SET geom = ST_SetSRID(ST_MakePoint(lng, lat), 4236);"))
  dbGetQuery(db$con, paste("ALTER TABLE", table_name, "ALTER COLUMN geom TYPE geometry(Point, 2163) USING ST_Transform(geom, 2163);"))
  dbGetQuery(db$con, paste("CREATE INDEX gix ON", table_name, "USING GIST (geom);"))
  dbDisconnect(db$con)
}

copy_zipshapefile_to_db <- function(table_name = "zipcode") {
  cmd <- paste("shp2pgsql -c -D -I -s 4326", zipcode_shapefile, "zipcode | psql -d pm -h", host, "-U", user)
  system(cmd)
  db <- src_postgres(dbname = dbname, host = host, port = port, user = user)
  dbGetQuery(db$con, paste("ALTER TABLE", table_name, "ALTER COLUMN geom TYPE geometry(MultiPolygon, 2163) USING ST_Transform(geom, 2163);"))
  dbDisconnect(db$con)
}

copy_pobox_to_db <- function(file = pobox) {
  db <- src_postgres(dbname = dbname, host = host, port = port, user = user)
  cmd <- "CREATE TABLE pobox (gid serial NOT NULL, zip character varying, enc_zip character varying, state character varying, area character varying, po_name character varying, nametype character varying, cty1fips character varying, cty2fips character varying, cty3fips character varying, ropo_flag character varying, zip_type character varying, lng double precision, lat double precision);"
  dbGetQuery(db$con, cmd)
  cmd <- paste0("COPY pobox from '", pobox, "' DELIMITERS ',' CSV header;")
  dbGetQuery(db$con, cmd)
  cmd <- "ALTER TABLE pobox ADD COLUMN geom geometry (Point, 4326);"
  dbGetQuery(db$con, cmd)
  cmd <- "UPDATE pobox SET geom = ST_SetSRID(ST_MakePoint(lng, lat), 4326);"
  dbGetQuery(db$con, cmd)
  cmd <- "ALTER TABLE pobox ALTER COLUMN geom TYPE geometry(Point, 2163) USING ST_Transform(geom, 2163);"
  dbGetQuery(db$con, cmd)
  cmd <- "CREATE INDEX pobox_gix ON pobox USING GIST (geom);"
  dbGetQuery(db$con, cmd)
  dbDisconnect(db$con)
}

link_zip_pm <- function(table_name = "link") {
  db <- src_postgres(dbname = dbname, host = host, port = port, user = user)
  cmd <- paste("CREATE TABLE", table_name ,"AS SELECT sum(pm)/count(*) AS averagepm, zip FROM pm25, zipcode WHERE ST_DWithin(zipcode.geom, pm25.geom, 707.11) GROUP BY zipcode.gid;")
  dbGetQuery(db$con, cmd)
  dbDisconnect(db$con)
}

link_pobox_pm <- function(table_name = "linkpo") {
  db <- src_postgres(dbname = dbname, host = host, port = port, user = user)
  cmd <- paste("CREATE TABLE", table_name ,"AS SELECT sum(pm)/count(*) AS averagepm, pobox.zip FROM pm25, pobox where ST_DWithin(pobox.geom, pm25.geom, 707.11) GROUP BY pobox.zip;")
  dbGetQuery(db$con, cmd)
  dbDisconnect(db$con)
}

get_table <- function(table_name = "link", ...) {
  db <- src_postgres(dbname = dbname, host = host, port = port, user = user)
  d <- collect(tbl(db, table_name), ...)
  dbDisconnect(db$con)
  return(d)
}

remove_table <- function(table_name) {
  db <- src_postgres(dbname = dbname, host = host, port = port, user = user)
  dbSendQuery(db$con, paste("DROP TABLE",  table_name))
  dbDisconnect(db$con)
}

##----- NEW: State for each grid cell

get_grid_cell_state <- function(table_name = "grid_cell_state") {
  db <- src_postgres(dbname = dbname, host = host, port = port, user = user)
  cmd <- paste("CREATE TABLE", table_name , "AS SELECT zipcode.state, pm25.lat, pm25.lng FROM zipcode, pm25 WHERE ST_Intersects(zipcode.geom, pm25.geom);")
  dbGetQuery(db$con, cmd)
  dbDisconnect(db$con)
}

