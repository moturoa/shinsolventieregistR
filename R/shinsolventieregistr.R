#' Make a database connection
#' @description Make a connection with RPostgres to one of the Shinto Labs databases.
#' @param what Welke database connectie in het config bestand? (bv. "BAGdata")
#' @param file Lokatie van het config bestand, normaal gezet via `options(shintobag_conf = /path/to/file)`
#' @param pool Logical. Als TRUE, gebruikt `pool::dbPool`, anders `DBI::dbConnect`
#' @export
shinto_db_connection <- function(what,
                                 file = getOption("shinsolventie_conf", "conf/config.yml"),
                                 port = 5432,
                                 pool = FALSE){

  conf <- config::get(what, file = file, config = "default")

  if(!pool){
    DBI::dbConnect(RPostgres::Postgres(),
                   dbname = conf$dbname,
                   host = conf$dbhost,
                   port = port,
                   user = conf$dbuser,
                   password = conf$dbpassword,
                   options="-c search_path=register"
    )
  } else {
    pool::dbPool(RPostgres::Postgres(),
                 dbname = conf$dbname,
                 host = conf$dbhost,
                 port = port,
                 user = conf$dbuser,
                 password = conf$dbpassword
    )
  }

}

#' Make an unnested dataframe
#' @description Create a new dataframe in which the adresses are unnested, such that there is a row for each adress
#' @param rownr Het rijnummer van het databestand wat wordt verwerkt
#' @param plaatsnaam Plaatsnaam waarvoor gezocht wordt
#' @param data Een dataframe met json objecten die uit de database zijn gehaald
#' @return een dataframe met unnested adressen
#' @export
get_unnested_df_by_place <- function(rownr, plaatsnaam, data){
  newdf <- data.frame(insolventie_nummer = character(),
                      kvk_nummer = integer(),
                      bedrijfsnaam = character(),
                      publicate_datum = character(),
                      straat = character(),
                      huisnr = integer(),
                      huisnrtoevoeging = character(),
                      plaats = character(),
                      postcode = character(),
                      geheim_adres = character(),
                      adres_type = character(),
                      verwijderd = character(),
                      stringsAsFactors = FALSE)
  if(nrow(data)>0){
    row <- data[rownr,]
    adressen <- data.frame(jsonlite::stream_in(textConnection(row$adressen)))
    pubdat <- jsonlite::stream_in(textConnection(row$recente_publicatie))
    for (i in 1:length(adressen)){
      cur_naam <- adressen[[glue("adres{i}")]][["Plaats"]]
      if(cur_naam == plaatsnaam){
        insolventie_nummer = as.character(row$insolventie_nummer)
        kvk_nummer <- as.integer(row$kvk_nummer)
        bedrijfsnaam <- as.character(row$bedrijfs_naam)
        publicatie_datum <- as.Date(pubdat$PublicatieDatum)
        straat <- as.character(adressen[[glue("adres{i}")]][["Straat"]])
        huisnummer <- as.integer(adressen[[glue("adres{i}")]][["HuisNummer"]])
        huisnummertoevoeging = as.character(adressen[[glue("adres{i}")]][["HuisNummerToevoegingen"]])
        woonplaats <- as.character(adressen[[glue("adres{i}")]][["Plaats"]])
        postcode <- as.character(adressen[[glue("adres{i}")]][["Postcode"]])
        geheim_adres <- as.logical(adressen[[glue("adres{i}")]][["GeheimAdres"]])
        adres_type <- as.character(adressen[[glue("adres{i}")]][["AdresType"]])
        verwijderd <- as.logical(row$verwijderd)
        rowdf <- data.frame(insolventie_nummer,kvk_nummer,bedrijfsnaam,publicatie_datum,straat,huisnummer,huisnummertoevoeging,woonplaats,postcode,geheim_adres,adres_type,verwijderd, stringsAsFactors = FALSE)
        newdf <- rbind(newdf,rowdf)
      }
    }
  }
  return(newdf)
}

#' Search for companies in the register in a certain place
#' @description Look for all the companies in the register based on the name of a city
#' @param plaatsnaam Plaatsnaam waarvoor gezocht wordt
#' @return een dataframe met kolommen die niet nested zijn
#' @export
shinsolventieregistr <- function(plaatsnaam){
  con <- shinto_db_connection("insolventie_register")

  out <- dbGetQuery(con, glue("SELECT i.insolventie_nummer, i.kvk_nummer, i.recente_publicatie, i.adressen, i.verwijderd, i.bedrijfs_naam  FROM insolventies i WHERE EXISTS (SELECT value FROM json_each(i.adressen) a WHERE a.value->>'Plaats' = '{plaatsnaam}') AND i.kvk_nummer IS NOT NULL"))
  data <- data.frame(out)

  df_list <- lapply(1:nrow(data), function(r_num) {get_unnested_df_by_place(r_num, plaatsnaam, data)})
  df_out_lapply <- do.call(rbind, df_list)
  df_out_lapply

  return(df_out_lapply)
}
