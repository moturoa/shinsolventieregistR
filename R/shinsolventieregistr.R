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
                      verwijderd = character())
  if(nrow(data)>0){
    row <- data[rownr,]
    adressen <- data.frame(jsonlite::stream_in(textConnection(row$adressen)))
    for (i in 1:length(adressen)){
      cur_naam <- adressen[[glue("adres{i}")]][["Plaats"]]
      if(cur_naam == plaatsnaam){
        insolventie_nummer = row$insolventie_nummer
        kvk_nummer <- row$kvk_nummer
        bedrijfsnaam <- row$bedrijfs_naam
        publicate_datum <- row$recente_publicatie
        straat <- adressen[[glue("adres{i}")]][["Straat"]]
        huisnr <- adressen[[glue("adres{i}")]][["HuisNummer"]]
        huisnrtoevoeging = adressen[[glue("adres{i}")]][["HuisNummerToevoegingen"]]
        plaats <- adressen[[glue("adres{i}")]][["Plaats"]]
        postcode <- adressen[[glue("adres{i}")]][["Postcode"]]
        geheim_adres <- adressen[[glue("adres{i}")]][["GeheimAdres"]]
        adres_type <- adressen[[glue("adres{i}")]][["AdresType"]]
        verwijderd <- row$verwijderd
        rowdf <- data.frame(insolventie_nummer,kvk_nummer,bedrijfsnaam,publicate_datum,straat,huisnr,huisnrtoevoeging,plaats,postcode,geheim_adres,adres_type,verwijderd)
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


  pubdat <- jsonlite::stream_in(textConnection(data$recente_publicatie))
  pubdat$PublicatieDatum


  df_list <- lapply(1:nrow(data), function(r_num) {get_unnested_df_by_place(r_num, plaatsnaam, data)})
  df_out_lapply <- do.call(rbind, df_list)
  df_out_lapply

  return(df_out_lapply)
}