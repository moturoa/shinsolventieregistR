% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/shinsolventieregistr.R
\name{get_unnested_df_by_place}
\alias{get_unnested_df_by_place}
\title{Make an unnested dataframe}
\usage{
get_unnested_df_by_place(rownr, plaatsnaam, data)
}
\arguments{
\item{rownr}{Het rijnummer van het databestand wat wordt verwerkt}

\item{plaatsnaam}{Plaatsnaam waarvoor gezocht wordt}

\item{data}{Een dataframe met json objecten die uit de database zijn gehaald}
}
\value{
een dataframe met unnested adressen
}
\description{
Create a new dataframe in which the adresses are unnested, such that there is a row for each adress
}
