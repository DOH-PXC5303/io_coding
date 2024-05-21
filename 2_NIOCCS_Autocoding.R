### Script: COVID-19 IO NIOCCS Autocoding Manual Upload
### Project: COVID-19 WDRS Industry and Occupation Coding
### Purpose: This script pulls in COVID-19 cleaned I&O data, selects data needed for NIOCCS upload, and formats
###          data to match NIOSH requirements. A .txt file is written to the Confidential drive and 
###          should be manual uploaded to the NIOSH autocoding services, which will return a coded file.
###
### Initial script creation: 12/29/2022 Philip Crain (philip.crain@doh.wa.gov)
### Added script on 8/14/2023 Cheri Levenson (cheri.levenson@doh.wa.gov) to send roster file directly to 
###				Data Support Team roster folder for import		
###  

library(httr, # used with autocoding webservice
        jsonlite, # used with autocoding webservice
        dplyr, # used throughout script
        data.table, # used to read and write data (fread & fwrite)
        glue, # glue::glue() used throughout script
        furrr, # used for multi-session processing with webservice
        future # used for multi-session processing with webservice
)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
### Part 1: Import cleaned data ###################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# create file name and path:
io_directory <- './data'

io_filename <- list.files(path = './data',
                          pattern = '^io_[0-9]{8}_[0-9]{8}.csv$')

# # If coding uncleaned data, run this:
# io_filename <- list.files(path = './data',
#                           pattern = '^before_[0-9]{8}_[0-9]{8}.csv$')

# select most recent file
io_filename <- io_filename[file.mtime(file.path(io_directory, io_filename)) == max(file.mtime(file.path(io_directory, io_filename)))]
io_filepath <- file.path(io_directory, io_filename)

# load data:
message(glue('Importing the following file to code: {io_filename}'))
cleaned_data <- data.table::fread(io_filepath, colClasses = 'character') %>% 
  mutate(across(.cols = where(is.character), .fns = function(x) ifelse(is.na(x), '', x)))

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
### Part 2: Send to webservice ###################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# this code gets coded I&O results in R calling the NIOSH IO Coding service:
# run in parallel for quicker runtime
message(glue('Finding I&O codes and probabilities for {nrow(cleaned_data)} rows'))

# (!!) running in parallel:

# create clusters; dedicating 5 workers for 4 cores

options('future.globals.maxSize' = (1014*1024^2)*7)

future::plan(list(future::tweak(future::multisession, workers = future::availableCores(omit = 3))))
nw <- future::nbrOfWorkers() %>% as.numeric
message(glue("number of workers: {nw}"))

# (a)	prepped as a URL string and cases are put as a list (CASE_ID and URL i&o string)
# (b)	URL i&o string sent to webservice for autocoding (safe handling of errors and using parallel processing occurs here)
# (c)	Returned results come back and converted to list

# scrub addresses special characters in string
addscrubber <- function(string){
  if (!is.character(string)) {
    stop("variable need to be in character format")
  }
  string_out <- gsub("%", "%25", string)
  string_out <- gsub("@", "%40", string_out)
  string_out <- gsub("[[:space:]]+", "%20", string_out)
  string_out <- gsub("#", "%23", string_out)
  string_out <- gsub("\\+", "%2B", string_out)
  string_out <- gsub("/", "%2F", string_out)
  string_out <- gsub(":", "%3A", string_out)
  string_out <- gsub(";", "%3B", string_out)
  return(string_out)
}


io_appending <-   function(x, y) {
  tryCatch(x %>% 
             mutate(NAICS_CODE = NA,
                    NAICS_TITLE = NA,
                    NAICS_PROB = NA,
                    CENSUS_IND_CODE = NA,
                    CENSUS_IND_TITLE = NA,
                    SOC_CODE = NA,
                    SOC_TITLE = NA,
                    SOC_PROB = NA,
                    CENSUS_OCC_CODE = NA,
                    CENSUS_OCC_TITLE = NA,
                    CODING_SCHEME = NA) %>% 
             mutate(
               NAICS_CODE = y[["Industry.NAICSCode"]],
               NAICS_TITLE = y[["Industry.NAICSTitle"]],
               NAICS_PROB = as.numeric(y[["Industry.NAICSProbability"]]),
               CENSUS_IND_CODE = y[["Industry.CensusIndustryCode"]],
               CENSUS_IND_TITLE = y[["Industry.CensusIndustryTitle"]],
               SOC_CODE = y[["Occupation.SOCCode"]],
               SOC_TITLE = y[["Occupation.SOCTitle"]],
               SOC_PROB = as.numeric(y[["Occupation.SOCProbability"]]),
               CENSUS_OCC_CODE = y[["Occupation.CensusOccupationCode"]],
               CENSUS_OCC_TITLE = y[["Occupation.CensusOccupationTitle"]],
               CODING_SCHEME = y[["Scheme"]]),
           # ... but if an error occurs, return x as it is
           error=function(error_message) {return(x)})
}

rx <-  cleaned_data %>% 
  dplyr::select(CASE_ID, cleaned.OCCUPATION, cleaned.OCCUPATION_BUSINESS_TYPE) %>%
  mutate(rCASE_ID = CASE_ID,
         reportingstr = glue::glue("https://wwwn.cdc.gov/nioccs/IOCode?i={addscrubber(cleaned.OCCUPATION_BUSINESS_TYPE)}&o={addscrubber(cleaned.OCCUPATION)}&v=18&c=2")
  ) %>% 
  group_by(rCASE_ID, .drop = F) %>%
  tidyr::nest(.names_sep = "rCASE_ID")

message(glue("approx. number of chunks divided amongst {nw} workers: {ceiling(length(rx[['data']])/(100 * nw)) * 100}"))
message(glue("exact number of chunks divided amongst {nw} workers: {length(rx[['data']])/nw}"))


chnksize <- ceiling(length(rx[['data']])/(100 * nw)) * 100

saferhandling = purrr::possibly( .f = ~ .x %>% httr::GET(.) %>% content(., as =  "text") %>% fromJSON(.) %>% unlist(.) %>% as.list(.)
                                 , otherwise = "errorrrrr")

ry <- furrr::future_map(.x = rx$data , .f =~saferhandling( .x[['reportingstr']]),
                        .options = furrr_options(chunk_size =  chnksize),
                        .progress = T)

# >  map/parse reporting add to df -----------------------------------------

# (d)	Cases list is joined/iterated over autocoded list -> output as
# dataframe at case level with autocoding fields (error handling occurs here too).

io_autocoded <- furrr::future_map2_dfr(.x = rx$data , 
                                       .y = ry , 
                                       .f = io_appending,
                                       .options = furrr_options(chunk_size = chnksize))

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
### Part 3: Prepare WDRS roster and save as csv ###################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# Join autocoding data to cleaned data:
cleaned_data_coded <- cleaned_data %>% 
  left_join(io_autocoded, by = c('CASE_ID', 'cleaned.OCCUPATION', 'cleaned.OCCUPATION_BUSINESS_TYPE')) %>% 
  dplyr::select(-reportingstr)

# Recode low probability codes and titles:
# cleaned_data_coded$NAICS_CODE[cleaned_data_coded$Industry.Prob < 0.8] <- '009990'
# cleaned_data_coded$NAICS_TITLE[cleaned_data_coded$Industry.Prob < 0.8] <- 'Insufficient Information'
# cleaned_data_coded$SOC_CODE[cleaned_data_coded$Occupation.Prob < 0.8] <- '00-9900'
# cleaned_data_coded$SOC_TITLE[cleaned_data_coded$Occupation.Prob < 0.8] <- 'Insufficient Information'

# Clean scheme and throw error if an unknown scheme has been returned:
cleaned_data_coded <- cleaned_data_coded %>% 
  mutate(
    CODING_SCHEME = case_when(
      CODING_SCHEME == 'NAICS 2012 and SOC 2010, Census Industry and Occupation 2012' ~ 'NAICS2012_SOC2010_CEN2012',
      CODING_SCHEME == 'NAICS 2017 and SOC 2018, Census Industry and Occupation 2018' ~ 'NAICS2017_SOC2018_CEN2018'
      # add future coding schemes here and convert them to WDRS value, otherwise they will be converted to NA
    )
  )

if(any(is.na(cleaned_data_coded$CODING_SCHEME))) {
  print('Returned coding schemes:')
  print(table(io_autocoded$CODING_SCHEME, useNA = 'always'))
  stop('Unexpected coding scheme was returned.')
}

# create final roster data:
roster <- cleaned_data_coded %>% 
  mutate(
    NIOCCS_DATA_AVAILABLE = 'Yes',
    Case.Note = 'NIOCCS Data updated via roster') %>% 
  dplyr::select(CaseID = CASE_ID
                , NIOCCS_DATA_AVAILABLE
                , CODING_SCHEME
                , NAICS_TITLE
                , NAICS_CODE	
                , NAICS_PROB	
                , SOC_TITLE	
                , SOC_CODE	
                , SOC_PROB	
                , CENSUS_IND_TITLE	
                , CENSUS_IND_CODE	
                , CENSUS_OCC_TITLE	
                , CENSUS_OCC_CODE	
                , Case.Note) %>%
  mutate(across(contains('CODE'), function(x) paste0(' ', x))) #add leading space in every code column to address excel removing leading zeros and dates if editing document

# Save WDRS roster:
data.table::fwrite(roster, glue('./roster/gcd_roster_{io_filename}'))

# # Save final coded DF: 
data.table::fwrite(cleaned_data_coded, glue('{io_directory}/coded_{io_filename}'))

# clear global R env if file was successfully written:
if (file.exists(glue('./roster/gcd_roster_{io_filename}'))) {
  rm(io_directory, io_filename, io_filepath, cleaned_data, nw, addscrubber, io_appending, rx, chnksize, saferhandling, ry, io_autocoded, cleaned_data_coded, roster)
}
