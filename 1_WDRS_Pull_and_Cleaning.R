### Script: COVID-19 IO WDRS Pull and Cleaning
### Project: COVID-19 WDRS Industry and Occupation Coding
### Purpose: This script pulls in COVID-19 case data, including I&O vars, from WDRS, filters for appropriate cases, 
###          cleans select variables, and creates a csv with the cleaned data.
###          This csv is written to the Confidential drive and can be used for manual upload to the NIOSH
###          autocoding services, or go be pulled into the webservice autocoding script.
###
### Initial script creation: 12/07/2022 Philip Crain (philip.crain@doh.wa.gov)
### Added updates to age range : 08/14/2023 Cheri Levenson (cheri.levenson@doh.wa.gov)
###
###

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
### Part 1: Pull data from WDRS ###################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# set to TRUE if pulling from test tables
test <- FALSE

# Set dates for data pull. 
date_start <- as.Date('2020-01-20')
date_end <- as.Date('2023-12-31')

# Check existence, class, and order of date_start/end:
if(!exists('date_start')) stop('Must supply date_start')
if(!exists('date_end')) stop('Must supply date_end')
if(class(date_start) != 'Date') stop('date_start must be Date class')
if(class(date_end) != 'Date') stop('date_end must be Date class')
if(date_start > date_end) stop('date_start must be greater than or equal to date_end')

# Change dates as needed. Data will be filter to only include cases where 
# POSITIVE_DEFINING_LAB_DATE_SARS is between these dates OR equal to either

library(
  odbc, # for wdrs query
  DBI,  # for wdrs query
  dplyr, # used throughout script
  lubridate, # used for age calc
  tidyr, # separate() used for data cleaning
  data.table, # for writing csv
  yaml, # for reading in credentials
  glue # for building conn string
)

# Begin WDRS pull ---------------------------------------------------------------------------------------------
message(paste('\n', 'Pulling WDRS data from', date_start, 'to', date_end, '\n\n')) #print days we are pulling

# Get credentials required to build connection string:
# creds.yml ignored from GitHub. Use creds_template.yml to build a creds.yml file if not present
creds <- read_yaml('./creds.yml')
if (test) {
  creds <- creds$test$conn_list_wdrs
} else {
  creds <- creds$default$conn_list_wdrs
}

# Connect to Washington Disease Reporting System (WDRS) where I&O data are stored:
wdrs_connect <- function(){
  
  conn_string <- glue("DRIVER={{{creds$Driver}};
            SERVER={creds$Server};
            DATABASE={creds$Database};
            trusted_connection={creds$Trusted_connection};
            ApplicationIntent={creds$ApplicationIntent}")
  conn <- DBI::dbConnect(odbc::odbc(), .connection_string = conn_string)
  return(conn)
}

wdrs <- wdrs_connect()

# This query will return a new row for each iteration of OCCUPATION, although other I&O vars will be returned already flattened:
qry <- glue::glue_sql("
SELECT cov.CASE_ID
   , cov.COUNTY
   , cov.CASE_COMPLETE_DATE
   , cov.INVESTIGATION_STATUS
   , cov.PATIENT_EMPLOYED_STUDENT
   , an.value as OCCUPATION
   , an.iteration
   , cov.OCCUPATION_TYPE
   , cov.SPECIFIC_HEALTHCARE_FIELD_WORK
   , cov.WORK_NAME
   , cov.OCCUPATION_BUSINESS_TYPE
   , cov.OCCUPATION_EMPLOYER
   , cov.OCCUPATION_CITY
   , cov.BIRTH_DATE
   , cov.POSITIVE_DEFINING_LAB_DATE_SARS
   , cov.INVESTIGATION_STATUS_UNABLE_TO_COMPLETE_REASON
   , cov.sex_at_birth
   , cov.age_years
   , NULL as AgeTest --We will calculate this variable in R after pulling data from WDRS
   , cov.ACCOUNTABLE_COUNTY
   , cov.CASE_DEDUPLICATION_STATUS
   , cov.PERSON_DEDUPLICATION_STATUS
   , cov.STATUS
   , cov.REPORTING_STATE
   , cov.POSITIVE_PCR_EXISTS_COVID19
   , cov.POSITIVE_AG_EXISTS_COVID19
   , cov.POSITIVE_PCR_LAB_DATE_COVID19
   , cov.POSITIVE_AG_LAB_DATE_COVID19
   , cov.EVENT_DATE
FROM IDS_ANSWER an 
INNER JOIN IDS_QUESTIONSET qs on an.questionset_id=qs.unid
INNER JOIN IDS_CASE cs on qs.case_id=cs.unid 
INNER JOIN DD_GCD_COVID_19_FLATTENED cov on cov.unid=cs.unid
WHERE an.question_id='OCCUPATION'
      AND cov.POSITIVE_DEFINING_LAB_DATE_SARS >= {date_start} 
      AND cov.POSITIVE_DEFINING_LAB_DATE_SARS <= {date_end}
      AND cov.ACCOUNTABLE_COUNTY <> 'WA-99'
      AND cov.CASE_DEDUPLICATION_STATUS <> 0
      AND cov.PERSON_DEDUPLICATION_STATUS <> 0 
      AND cov.STATUS <> 6
      AND cov.INVESTIGATION_STATUS = 'Complete'
      AND cov.REPORTING_STATE = 'WA'
      AND (cov.POSITIVE_PCR_EXISTS_COVID19='YES' OR cov.POSITIVE_AG_EXISTS_COVID19 = 'YES')
      AND (cov.IO_NIOCCS_DATA_AVAILABLE is NULL OR cov.IO_NIOCCS_DATA_AVAILABLE <> 'Yes') --remove cases where coding has already been completed
;", .con=wdrs)  

#Pull data:
out <- DBI::dbGetQuery(wdrs, qry)

# Close connection:
DBI::dbDisconnect(wdrs)

# For OCCUPATION, change all extra commas to semicolons and all NAs to empty strings:
out$OCCUPATION <- gsub(',', ';', out$OCCUPATION, fixed = T)
out$OCCUPATION[is.na(out$OCCUPATION)] <- ''

# Paste OCCUPATION together and get rid of duplicate rows:
out <- out %>% 
  group_by(CASE_ID) %>% 
  arrange(iteration, .by_group = T) %>% 
  mutate(OCCUPATION = paste(OCCUPATION, collapse = ', '),
         iteration = NULL) %>% 
  distinct()

# Calc AgeTest var:
out$AgeTest <- lubridate::decimal_date(out$POSITIVE_DEFINING_LAB_DATE_SARS) - lubridate::decimal_date(out$BIRTH_DATE) # calc date difference as float. Calc can be 1/366th different if comparing leap year and non leap year
out$AgeTest <- round(out$AgeTest, digits = 1) # round to one decimal place

#filter out ages 15 and younger or 66 and older (non-working ages)
#out <- out %>% filter(is.na(AgeTest) | (AgeTest >= 16 & AgeTest < 66))
#filter out ages ages 11 and younger or 86 and older - concern about missing younger and older workers updated 08/14/2023
out <- out %>% filter(is.na(AgeTest) | (AgeTest >= 11 & AgeTest < 86))


# remove rows where all I&O vars are NA:
out <- out %>% filter(!if_all(OCCUPATION:OCCUPATION_EMPLOYER, is.na))

# save a copy of this dataframe where nothing has been cleaned
before <- out

# Throw warning if there are any duplicate CASE_IDs, then dedup:
if(nrow(out) != length(unique(out$CASE_ID))) {
  warning('Duplicate CASE_IDs included in `out`. Only the first row will be kept for each duplicate.')
  out <- dplyr::distinct(out, CASE_ID, .keep_all = T)
}

message(paste('\n', nrow(out), 'cases to attempt to clean', '\n\n')) #print number of rows pulled and filtered to
##### End WDRS pull


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
### Part 2: Data cleaning #########################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# Data cleaning occurs by column in 5 steps:
# A) column is separated (by commas) into new sets of columns
# B) each column is sent through the io.scrub function, which mainly utilizes regex for substring removal/substitution
# C) the appropriate separated column is selected as the final cleaned column, and the others discarded
# D) rows where is.na(cleaned.OCCUPATION) go through another round of cleaning, selecting any available data (instead of discarding)
# E) extra cleaned columns (i.e., cleaned.WORK_NAME, .OCCUPATION_EMPLOYER) are used to fill-in NA occupation or industries, or add additional information

##### Begin data cleaning ----------------------------------------------------------------------------------

# remove all non-UTF-8 characters and substitute with space:
out <- out %>% mutate(across(where(is.character), function(x) iconv(x, "UTF-8", "UTF-8", sub = ' ')))

# Data cleaning steps A - C --------------------------------------------------------------------------------

# create io.scrub function which is to be used (for cleaning step B) in the larger data cleaning function:

#' This function cleans character elements or vectors, relying mostly on regex for pattern recognition.
#' The function will replace 'refuse collector/truck/supervisor' with 'garbage/waste collector/truck/supervisor'
#' so that these occupations will not be cleaned. '7-11' is replaced with '7-11 convenience store' for the
#' same reason.
#' 
#' Then the element is searched for sub-strings 'refuse', 'refusal', 'unknown' (and variations),  'no info', 
#' 'not provid', 'rather not say', 'declin' , 'would not answer', 'chose not to answer', 'unspec', and 'unsure'
#' (all case insensitive) and replaced with NA if any are present.
#' 
#' Sub-strings 'remote', '100%', 'self-employed' (and variations), 'work from home', 'WFH' (case-sensitive), 
#' 'n/a', '^na$', and 'works at/for a' (all case insensitive besides where indicated otherwise) are replaced 
#' with empty strings.
#' 
#' Sub-strings that are variations of abbreviations for long term care facility ('LTC', 'LTCF', 'LTFC', 
#' 'LCF', 'LTHC') are replaced with 'LONG TERM CARE FACILITY'.
#' 
#' The final element is checked to contain 2 or more alpha-characters. If there are less than 2 characters,
#' the element is repalced with NA. 
#' 
#' These steps are set in the function so that it can be repeatable for multiple i&o variables.
#' 
#' @param x A character element or vector.
#' @return A character element/vector of the same length as x with cleaned text and NA for bad values. 
#' @examples
#' io.scrub('tech bro remote WFH 100% @ Amazon')
#' io.scrub(c('refuse truck driver', '7-11'))
#' io.scrub(c('', 'refused to say', 'case gave no information', '123456'))
#' 
io.scrub <- function(x) {
  
  x <- gsub('refuse collector', 'garbage/waste collector', x, ignore.case = T) # replace refuse collector with garbage collector (so 'refuse' is not removed below)
  x <- gsub('refuse truck', 'garbage/waste truck', x, ignore.case = T) # replace refuse collector with garbage collector (so 'refuse' is not removed below)
  x <- gsub('refuse supervisor', 'garbage/waste supervisor', x, ignore.case = T) # replace refuse collector with garbage collector (so 'refuse' is not removed below)
  
  # sanitized string removed
  
  x[grepl('refuse|refusal|refual', x, ignore.case = T)] <- NA # get rid of all text if it includes "refus(ed/al)" and no other entry (comma)
  x[grepl('u[A-Z]{1,3}nown|^unk$|^unk-', x, ignore.case = T)] <- NA # remove 'unknown' and variations 
  x[grepl('(no|not|n\'t) (answer|to answer|info|provid|say|specif|state|indicate|give|disclose|work while|want to|wish to disclose|to disclose|disclose|feel comfortable|willing|recall|go to a worksite)', x, ignore.case = T)] <- NA # remove 'no information provided', 'not provided' and variations
  x[grepl('declin', x, ignore.case = T)] <- NA # remove 'decline' and variations 
  x[grepl('unsure', x, ignore.case = T)] <- NA # remove 'unsure'
  x[grepl('wouldn\'t', x, ignore.case = T)] <- NA # remove 'wouldn't'
  x[grepl('at my home', x, ignore.case = T)] <- NA # remove 'at my home' and 'at my home I don't go anywhere'
  
  #Added by CW on 9/13/2023
  x[grepl('no[nt] healthcare', x, ignore.case = T)] <- NA # get rid of all text if it includes 'non/not healthcare'
  x[grepl('no[nt] childcare', x, ignore.case = T)] <- NA # get rid of all text if it includes 'non/not childcare'
  x[grepl('no[nt] food', x, ignore.case = T)] <- NA # get rid of all text if it includes 'non food'
  x[grepl('non-sensitive occupation', x, ignore.case = T)] <- NA # get rid of all text if it includes 'non-sensitive occupation'
  x[grepl('no sensitive occupation', x, ignore.case = T)] <- NA # get rid of all text if it includes 'no sensitive occupation'
  x[grepl('non sensitive occupation', x, ignore.case = T)] <- NA # get rid of all text if it includes 'non sensitive occupation'
  x[grepl('unable to determine from emr', x, ignore.case = T)] <- NA # get rid of all text if it includes 'unable to determine from emr'
  x[grepl('sometimes yes and no', x, ignore.case = T)] <- NA # get rid of all text if it includes'sometimes yes and no'
  ## END
  
  x <- gsub('self.employ[a-z ]{0,5}\\((.+)\\)', '\\1', x, ignore.case = T) # replace 'self employed (occupation)' with 'occupation'
  x <- gsub('(case ){0,1}work.+most of the time|wfh.+most of the time|most of the time', '', x, ignore.case = T) # remove 'works from home most of the time' and variations
  x <- gsub('remotely|remote|100%', '', x, ignore.case = T) # remove 'rather not say', 'remote', and '100%'
  x <- gsub('self[- ]+employ[A-Z]+', '', x, ignore.case = T) # remove 'self-employed' and variations
  x <- gsub('(work|works|working) from home', '', x, ignore.case = T) # remove 'work from home'
  x <- gsub('WFH', '', x) # remove 'WFH' (case-sensitive)
  x <- gsub('n/a|^na$', '', x, ignore.case = T) # remove 'n/a'/'NA'
  x <- gsub('works [atfor]{2,3} a |works at | works for ', '', x, ignore.case = T) # remove 'works at/for a ' and variations
  x <- gsub('(^|[^ [:alpha:]][ [:alpha:]]{0,})not elaborate($|[ [:alpha:]]{0,}[^ [:alpha:]]{0,})', '', x, ignore.case = T) # remove 'did not elaborate' and variations
  x <- gsub('position not documented|not documented', '', x, ignore.case = T) # remove 'not documented' and variations
  x <- gsub('that was all he would give me|no more information given', '', x, ignore.case = T) # remove 'not documented' and variations
  x <- gsub('unspec[a-z]+', '', x, ignore.case = T) # remove 'unspecified'
  
  x <- gsub('^ma$|^cma$', 'medical assistant', x, ignore.case = T) # replace 'ma'/'cma' with 'medical assistant'
  x <- gsub('(^|[^[:alpha:]])ma-c($|[^[:alpha:]])', '\\1medical assistant certified\\2', x, ignore.case = T) # replace 'ma-c' with 'medical assistant certified'
  # sanitized string removed
  x <- gsub('(^|[^[:alpha:]])DSP($|[^[:alpha:]])', '\\1direct support professional\\2', x, ignore.case = T) # replace 'dsp' with 'direct support professional'
  # sanitized string removed
  x <- gsub('h2a', 'seasonal farm and agriculture', x, ignore.case = T) # replace 'h2a' (as in H-2A temporary agricultural program permit) with 'seasonal farm and agriculture'
  x <- gsub('(^|[^[:alpha:]])hcw($|[^[:alpha:]])', '\\1health care worker\\2', x, ignore.case = T) # replace 'hcw' with 'health care worker'
  # Certain acronym substitutions removed to sanitize business names
  x <- gsub('(^|[^[:alpha:]])CNA($|[^[:alpha:]])', '\\1Certified Nursing Assistant\\2', x, ignore.case = T) # replace 'CNA' with 'Certified Nursing Assistant'
  x <- gsub('(^|[^[:alpha:]])RN($|[^[:alpha:]])', '\\1Registered Nurse\\2', x, ignore.case = T) # replace 'RN' with 'Registered Nurse'
  x <- gsub('(^|[^\\.[:alpha:]])R\\.N\\.{0,}($|[^[:alpha:]])', '\\Registered Nurse\\2', x, ignore.case = T) # replace 'R.N.' with 'Registered Nurse'
  x <- gsub('(^|[^[:alpha:]])ARNP($|[^[:alpha:]])', '\\1Advanced Registered Nurse Practitioner\\2', x, ignore.case = T) # replace 'ARNP' with 'Advanced Registered Nurse Practitioner'
  
  x <- gsub('\\( {0,}\\)', '', x, ignore.case = T) # remove '()' and variations
  x <- gsub('[\'"]+([ [:alpha:]]+)[\'"]+', '\\1', x) # remove single and double quotes around words and phrases
  x <- gsub('^[^[:alnum:]\\(\\)]+|[^[:alnum:]\\(\\)]+$', '', x) # remove spaces and punctuation from beginning and end of strings, excluding parentheses
  x <- gsub('\\s{2,}', ' ', x, ignore.case = T) # remove extra whitespaces
  
  x <- gsub('^IT$|^I\\.T$|^I\\.T\\.', 'Internet Technology', x, ignore.case = T) # replace 'IT' with 'Internet Technology'

  # replace LCF, LTC, LTCF, LTHC, LTFC (with no alpha-char preceding or following) with 'LONG TERM CARE FACILITY'
  x <- gsub('([^A-Za-z]|^)(LCF|LTC|LTFC|LTHC|LTCF)([^A-Za-z]|$)', '\\1LONG TERM CARE FACILITY\\3', x, ignore.case = T)
  x <- gsub('facility facility', 'facility', x, ignore.case = T) #get rid of repeating 'facility' as sometimes text will be "LTC FACILITY" and end up with two "FACILITY"s
  
  x[nchar(gsub('[^[:alpha:]]', '', x)) < 2] <- NA # remove elements that have less than two characters
  
  return(x)
}

# create large data cleaning function which will put data through cleaning steps 1-3:

#' This function is large and goes through 3 steps:
#' 
#' First, the supplied column is separated (using tidyr::separate) into five columns using commas as separators.
#' Columns supplied to this function are often free text which can contain commas inside the text, as well as commas
#' separating one instance from the next (as columns are also often repeatable fields in WDRS). Because of this,
#' the column 'OCCUPATION_TYPE' from the supplied df is used to check that the number of commas in the supplied column
#' matches the number of commas instances of the repeated fields. ('OCCUPATION_TYPE' is not free text and will only have
#' the number of commas equal to the number of instances of the repeatable fields - 1.) This check is only done for cases
#' that have only one instance of the repeatable fields, but contain a comma. These commas are changed to semicolons so they
#' do not cause a shift in the separate step.
#' 
#' Second, the 5 new columns undergo character cleaning using regex to replaces strings and sub-strings in the io.scrub()
#' function. See this function for more details on which strings are cleaned and other changes.
#' 
#' Third, the 5 columns are coalesced to only keep data from one column (unless count==0, see below). A new column, named
#' 'cleaned.' plus the name of the original column provided, is created which contains the x instance of the five separated
#' columns, where x is equal to the value in the 'count' column.
#' 
#' If create.count==T, the variable 'count' is created which equals the int value of the first column (within the 5 created 
#' columns) which is not equal to NA. E.g., if x_cleaning_1 and x_cleaning_2 == NA_character_, but x_cleaning_3 == 'some text', 
#' count == 3. 'count' is used to ensure data from the same instance are kept across variables, so data for 'OCCUPATION' will 
#' align with data in 'OCCUPATION_EMPLOYER' and other columns. Otherwise, data could be mixed between instances of the 
#' repeatable fields.
#' 
#' If 'count' == 0 (there were no non-NA columns after io.scrub), then the cleaned.variable == NA.
#' 
#' These steps are set in the function so that it can be repeatable for multiple i&o variables.
#' 
#' @param .df A data frame
#' @param col Unquoted name of the column to be cleaned. Column must be character class. 
#' @param create.count If TRUE, a 'count' column is created within .df which indicates the position of the first non-NA
#' data in the supplied column. If FALSE, 'count' must already be present. Default is FALSE. 
#' @return A data frame with a new column: cleaned.col (where 'col' is the name of the object assigned to col param). 
#' cleaned.col is a character class and contains the cleaned data from the supplied col, after undergoing cleaning with regex
#' and selection of instances using count. If create.count == TRUE, a new column named 'count' will be present which indicates
#' the first position of non-NA data in the original col.  
#' @examples
#' OCCUPATION = c('hacker, and computer genius', 'refuse truck driver, garbage man', 'refused to answer')
#' OCCUPATION_TYPE = c('', ',', '')
#' df = data.frame(OCCUPATION, OCCUPATION_TYPE)
#' 
#' io.clean(df, OCCUPATION, create.count = T)
#' 
io.clean <- function(.df, col, create.count = F) {
  
  .name = as.character(substitute(col))

  # Data cleaning step A --------------------------------------------------------------------------------
  # separate column into 5 'cleaning' column:
  
  .df <- .df %>% mutate(x = {{col}})
  
  # remove/replace commas from occupation if there are not multiple entries (i.e., if comma was written in free text instead of added by concatenating multiple OCCUPATION entries):
  .df$x[!grepl(',', .df$OCCUPATION_TYPE)] = gsub(',', ';', .df$x[!grepl(',', .df$OCCUPATION_TYPE)])
  
  # separate column
  .df = separate(data = .df,
                col = x, 
                into = paste0('x_cleaning_', 1:5), 
                sep = ',',
                extra = 'merge', 
                fill = 'right')

  # Data cleaning step B --------------------------------------------------------------------------------
  # create cleaned vars
  
  .df <- .df %>% mutate(across(.cols = starts_with('x_cleaning_'), .fns = io.scrub))

  # Data cleaning step C --------------------------------------------------------------------------------
  # select one final cleaned variable:
  
  # let's id which instance of this variable we are going to keep. We can use this to select appropriate instances for other vars later:
  if (create.count) {
    #returns the index (as int) of the first non-NA element. If all strings are NA, returns 0
    .df <- .df %>% mutate(count = case_when(!is.na(x_cleaning_1) ~ 1,
                                          !is.na(x_cleaning_2) ~ 2,
                                          !is.na(x_cleaning_3) ~ 3,
                                          !is.na(x_cleaning_4) ~ 4,
                                          !is.na(x_cleaning_5) ~ 5,
                                          T ~ 0))
    }
  
  .df <- .df %>% mutate(newcol = case_when(count == 1 ~ x_cleaning_1,
                                         count == 2 ~ x_cleaning_2,
                                         count == 3 ~ x_cleaning_3,
                                         count == 4 ~ x_cleaning_4,
                                         count == 5 ~ x_cleaning_5,
                                         count == 0 ~ NA_character_))
  
  names(.df)[names(.df) == 'newcol'] <- paste0('cleaned.', .name) #rename final cleaned.column
  rm(.name)
  
  .df <- .df %>% select(-contains('x_cleaning_')) #remove intermediate columns
  
  return(.df)
}

out <- out %>% 
  io.clean(OCCUPATION, create.count = T) %>%
  io.clean(WORK_NAME) %>% 
  io.clean(OCCUPATION_BUSINESS_TYPE) %>% 
  io.clean(OCCUPATION_EMPLOYER)

# If cleaned.OCCUPATION_EMPLOYER is NA, fill with text after "@" in cleaned.OCCUPATION (if present):
out <- out %>% mutate(cleaned.OCCUPATION_EMPLOYER = case_when(!is.na(cleaned.OCCUPATION_EMPLOYER) ~ cleaned.OCCUPATION_EMPLOYER, # Keep employer if already filled
                                                              grepl('@.+', cleaned.OCCUPATION) ~ sub('.+@', '', cleaned.OCCUPATION))) # get rid of first part of string to fill employer
out$cleaned.OCCUPATION <- sub('@.+', '', out$cleaned.OCCUPATION) # remove text after "@" from cleaned.OCCUPATION

# Data cleaning step D -----------------------------------------------------------------------------------------------
# re-clean empty Occupation rows

# If 'count' was == 0, all of the cleaned columns will be NA. re-cleaning these columns and creating a new count each time will
# mean the first non-NA data will be taken for each cleaned variable.
empty_occupation <- out %>% filter(count == 0)
out <- anti_join(out, empty_occupation, by = names(out))
empty_occupation <- select(empty_occupation, -cleaned.OCCUPATION_BUSINESS_TYPE, -cleaned.OCCUPATION_EMPLOYER, -cleaned.WORK_NAME) # remove coded cleaned.vars before we code again

no.occ <- nrow(empty_occupation) # number of cases missing occupation. Used later to count how many cases were removed due to no data

# Fill data for cases with no cleaned.OCCUPATION:
empty_occupation$OCCUPATION_TYPE.2 <- gsub('Other', '', empty_occupation$OCCUPATION_TYPE) # get rid of "Other" in OCCUPATION_TYPE

empty_occupation <- io.clean(empty_occupation, OCCUPATION_TYPE.2, create.count = T)
empty_occupation$cleaned.OCCUPATION_TYPE <- empty_occupation$cleaned.OCCUPATION_TYPE.2 #fix name
empty_occupation <- select(empty_occupation, -contains('OCCUPATION_TYPE.2')) #remove extra vars

# this code will grab the first non-NA values from each variable. This means there potentially could be mixing of different entries. 
empty_occupation <- io.clean(empty_occupation, WORK_NAME, create.count = T) # select the first non-NA WORK_NAME
empty_occupation <- io.clean(empty_occupation, OCCUPATION_BUSINESS_TYPE, create.count = T) # select the first non-NA OCCUPATION_BUSINESS_TYPE
empty_occupation <- io.clean(empty_occupation, OCCUPATION_EMPLOYER, create.count = T) # select the first non-NA OCCUPATION_EMPLOYER

# Fill cleaned.OCCUPATION with SPECIFIC_HEALTHCARE_FIELD_WORK if it is non-NA, else fill with cleaned.OCCUPATION_TYPE.
# Then, remove cases where no data is present in the cleaned OCCUPATION, OCCUPATION_BUSINESS_TYPE, or OCCUPATION_EMPLOYER vars:
empty_occupation <- empty_occupation %>% 
  mutate(cleaned.OCCUPATION = case_when(SPECIFIC_HEALTHCARE_FIELD_WORK %in% 'Other (Health Services)' ~ 'Health Services',
                                        !is.na(SPECIFIC_HEALTHCARE_FIELD_WORK) ~ SPECIFIC_HEALTHCARE_FIELD_WORK),
         cleaned.OCCUPATION = case_when(!is.na(cleaned.OCCUPATION) ~ cleaned.OCCUPATION,
                                        !is.na(cleaned.OCCUPATION_TYPE) ~ cleaned.OCCUPATION_TYPE),
         cleaned.OCCUPATION_TYPE = NULL) %>% 
  filter(!if_all(starts_with('cleaned.'), is.na))

# combine empty occupations and filled occupations into one df. Remove 'count' column and trim whitespace:
io_final <- rbind(out, empty_occupation) %>% mutate(count = NULL, across(.cols = where(is.character), .fns = trimws))

message(paste('\n', no.occ - nrow(empty_occupation), 'cases removed due to absence of any I&O data after cleaning', '\n\n', #print number of rows removed as all 'cleaned.' vars were NA
          nrow(io_final), 'cases to be written to cleaned data file', '\n\n')) #print number of rows of final data

# Data cleaning step E --------------------------------------------------------------------------------
# final cleaning: fill-in data using extra cleaned columns

io_final <- io_final %>% 
  mutate(cleaned.OCCUPATION = 
           case_when(
             # add Daycare Worker to cleaned.OCCUPATION when worker indicates at home work:
             OCCUPATION_TYPE == 'Daycare worker' & grepl('home day care|home daycare|self', OCCUPATION, ignore.case = T) ~ 'Daycare worker',
             T ~ cleaned.OCCUPATION),
         cleaned.OCCUPATION_BUSINESS_TYPE =
           case_when(
             # Change OCCUPATION_BUSINESS_TYPE to ltcf for certain ltcf's:
             # sanitized string removed
             # sanitized string removed
             # sanitized string removed
             # add employer to occupation if RN
             startsWith(cleaned.OCCUPATION, 'Registered Nurse') & !is.na(cleaned.OCCUPATION_EMPLOYER) & !is.na(cleaned.OCCUPATION_BUSINESS_TYPE) ~ paste(cleaned.OCCUPATION_BUSINESS_TYPE, 'at', cleaned.OCCUPATION_EMPLOYER),
             # add school type (elementary, middle, high, etc.) to occupation if teacher
             grepl('teacher', cleaned.OCCUPATION, ignore.case = T) & grepl('preschool|kindergar.en|elementary|middle|junior high|high school', cleaned.OCCUPATION_BUSINESS_TYPE, ignore.case = T) ~ cleaned.OCCUPATION_BUSINESS_TYPE,
             grepl('teacher', cleaned.OCCUPATION, ignore.case = T) & grepl('preschool|kindergar.en|elementary|middle|junior high|high school', cleaned.OCCUPATION_EMPLOYER, ignore.case = T) ~
               paste(ifelse(is.na(cleaned.OCCUPATION_BUSINESS_TYPE), 'education', cleaned.OCCUPATION_BUSINESS_TYPE), 'at', gsub('.{0,}(preschool|elementary school|elementary|kindergar.en|middle school|middle|junior high|high school).{0,}', '\\1', cleaned.OCCUPATION_EMPLOYER, ignore.case = T)),
             grepl('teacher', cleaned.OCCUPATION, ignore.case = T) & grepl('([1-5](st|nd|rd|th)|first|second|third|fourth|fifth) grade', cleaned.OCCUPATION, ignore.case = T) ~ paste(ifelse(is.na(cleaned.OCCUPATION_BUSINESS_TYPE), 'education', cleaned.OCCUPATION_BUSINESS_TYPE), 'at elementary school'),
             grepl('teacher', cleaned.OCCUPATION, ignore.case = T) & grepl('([6-8]th|sixth|seventh|eigth) grade', cleaned.OCCUPATION, ignore.case = T) ~ paste(ifelse(is.na(cleaned.OCCUPATION_BUSINESS_TYPE), 'education', cleaned.OCCUPATION_BUSINESS_TYPE), 'at middle school'),
             grepl('teacher', cleaned.OCCUPATION, ignore.case = T) & grepl('(9th|10th|11th|12th|ninth|tenth|eleventh|twelfth) grade', cleaned.OCCUPATION, ignore.case = T) ~ paste(ifelse(is.na(cleaned.OCCUPATION_BUSINESS_TYPE), 'education', cleaned.OCCUPATION_BUSINESS_TYPE), 'at high school'),
             T ~ cleaned.OCCUPATION_BUSINESS_TYPE))

# Check if cleaned.OCCUPATION/OCCUPATION_BUSINESS_TYPE are NA. If so, fill with other present variables:
io_final <- io_final %>% 
  mutate(
    cleaned.OCCUPATION = case_when(
      !is.na(cleaned.OCCUPATION) ~ cleaned.OCCUPATION,
      !is.na(cleaned.WORK_NAME) ~ cleaned.WORK_NAME,
      !is.na(cleaned.OCCUPATION_EMPLOYER) ~ cleaned.OCCUPATION_EMPLOYER),
    cleaned.OCCUPATION_BUSINESS_TYPE = case_when(
      !is.na(cleaned.OCCUPATION_BUSINESS_TYPE) ~ cleaned.OCCUPATION_BUSINESS_TYPE,
      !is.na(cleaned.OCCUPATION_EMPLOYER) ~ cleaned.OCCUPATION_EMPLOYER,
      !is.na(cleaned.WORK_NAME) ~ cleaned.WORK_NAME)
  )

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
### Part 3: Write final data file #################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# create file name and path:
io_filename <- glue("io_{format(date_start, '%Y%m%d')}_{format(date_end, '%Y%m%d')}.csv")
io_filepath <- file.path('./data', io_filename)

# write data:
data.table::fwrite(io_final, io_filepath)

###
# Create uncleaned final file:
before_final <- before %>% 
  mutate("cleaned.OCCUPATION" = OCCUPATION,
         "cleaned.WORK_NAME" = WORK_NAME,
         "cleaned.OCCUPATION_BUSINESS_TYPE" = OCCUPATION_BUSINESS_TYPE,
         "cleaned.OCCUPATION_EMPLOYER" = OCCUPATION_EMPLOYER) %>% 
  select(all_of(names(io_final)))

before_filename <- glue("before_{format(date_start, '%Y%m%d')}_{format(date_end, '%Y%m%d')}.csv")
before_filepath <- file.path('./data', before_filename)

# write data:
data.table::fwrite(before_final, before_filepath)
###

# clear global R env if file was successfully written:
if (file.exists(io_filepath)) {
  rm(test, creds, date_end, date_start, empty_occupation, io_final, out, wdrs, no.occ, qry, io.clean, io.scrub, wdrs_connect, io_filename, io_filepath,
     before, before_final, before_filename, before_filepath)
}
