# Description

This repository contains code used to clean and code Industry and Occupation (I&O) data for COVID-19 cases stored in the [Washington Disease Reporting System](https://doh.wa.gov/public-health-provider-resources/public-health-system-resources-and-services/wdrs) (WDRS). The repo contains two scipts. The first pulls data out of WDRS, then cleans and imputes data in relevant I&O fields. The second takes these cleaned data and sends them to the [NIOCCS](https://csams.cdc.gov/nioccs/) web api, which returns coded I&O results. Please note this work is being published to share with attendees of [CSTE Conference 2024](https://www.csteconference.org/) and will not be useful without modification by persons outside of the Washington State Department of Health.

# Prerequesites
_Note_: This repo was built with the following R, renv, and OS versions. Other versions have not been tested and may produce undiscovered issues. This work may be modified to work with other data sources, but adjustments would be needed to read in the data and ensure a consistent data structure.
- creds.yml: this file is built using the [creds_TEMPLATE.yml](https://github.com/DOH-EPI-Coders/cleaned_industry_occupation_coding/blob/main/creds_TEMPLATE.yml) included in the repo. This file should be filled with the appropriate credentials to connect to WDRS.
- odbc setup: the connection to WDRS is built using odbc. WA DOH employees can use internal documentation to set up these connections.
-  renv v1.0.2
-  R v4.2.2
-  Microsoft Windows 10 Enterprise
