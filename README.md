# box-office-tracking

**Tracking a box office draft in a google sheet**

etl.py is the main entry point, and takes in a list of years to process.

To run this on your own, you will need:
- an s3 bucket with the following files:
  - boxofficemojo_YYYYMMDD.parquet
    - Contains the box office data for the year scraped from [Box Office Mojo](https://www.boxofficemojo.com/year/world/) on YYYYMMDD with the following columns:
      - "Release Group"
      - "Worldwide"
      - "Domestic"
      - "Foreign"
      - "Date"
    - See example of how I scrape these [here](https://github.com/ethanfuerst/chrono/tree/main/box_office_tracking).
- a .env file with the following variables:
  - S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY
    - Access key id and secret access key for the s3 bucket that contains the box office data.
  - MODAL_TOKEN_ID and MODAL_TOKEN_SECRET
    - Token id and secret for the [modal](https://modal.com/) account.
  - BUCKET
    - Name of the s3 bucket (and path, if applicable) that contains the box office data.
    - The files must be named in the format boxofficemojo_YYYYMMDD.parquet, where YYYYMMDD is the date the data was scraped.

Each year must have the following:
- a corresponding folder in assets/ with the following files:
  - box_office_draft.csv
    - Source of truth for the box office draft.
    - Columns: round, overall, name, movie
  - manual_adds.csv
    - List of movies that do not show up in the top 200 at the end of the year so they must be added manually.
    - This file must have at least 1 row, which can be an empty row if there are no manual adds.
    - Columns: title, revenue, domestic_rev, foreign_rev, release_date
  - round_multiplier_overrides.csv
    - If you want, you can add a multiplier for each round. These amounts will be multiplied by the revenue to determine the scored revenue.
    - Columns: round, multiplier
- a google sheet named "**year** Fantasy Box Office Draft" with a tab called "Dashboard"
- a variable in the .env file named GSPREAD_CREDENTIALS_**year**
  - Credentials for the google sheet that data will be written to for that year. [Here](https://docs.gspread.org/en/latest/oauth2.html#for-bots-using-service-account) is how to generate these credentials and add the account to the google sheet.
