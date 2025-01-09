# box-office-tracking

Tracking a box office draft in a google sheet

etl.py is the main entry point, and takes in a list of years to process.
Each year must have the following:
- a corresponding folder in assets/ with the following files:
  - box_office_draft.csv
    - Source of truth for the box office draft.
    - Columns: round, overall, name, movie
  - manual_adds.csv
    - List of movies that do not show up in the top 200 at the end of the year so they must be added manually.
    - This file must have at least 1 row, which can be an empty row if there are no manual adds.
    - Columns: title, revenue, domestic_rev, foreign_rev, release_date
- a google sheet named "**year** Fantasy Box Office Draft" with a tab called "Dashboard"
- a .env file with the following variables:
  - GSPREAD_CREDENTIALS_**year**
    - Credentials for the google sheet that data will be written to for that year. [Here](https://docs.gspread.org/en/latest/oauth2.html#for-bots-using-service-account) is how to generate these credentials and add the account to the google sheet.
  - S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY
    - Access key id and secret access key for the s3 bucket that contains the box office data.
  - MODAL_TOKEN_ID and MODAL_TOKEN_SECRET
    - Token id and secret for the [modal](https://modal.com/) account.

To Do:
- add in a check after loading to get the movies that have a lower revenue than the lowest revenue in the most recent top 200
- add tests
