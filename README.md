# box-office-tracking

Tracking a box office draft in a google sheet

etl.py is the main entry point, and takes in a list of years to process.
Each year must have a corresponding folder in assets/ with the following files:
- box_office_draft.csv
  - Source of truth for the box office draft.
  - Columns: round, overall, name, movie
- manual_adds.csv
  - List of movies that do not show up in the top 200 at the end of the year so they must be added manually.
  - This file must have at least 1 row, which can be an empty row if there are no manual adds.
  - Columns: title, revenue, domestic_rev, foreign_rev, release_date

Each year for the draft must also have an environment variable in the .env file with the name **year**_GSPREAD_CREDENTIALS. This contains the credentials for the google sheet that data will be written to for that year. [Here](https://docs.gspread.org/en/latest/oauth2.html#for-bots-using-service-account) is how to generate these credentials.

To Do:
- add tests
- get list of movies that are not in top 200 but are drafted at the end of the year and compare
 