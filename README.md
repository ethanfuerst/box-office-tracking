# box-office-tracking

**Tracking a box office draft in a google sheet**

This repo is a tool I use to track a box office draft in a google sheet. It produces a dashboard in the google sheet that shows the box office data for the year.

Here is an example of the dashboard:

![dashboard](assets/dashboard.png)

In this example, I drafted with 4 friends. We did a snake draft, and we each picked 15 movies. There's a concept of "scored revenue", which is the revenue of a movie multiplied by a multiplier. The multipliers are set in the config.yml file. You can set a multiplier for a round or a movie. In this example, the revenue of the movies that we drafted in the last round are multiplied by 5 for scoring purposes. You can also set a multiplier for a movie, which is useful if you want to make sure a movie doesn't skew the results too much.

There's 3 main sections of the dashboard:

- Standings
  - On the top left, each drafter's name is listed with their scored revenue, as well as the number of movies that they drafted that have been released. There's also a column to show how many picks they chose that were optimal, which is a pick that was better than any movie avaiable at the time of that pick.
- Released Movies
  - Every movie that has been drafted or is listed in manual_adds.csv is listed here.
  - The movies are sorted by scored revenue.
- Worst Picks
  - The worst picks are listed here, which are the movies that missed out on the most revenue. For example, if you picked a movie first that made $100 million, and then the next best movie made $200 million, you missed out on $100 million.
  - The movies are sorted by the maximum amount of revenue they missed out on.

Throughout the year, I make sure that each movie has the correct title in the box_office_draft.csv file. Each movie will only be picked up if it's in the top 200 on the Box Office Mojo Worldwide page.

The way I originally set this up was to scrape the box office data from the Box Office Mojo page each day and store it in an s3 bucket. Then I would read the data from the s3 bucket and load it in to the google sheet. This is more accurate because it's possible for a smaller movie to show up in the top 200 at one point in time, but then drop out of the top 200 by the end of the year as larger movies get released. I have some logging in the code to record these movies that may have missed revenue.

If you want to do a smaller draft, you can choose for a draft to pull from the Box Office Mojo page each time the dashboard is updated. This is easier to set up, but it's not as accurate as the s3 method.

---

To run this on your own, you will need:
 - a config.yml file in the config folder that contains an id for each dashboard you want to update. The top level tag is "**dashboards**", and each id must have the following tags:
   - **name**
     - Name of the draft
   - **description**
     - Description of the draft
   - **year**
     - Release year of movies that will be scraped
   - **update_type**
     - Can be "scrape" or "s3"
     - "scrape" will scrape the box office data from [Box Office Mojo](https://www.boxofficemojo.com/year/world/)
       - This is the easiest way to run this as it doesn't require setting up a s3 bucket.
     - "s3" will assume the box office data is already in an s3 bucket and will load it into duckdb (more on that below)
       - This method is a more accurate than the scrape method and but requrires setting up a s3 bucket.
   - **sheet_name**
     - Name of the google sheet that the dashboard will be written to.
   - **folder_name**
     - Name of the folder in the assets folder that contains the config files for the draft (more on that below)
  and optionally the following tags:
    - **movie_multiplier_overrides**
      - If you want to change the scored revenue for a specific multiplier for a movie, you can do so here.
      - Typically I do this for movies that are very large, like Avatar, and want to make sure they don't skew the results too much.
      - Columns: movie, multiplier
    - **round_multiplier_overrides**
      - If you want to add a specific scored revenue multiplier for a round, you can do so here.
      - Columns: round, multiplier
For each id in your config.yml file, you will need the following:
- a google sheet named "**sheet_name**" with a tab called "Dashboard"
  - This sheet will be used to display the box office data for the year.
  - See below about how to set up access to this sheet.
- a .env file with the following variables:
  - **MODAL_TOKEN_ID** and **MODAL_TOKEN_SECRET**
    - Token id and secret for the [modal](https://modal.com/) account.
  - **GSPREAD_CREDENTIALS_<year>**
    - Credentials for the google sheet that data will be written to for that year. [Here](https://docs.gspread.org/en/latest/oauth2.html#for-bots-using-service-account) is how to generate these credentials and add the account to the google sheet.
    - You can change the name of this variable in the config.yml file using the gspread_credentials_name tag, but it defaults to GSPREAD_CREDENTIALS_<year>.
- a corresponding folder (specified in the folder_name tag in the config.yml file) in assets/ with the following files:
  - **box_office_draft.csv**
    - Source of truth for the box office draft.
    - Columns: round, overall, name, movie
  - **manual_adds.csv**
    - List of movies that do not show up in the top 200 at the end of the year so they must be added manually.
    - This file must have at least 1 row, which can be an empty row if there are no manual adds.
    - Columns: title, revenue, domestic_rev, foreign_rev, release_date
If your id is set to scrape, your config.yml file will also need the following:
- **S3_ACCESS_KEY_ID** and **S3_SECRET_ACCESS_KEY**
  - Access key id and secret access key for the s3 bucket that contains the box office data.
- **BUCKET**
  - Name of the s3 bucket (and path, if applicable) that contains the box office data.
  - The files must be named in the format boxofficemojo_YYYYMMDD.parquet, where YYYYMMDD is the date the data was scraped.
and you will need an s3 bucket with the following files:
- boxofficemojo_YYYYMMDD.parquet
  - Contains the box office data for the year scraped from [Box Office Mojo](https://www.boxofficemojo.com/year/world/) on YYYYMMDD with the following columns:
    - "Release Group"
    - "Worldwide"
    - "Domestic"
    - "Foreign"
    - "Date"

Here is an example of the config/config.yml file:

```
dashboards:
  my_2025_draft:
    name: 2025 Fantasy Box Office Standings
    description: Draft with Holden, Troy, Nahiyan and Larry for 2025
    year: 2025
    update_type: s3
    sheet_name: 2025 Fantasy Box Office Draft
    folder_name: my_2025_draft_data
    movie_multiplier_overrides:
      - movie: "Avatar: Fire and Ash"
        multiplier: 0.5
    round_multiplier_overrides:
      - round: 20
        multiplier: 5
    gspread_credentials_name: GSPREAD_CREDENTIALS_FRIENDS_2025 # if you want to specify the name of the gspread credentials variable, defaults to GSPREAD_CREDENTIALS_<year>
    bucket: box-office-tracking # only needed if update_type is s3
    s3_access_key_id_var_name: S3_ACCESS_KEY_ID_MY_2025_DRAFT # only needed if update_type is s3
    s3_secret_access_key_var_name: S3_SECRET_ACCESS_KEY_MY_2025_DRAFT # only needed if update_type is s3
```
