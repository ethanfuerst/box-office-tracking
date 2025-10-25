import json
import logging
import os
from datetime import datetime
from typing import Dict

import gspread_formatting as gsf
from dotenv import load_dotenv
from gspread import service_account_from_dict

from src.utils.db_connection import DuckDBConnection
from src.utils.format import load_format_config
from src.utils.gspread_format import df_to_sheet
from src.utils.logging_config import setup_logging
from src.utils.query import table_to_df

setup_logging()

load_dotenv()


class GoogleSheetDashboard:
    def __init__(self, config: Dict):
        self.config = config
        self.year = config['year']
        self.gspread_credentials_name = config.get(
            'gspread_credentials_name', f'GSPREAD_CREDENTIALS_{self.year}'
        )
        self.dashboard_name = config['name']
        self.sheet_name = config['sheet_name']
        self.released_movies_df = table_to_df(
            config,
            'combined.base_query',
            columns=[
                'Rank',
                'Title',
                'Drafted By',
                'Revenue',
                'Scored Revenue',
                'Round Drafted',
                'Overall Pick',
                'Multiplier',
                'Domestic Revenue',
                'Domestic Revenue %',
                'Foreign Revenue',
                'Foreign Revenue %',
                'Better Pick',
                'Better Pick Scored Revenue',
                'First Seen Date',
                'Still In Theaters',
            ],
        )

        self.scoreboard_df = table_to_df(
            config,
            'dashboards.scoreboard',
            columns=[
                'Name',
                'Scored Revenue',
                '# Released',
                '# Optimal Picks',
                '% Optimal Picks',
                'Unadjusted Revenue',
            ],
        )

        self.worst_picks_df = table_to_df(
            config,
            'dashboards.worst_picks',
            columns=[
                'Rank',
                'Title',
                'Drafted By',
                'Overall Pick',
                'Number of Better Picks',
                'Missed Revenue',
            ],
        )

        self.dashboard_elements = [
            (
                self.scoreboard_df,
                'B4',
                load_format_config('src/assets/scoreboard_format.json'),
            ),
            (
                self.released_movies_df,
                'I4',
                load_format_config('src/assets/released_movies_format.json'),
            ),
        ]

        self.add_worst_picks = (
            len(self.released_movies_df) > len(self.scoreboard_df) + 3
            and len(self.worst_picks_df) > 1
        )

        self.better_picks_row_num = (
            5 + len(self.scoreboard_df) + 2
        )  # 3 for top rows, len of scoreboard and 2 rows of column names

        if self.add_worst_picks:
            self.worst_picks_df_height = (
                len(self.released_movies_df) - len(self.scoreboard_df) - 3
            )

            self.worst_picks_df = self.worst_picks_df.head(self.worst_picks_df_height)

            # replace the row number in the format config with the better picks row number
            self.dashboard_elements.append(
                (
                    self.worst_picks_df,
                    f'B{self.better_picks_row_num}',
                    {
                        key.replace('12', str(self.better_picks_row_num)).replace(
                            '13', str(self.better_picks_row_num + 1)
                        ): value
                        for key, value in load_format_config(
                            'src/assets/worst_picks_format.json'
                        ).items()
                    },
                )
            )

        self.setup_worksheet()

    def setup_worksheet(self) -> None:
        gspread_credentials_key = self.gspread_credentials_name
        gspread_credentials = os.getenv(gspread_credentials_key)

        if gspread_credentials is not None:
            credentials_dict = json.loads(gspread_credentials.replace('\n', '\\n'))
            gc = service_account_from_dict(credentials_dict)
        else:
            raise ValueError(
                f'{gspread_credentials_key} is not set or is invalid in the .env file.'
            )

        sh = gc.open(self.sheet_name)

        worksheet_title = 'Dashboard'
        worksheet = sh.worksheet(worksheet_title)

        sh.del_worksheet(worksheet)
        # 3 rows for title, 1 row for column titles, 1 row for footer
        self.sheet_height = len(self.released_movies_df) + 5
        worksheet = sh.add_worksheet(
            title=worksheet_title, rows=self.sheet_height, cols=25, index=1
        )
        self.worksheet = sh.worksheet(worksheet_title)


def update_dashboard(gsheet_dashboard: GoogleSheetDashboard) -> None:
    for element in gsheet_dashboard.dashboard_elements:
        df_to_sheet(
            df=element[0],
            worksheet=gsheet_dashboard.worksheet,
            location=element[1],
            format_dict=element[2] if len(element) > 2 else None,
        )

    dashboard_done_updating = (
        gsheet_dashboard.released_movies_df['Still In Theaters'].eq('No').all()
        and len(gsheet_dashboard.released_movies_df) > 0
    )

    log_string = f'Last Updated UTC\n{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'

    if dashboard_done_updating:
        log_string += '\nDashboard is done updating\nand can be removed from the etl'

    # Adding last updated header
    gsheet_dashboard.worksheet.update(
        values=[[log_string]],
        range_name='G2',
    )

    gsheet_dashboard.worksheet.format(
        'G2',
        {
            'horizontalAlignment': 'CENTER',
        },
    )

    # Columns are created with 12 point font, then auto resized and reduced to 10 point bold font
    gsheet_dashboard.worksheet.columns_auto_resize(1, 7)
    gsheet_dashboard.worksheet.columns_auto_resize(8, 23)

    gsheet_dashboard.worksheet.format(
        'B4:G4',
        {
            'horizontalAlignment': 'CENTER',
            'textFormat': {
                'fontSize': 10,
                'bold': True,
            },
        },
    )

    if gsheet_dashboard.add_worst_picks:
        gsheet_dashboard.worksheet.format(
            f'B{gsheet_dashboard.better_picks_row_num}:G{gsheet_dashboard.better_picks_row_num}',
            {
                'horizontalAlignment': 'CENTER',
                'textFormat': {
                    'fontSize': 10,
                    'bold': True,
                },
            },
        )

    gsheet_dashboard.worksheet.format(
        'I4:X4',
        {
            'horizontalAlignment': 'CENTER',
            'textFormat': {
                'fontSize': 10,
                'bold': True,
            },
        },
    )

    for i in range(5, gsheet_dashboard.sheet_height):
        if gsheet_dashboard.worksheet.acell(f'V{i}').value == '$0':
            gsheet_dashboard.worksheet.update(values=[['']], range_name=f'V{i}')

    # resizing spacer columns
    spacer_columns = ['A', 'H', 'Y']
    for column in spacer_columns:
        gsf.set_column_width(gsheet_dashboard.worksheet, column, 25)

    # for some reason the auto resize still cuts off some of the title
    title_columns = ['J', 'U']

    if gsheet_dashboard.add_worst_picks:
        title_columns.append('C')

    for column in title_columns:
        gsf.set_column_width(gsheet_dashboard.worksheet, column, 284)

    # revenue columns will also get cut off
    revenue_columns = ['L', 'M', 'R', 'S']
    for column in revenue_columns:
        gsf.set_column_width(gsheet_dashboard.worksheet, column, 120)

    # gets resized wrong and have to do it manually
    gsf.set_column_width(gsheet_dashboard.worksheet, 'R', 142)
    gsf.set_column_width(gsheet_dashboard.worksheet, 'W', 104)
    gsf.set_column_width(gsheet_dashboard.worksheet, 'X', 106)

    if dashboard_done_updating:
        gsf.set_column_width(gsheet_dashboard.worksheet, 'C', 174)


def update_titles(gsheet_dashboard: GoogleSheetDashboard) -> None:
    gsheet_dashboard.worksheet.update(
        values=[[gsheet_dashboard.dashboard_name]], range_name='B2'
    )
    gsheet_dashboard.worksheet.format(
        'B2',
        {'horizontalAlignment': 'CENTER', 'textFormat': {'fontSize': 20, 'bold': True}},
    )
    gsheet_dashboard.worksheet.merge_cells('B2:F2')
    gsheet_dashboard.worksheet.update(values=[['Released Movies']], range_name='I2')
    gsheet_dashboard.worksheet.format(
        'I2',
        {'horizontalAlignment': 'CENTER', 'textFormat': {'fontSize': 20, 'bold': True}},
    )
    gsheet_dashboard.worksheet.merge_cells('I2:X2')

    if gsheet_dashboard.add_worst_picks:
        worst_picks_row_num = gsheet_dashboard.better_picks_row_num - 1
        gsheet_dashboard.worksheet.update(
            values=[['Worst Picks']], range_name=f'B{worst_picks_row_num}'
        )
        gsheet_dashboard.worksheet.format(
            f'B{worst_picks_row_num}',
            {'horizontalAlignment': 'CENTER', 'textFormat': {'bold': True}},
        )
        gsheet_dashboard.worksheet.merge_cells(
            f'B{worst_picks_row_num}:G{worst_picks_row_num}'
        )


def apply_conditional_formatting(gsheet_dashboard: GoogleSheetDashboard) -> None:
    still_in_theater_rule = gsf.ConditionalFormatRule(
        ranges=[gsf.GridRange.from_a1_range('X5:X', gsheet_dashboard.worksheet)],
        booleanRule=gsf.BooleanRule(
            condition=gsf.BooleanCondition('TEXT_EQ', ['Yes']),
            format=gsf.CellFormat(
                backgroundColor=gsf.Color(0, 0.9, 0),
            ),
        ),
    )

    rules = gsf.get_conditional_format_rules(gsheet_dashboard.worksheet)
    rules.append(still_in_theater_rule)
    rules.save()

    logging.info('Dashboard updated and formatted')


def log_missing_movies(gsheet_dashboard: GoogleSheetDashboard) -> None:
    draft_df = table_to_df(
        gsheet_dashboard.config,
        'cleaned.drafter',
    )
    released_movies = [
        str(movie) for movie in gsheet_dashboard.released_movies_df['Title'].tolist()
    ]
    drafted_movies = [str(movie) for movie in draft_df['movie'].tolist()]
    movies_missing_from_scoreboard = list(set(drafted_movies) - set(released_movies))

    if movies_missing_from_scoreboard:
        logging.info(
            'The following movies are missing from the scoreboard and should be added to the manual_adds.csv file:'
        )
        logging.info(', '.join(sorted(movies_missing_from_scoreboard)))
    else:
        logging.info('All movies are on the scoreboard.')


def log_min_revenue_info(gsheet_dashboard: GoogleSheetDashboard, config: Dict) -> None:
    duckdb_con = DuckDBConnection(config)

    min_revenue_of_most_recent_data = duckdb_con.query(
        f'''
        with most_recent_data as (
            select title, revenue
            from raw.box_office_mojo_dump where year_part = {gsheet_dashboard.year}
            qualify rank() over (order by loaded_date desc) = 1
            order by 2 desc
        )

        select title, revenue
        from most_recent_data qualify row_number() over (order by revenue asc) = 1;
        '''
    ).fetchnumpy()['revenue'][0]

    logging.info(
        f'Minimum revenue of most recent data: {min_revenue_of_most_recent_data}'
    )

    movies_under_min_revenue = (
        duckdb_con.query(
            f'''
            with raw_data as (
                select title, revenue
                from raw.box_office_mojo_dump
                where year_part = {gsheet_dashboard.year}
                qualify row_number() over (partition by title order by loaded_date desc) = 1
            )

            select raw_data.title from raw_data
            inner join combined.base_query as base_query
                on raw_data.title = base_query.title
            where raw_data.revenue <= {min_revenue_of_most_recent_data}
            '''
        )
        .fetchnumpy()['title']
        .tolist()
    )

    duckdb_con.close()

    if movies_under_min_revenue:
        logging.info(
            'The most recent records for the following movies are under the minimum revenue of the most recent data pull'
            + ' and may not have the correct revenue and should be added to the manual_adds.csv file:'
        )
        logging.info(', '.join(sorted(movies_under_min_revenue)))
    else:
        logging.info(
            'All movies are above the minimum revenue of the most recent data pull.'
        )


def load(config: Dict) -> None:
    gsheet_dashboard = GoogleSheetDashboard(config)

    update_dashboard(gsheet_dashboard)
    update_titles(gsheet_dashboard)
    apply_conditional_formatting(gsheet_dashboard)
    log_missing_movies(gsheet_dashboard)
    log_min_revenue_info(gsheet_dashboard, config)
