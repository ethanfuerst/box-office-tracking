def get_scoreboard_format():
    return {
        "B4:G4": {  # Column Titles
            "horizontalAlignment": "CENTER",
            "textFormat": {
                "fontSize": 10,
                "bold": True,
            },
        },
        "B5:B9": {  # Name
            "horizontalAlignment": "LEFT",
        },
        "C5:C9": {  # Scored Revenue
            "horizontalAlignment": "RIGHT",
            "numberFormat": {
                "type": "CURRENCY",
                "pattern": "$#,##0",
            },
        },
        "F5:F9": {  # % Optimal Picks
            "horizontalAlignment": "RIGHT",
            "numberFormat": {
                "type": "PERCENT",
                "pattern": "#0.0#%",
            },
        },
        "G5:G9": {  # Unadjusted Revenue
            "horizontalAlignment": "RIGHT",
            "numberFormat": {
                "type": "CURRENCY",
                "pattern": "$#,##0",
            },
        },
    }


def get_released_movies_format():
    return {
        "I4:V4": {  # Column Titles
            "horizontalAlignment": "CENTER",
            "textFormat": {
                "fontSize": 10,
                "bold": True,
            },
        },
        "I5:K": {  # Rank to Drafted By
            "horizontalAlignment": "LEFT",
        },
        "L5:M": {  # Revenue to Scored Revenue
            "horizontalAlignment": "LEFT",
            "numberFormat": {
                "type": "CURRENCY",
                "pattern": "$#,##0",
            },
        },
        "N5:P": {  # Round Drafted to Multiplier
            "horizontalAlignment": "RIGHT",
        },
        "Q5:Q": {  # Domestic Revenue
            "horizontalAlignment": "RIGHT",
            "numberFormat": {
                "type": "CURRENCY",
                "pattern": "$#,##0",
            },
        },
        "R5:R": {  # Domestic Revenue %
            "horizontalAlignment": "RIGHT",
            "numberFormat": {
                "type": "PERCENT",
                "pattern": "#0.0#%",
            },
        },
        "S5:S": {  # Foreign Revenue
            "horizontalAlignment": "RIGHT",
            "numberFormat": {
                "type": "CURRENCY",
                "pattern": "$#,##0",
            },
        },
        "T5:T": {  # Foreign Revenue %
            "horizontalAlignment": "RIGHT",
            "numberFormat": {
                "type": "PERCENT",
                "pattern": "#0.0#%",
            },
        },
        "V5:V": {  # Better Pick Scored Revenue
            "horizontalAlignment": "RIGHT",
            "numberFormat": {
                "type": "CURRENCY",
                "pattern": "$#,##0",
            },
        },
    }
