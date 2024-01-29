def get_scoreboard_format():
    return {
        "B4:F4": {
            "horizontalAlignment": "CENTER",
            "textFormat": {
                "fontSize": 10,
                "bold": True,
            },
        },
        "B5:B9": {
            "horizontalAlignment": "LEFT",
        },
        "C5:C9": {
            "horizontalAlignment": "RIGHT",
            "numberFormat": {
                "type": "CURRENCY",
                "pattern": "$#,##0",
            },
        },
        "E5:E9": {
            "horizontalAlignment": "RIGHT",
            "numberFormat": {
                "type": "CURRENCY",
                "pattern": "$#,##0",
            },
        },
        "F5:F9": {
            "horizontalAlignment": "RIGHT",
            "numberFormat": {
                "type": "PERCENT",
                "pattern": "#0.0#%",
            },
        },
    }


def get_released_movies_format():
    return {
        "H4:R4": {
            "horizontalAlignment": "CENTER",
            "textFormat": {
                "fontSize": 10,
                "bold": True,
            },
        },
        "H5:H": {
            "horizontalAlignment": "LEFT",
        },
        "I5:I": {
            "horizontalAlignment": "LEFT",
        },
        "J5:J": {
            "horizontalAlignment": "LEFT",
        },
        "K5:K": {
            "horizontalAlignment": "LEFT",
            "numberFormat": {
                "type": "DATE",
                "pattern": "m/d/yy",
            },
        },
        "L5:L": {
            "horizontalAlignment": "LEFT",
            "numberFormat": {
                "type": "NUMBER",
                "pattern": "#",
            },
        },
        "M5:O": {
            "horizontalAlignment": "RIGHT",
            "numberFormat": {
                "type": "CURRENCY",
                "pattern": "$#,##0",
            },
        },
        "P5:P": {
            "horizontalAlignment": "RIGHT",
            "numberFormat": {
                "type": "PERCENT",
                "pattern": "#0.0#%",
            },
        },
        "Q5:Q": {
            "horizontalAlignment": "RIGHT",
            "numberFormat": {
                "type": "NUMBER",
                "pattern": "#.0#",
            },
        },
        "R5:R": {
            "horizontalAlignment": "RIGHT",
            "numberFormat": {"type": "NUMBER"},
        },
    }
