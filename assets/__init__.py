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
