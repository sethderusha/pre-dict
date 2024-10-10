pre-dict

This program was designed to predict NBA game results and point differentials. It uses historical data and the RandomForestClassifier/Regressor to make these predictions.

Installation:

Run pip3 -r requirements.txt

Running:

Fill a .txt file with games on each row in this format:
home_team_abbreviation,away_team_abbreviation,home_team_days_rest,away_team_days_rest,moneyline,spread

To run python3 pre-dict.py {path to the file above}
