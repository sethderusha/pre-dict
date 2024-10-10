import sqlite3
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.preprocessing import StandardScaler
import sys
from get_games import load_games

# Update this to match your .sqlite file name
DB_NAME = 'nba.sqlite'

def get_home_games(team_id, num_games=10):
    query = f"""
    SELECT * FROM games
    WHERE home_team_id = {team_id}
    ORDER BY game_date DESC
    LIMIT {num_games}
    """
    with sqlite3.connect(DB_NAME) as conn:
        return pd.read_sql_query(query, conn)
    
def get_visitor_games(team_id, num_games=10):
    query = f"""
    SELECT * FROM games
    WHERE visitor_team_id = {team_id}
    ORDER BY game_date DESC
    LIMIT {num_games}
    """
    with sqlite3.connect(DB_NAME) as conn:
        return pd.read_sql_query(query, conn)

def get_matchup_stats(home_team_id, visitor_team_id, num_games=10):
    query = f"""
    SELECT * FROM games
    WHERE home_team_id = {home_team_id} AND visitor_team_id = {visitor_team_id}
    ORDER BY game_date DESC
    LIMIT {num_games}
    """
    with sqlite3.connect(DB_NAME) as conn:
        return pd.read_sql_query(query, conn)

def clean_data(df):
    # Remove rows with NaN values
    df_cleaned = df.dropna()
    
    return df_cleaned

def train_models():
    query = "SELECT * FROM games"
    
    with sqlite3.connect(DB_NAME) as conn:
        df = pd.read_sql_query(query, conn)
    
    # Clean the data
    df = clean_data(df)
    
    feature_columns = [
        'pts', 'fgm', 'fga', 'fg_pct', 'fg3m', 'fg3a', 'fg3_pct',
        'ftm', 'fta', 'ft_pct', 'oreb', 'dreb', 'ast', 'stl', 'blk',
        'tov', 'pf', 'team_rest_days', 'efg', 'ts', 'treb', 'ast_to_ratio', 'possessions',
        'ortg', 'drtg', 'efg_pct', 'tov_pct', 'orb_pct', 'ft_rate'
    ]
    
    home_columns = [f'home_{col}' for col in feature_columns]
    visitor_columns = [f'visitor_{col}' for col in feature_columns]
    X = df[home_columns + visitor_columns]
    y_win = df['home_team_win']
    y_diff = df['home_pts'] - df['visitor_pts']  # Point differential
    
    # Check for NaN values
    if X.isna().any().any():
        print("Warning: X contains NaN values")
        print(X.isna().sum())
    if y_win.isna().any():
        print("Warning: y_win contains NaN values")
    if y_diff.isna().any():
        print("Warning: y_diff contains NaN values")
    
    scaler = StandardScaler()
    X_scaled = pd.DataFrame(scaler.fit_transform(X), columns=X.columns)
    
    X_train, X_test, y_win_train, y_win_test, y_diff_train, y_diff_test = train_test_split(
        X_scaled, y_win, y_diff, test_size=0.2, random_state=42)
    
    # Train win probability model
    win_model = RandomForestClassifier(n_estimators=100, random_state=42)
    win_model.fit(X_train, y_win_train)
    
    # Train point differential model
    diff_model = RandomForestRegressor(n_estimators=100, random_state=42)
    diff_model.fit(X_train, y_diff_train)
    
    print(f"Win probability model accuracy: {win_model.score(X_test, y_win_test):.2f}")
    print(f"Point differential model R² score: {diff_model.score(X_test, y_diff_test):.2f}")
    
    return win_model, diff_model, scaler, feature_columns

def predict_game(win_model, diff_model, scaler, feature_columns, home_team_id, visitor_team_id, home_rest_days, visitor_rest_days):
    home_games = get_home_games(home_team_id, 20)
    visitor_games = get_visitor_games(visitor_team_id, 20)
    matchup_games = get_matchup_stats(home_team_id, visitor_team_id, 4)
    
    home_stats = home_games[[f'home_{col}' for col in feature_columns]].mean()
    visitor_stats = visitor_games[[f'visitor_{col}' for col in feature_columns]].mean()
    
    # Calculate matchup stats
    if not matchup_games.empty:
        matchup_home_stats = matchup_games[[f'home_{col}' for col in feature_columns]].mean()
        matchup_visitor_stats = matchup_games[[f'visitor_{col}' for col in feature_columns]].mean()
        
        # Combine regular stats with matchup stats (you can adjust the weights)
        home_stats = 0.8 * home_stats + 0.2 * matchup_home_stats
        visitor_stats = 0.8 * visitor_stats + 0.2 * matchup_visitor_stats
    
    # Update rest days
    home_stats['home_team_rest_days'] = home_rest_days
    visitor_stats['visitor_team_rest_days'] = visitor_rest_days
    
    features = pd.concat([home_stats, visitor_stats])
    features = features.to_frame().T  # Convert to a single-row DataFrame
    features.columns = [f'home_{col}' for col in feature_columns] + [f'visitor_{col}' for col in feature_columns]
    
    # Check for NaN values
    if features.isna().any().any():
        print("Warning: features contain NaN values")
        print(features.isna().sum())
    
    features_scaled = pd.DataFrame(scaler.transform(features), columns=features.columns)
    win_probability = win_model.predict_proba(features_scaled)[0][1]
    point_differential = diff_model.predict(features_scaled)[0]
    
    return win_probability, point_differential

def get_team_id(team_abbr):
    query = f"SELECT DISTINCT home_team_id FROM games WHERE home_team_abbr = '{team_abbr}'"
    with sqlite3.connect(DB_NAME) as conn:
        result = pd.read_sql_query(query, conn)
    if result.empty:
        raise ValueError(f"Team '{team_abbr}' not found in the database.")
    return result.iloc[0]['home_team_id']

def process_input_file(file_path, win_model, diff_model, scaler, feature_columns):
    with open(file_path, 'r') as file:
        for line in file:
            try:
                home_team, visitor_team, home_rest_days, visitor_rest_days, moneyline, spread = line.strip().split(',')
                home_rest_days = int(home_rest_days)
                visitor_rest_days = int(visitor_rest_days)
                
                home_team_id = get_team_id(home_team)
                visitor_team_id = get_team_id(visitor_team)
                
                win_probability, point_differential = predict_game(win_model, diff_model, scaler, feature_columns, 
                                                                   home_team_id, visitor_team_id, home_rest_days, visitor_rest_days)
                
                print(f"\n{home_team} vs {visitor_team}: ML {moneyline}, {spread}")
                print(f"probability: {win_probability:.2f}")
                print(f"point diff: {point_differential:.1f}")
            except ValueError as e:
                print(f"Error processing line: {line.strip()}")
                print(f"Error message: {str(e)}")
            except Exception as e:
                print(f"Unexpected error processing line: {line.strip()}")
                print(f"Error message: {str(e)}")

# Main execution
if __name__ == "__main__":
    load_games()
    if len(sys.argv) != 2:
        print("Usage: python script.py <input_file_path>")
        sys.exit(1)

    input_file = sys.argv[1]

    try:
        print("Training models...")
        win_model, diff_model, scaler, feature_columns = train_models()
        
        print("\nProcessing input file...")
        process_input_file(input_file, win_model, diff_model, scaler, feature_columns)
    except Exception as e:
        print(f"An error occurred: {e}")
        print("Please check your database and input file to ensure they contain the necessary data.")
        raise  # Re-raise the exception to see the full traceback