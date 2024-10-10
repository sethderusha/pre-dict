import sqlite3
from nba_api.stats.endpoints import leaguegamefinder

def clean_abbreviation(abbr):
    return abbr.strip().upper()

def create_team_tables(conn, cursor):
    # Fetch team data
    teams = cursor.execute('SELECT id FROM team').fetchall()

    # Iterate over each team
    for team in teams:
        id = team[0]
        # Fetch games for the team
        gamefinder = leaguegamefinder.LeagueGameFinder(team_id_nullable=id)
        games = gamefinder.get_data_frames()[0]


        # Create a table for the team's games
        table_name = f"{id}_games"
        cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS "{table_name}" (
            game_id TEXT PRIMARY KEY,
            game_date TEXT,
            team_id INTEGER,
            matchup TEXT,
            PTS INTEGER,
            FGM INTEGER,
            FGA INTEGER,
            FG_PCT FLOAT,
            FG3M INTEGER,
            FG3A INTEGER,
            FG3_PCT FLOAT,
            FTM INTEGER,
            FTA INTEGER,
            FT_PCT FLOAT,
            OREB INTEGER,
            DREB INTEGER,
            AST INTEGER,
            STL INTEGER,
            BLK INTEGER,
            TOV INTEGER,
            PF INTEGER
        )
        ''')
        
        # Insert game data into the team's table
        for _, game in games.iterrows():
            cursor.execute(f'''
            INSERT OR REPLACE INTO "{table_name}" (
                game_id, game_date, team_id, matchup, PTS, FGM, FGA, FG_PCT, FG3M, FG3A, FG3_PCT,
                FTM, FTA, FT_PCT, OREB, DREB, AST, STL, BLK, TOV, PF
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                game['GAME_ID'], game['GAME_DATE'], game['TEAM_ID'], game['MATCHUP'], game['PTS'], game['FGM'], game['FGA'],
                game['FG_PCT'], game['FG3M'], game['FG3A'], game['FG3_PCT'], game['FTM'], game['FTA'], game['FT_PCT'],
                game['OREB'], game['DREB'], game['AST'], game['STL'], game['BLK'], game['TOV'], game['PF']
            ))
    print("team tables created")

def create_games_table(conn, cursor):
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS games (
        game_date TEXT,
        home_team_id INTEGER,
        home_team_abbr TEXT,
        visitor_team_id INTEGER,
        visitor_team_abbr TEXT,
        home_pts INTEGER,
        visitor_pts INTEGER,
        home_fgm INTEGER,
        visitor_fgm INTEGER,
        home_fga INTEGER,
        visitor_fga INTEGER,
        home_fg_pct FLOAT,
        visitor_fg_pct FLOAT,
        home_fg3m INTEGER,
        visitor_fg3m INTEGER,
        home_fg3a INTEGER,
        visitor_fg3a INTEGER,
        home_fg3_pct FLOAT,
        visitor_fg3_pct FLOAT,
        home_ftm INTEGER,
        visitor_ftm INTEGER,
        home_fta INTEGER,
        visitor_fta INTEGER,
        home_ft_pct FLOAT,
        visitor_ft_pct FLOAT,
        home_oreb INTEGER,
        visitor_oreb INTEGER,
        home_dreb INTEGER,
        visitor_dreb INTEGER,
        home_ast INTEGER,
        visitor_ast INTEGER,
        home_stl INTEGER,
        visitor_stl INTEGER,
        home_blk INTEGER,
        visitor_blk INTEGER,
        home_tov INTEGER,
        visitor_tov INTEGER,
        home_pf INTEGER,
        visitor_pf INTEGER
    )
    ''')
    conn.commit()

def combine_games(conn, cursor):
    teams = cursor.execute('SELECT * FROM team').fetchall()

    for team in teams:
        team_id = team[0]
        table_name = f"{team_id}_games"
        games = cursor.execute(f'SELECT * FROM "{table_name}"').fetchall()

        for game in games:
            game_id, game_date, team_id, matchup, pts, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct, ftm, fta, ft_pct, oreb, dreb, ast, stl, blk, tov, pf = game
            
            # Extract home and visitor team abbreviations from matchup
            if "@" in matchup:
                this_team_abbr, opponent_team_abbr = matchup.split(" @ ")
                is_home = False
            else:
                this_team_abbr, opponent_team_abbr = matchup.split(" vs. ")
                is_home = True

            # Clean abbreviations
            this_team_abbr = this_team_abbr.strip().upper()
            opponent_team_abbr = opponent_team_abbr.strip().upper()

            # Check if the game already exists in the database
            existing_game = cursor.execute('''
                SELECT * FROM games WHERE game_date = ? AND 
                ((home_team_abbr = ? AND visitor_team_abbr = ?) OR 
                (home_team_abbr = ? AND visitor_team_abbr = ?))
            ''', (game_date, this_team_abbr, opponent_team_abbr, opponent_team_abbr, this_team_abbr)).fetchone()

            if existing_game:
                if is_home:
                    cursor.execute('''
                    UPDATE games SET
                        home_team_id = ?, home_pts = ?, home_fgm = ?, home_fga = ?, home_fg_pct = ?, 
                        home_fg3m = ?, home_fg3a = ?, home_fg3_pct = ?, home_ftm = ?, home_fta = ?, 
                        home_ft_pct = ?, home_oreb = ?, home_dreb = ?, home_ast = ?, home_stl = ?, 
                        home_blk = ?, home_tov = ?, home_pf = ?
                    WHERE game_date = ? AND home_team_abbr = ? AND visitor_team_abbr = ?
                    ''', (
                        team_id, pts, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct, ftm, fta, ft_pct, 
                        oreb, dreb, ast, stl, blk, tov, pf,
                        game_date, this_team_abbr, opponent_team_abbr
                    ))
                else:
                    cursor.execute('''
                    UPDATE games SET
                        visitor_team_id = ?, visitor_pts = ?, visitor_fgm = ?, visitor_fga = ?, visitor_fg_pct = ?, 
                        visitor_fg3m = ?, visitor_fg3a = ?, visitor_fg3_pct = ?, visitor_ftm = ?, visitor_fta = ?, 
                        visitor_ft_pct = ?, visitor_oreb = ?, visitor_dreb = ?, visitor_ast = ?, visitor_stl = ?, 
                        visitor_blk = ?, visitor_tov = ?, visitor_pf = ?
                    WHERE game_date = ? AND home_team_abbr = ? AND visitor_team_abbr = ?
                    ''', (
                        team_id, pts, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct, ftm, fta, ft_pct, 
                        oreb, dreb, ast, stl, blk, tov, pf,
                        game_date, opponent_team_abbr, this_team_abbr
                    ))
            else:
                if is_home:
                    cursor.execute('''
                    INSERT INTO games (
                        game_date, home_team_abbr, visitor_team_abbr, home_team_id, home_pts, home_fgm, home_fga, home_fg_pct, home_fg3m, 
                        home_fg3a, home_fg3_pct, home_ftm, home_fta, home_ft_pct, home_oreb, home_dreb, home_ast, home_stl, 
                        home_blk, home_tov, home_pf
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        game_date, this_team_abbr, opponent_team_abbr, team_id, pts, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct, ftm, fta, ft_pct, 
                        oreb, dreb, ast, stl, blk, tov, pf
                    ))
                else:
                    cursor.execute('''
                    INSERT INTO games (
                        game_date, home_team_abbr, visitor_team_abbr, visitor_team_id, visitor_pts, visitor_fgm, visitor_fga, visitor_fg_pct, visitor_fg3m, 
                        visitor_fg3a, visitor_fg3_pct, visitor_ftm, visitor_fta, visitor_ft_pct, visitor_oreb, visitor_dreb, visitor_ast, visitor_stl, 
                        visitor_blk, visitor_tov, visitor_pf
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        game_date, opponent_team_abbr, this_team_abbr, team_id, pts, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct, ftm, fta, ft_pct, 
                        oreb, dreb, ast, stl, blk, tov, pf
                    ))

            # Log the result to ensure it worked
            row_count = cursor.execute('SELECT COUNT(*) FROM games WHERE game_date = ? AND ((home_team_abbr = ? AND visitor_team_abbr = ?) OR (home_team_abbr = ? AND visitor_team_abbr = ?))', 
                                       (game_date, this_team_abbr, opponent_team_abbr, opponent_team_abbr, this_team_abbr)).fetchone()[0]
            if row_count == 0:
                print(f"Error: Failed to add or update game on {game_date} for {this_team_abbr}.")
    print("games table created")


def execute_comprehensive_stats_update(conn, cursor):

    sql_commands = [
        # Update rest days
        """
        WITH previous_games AS (
          SELECT 
            game_date, 
            home_team_id,
            visitor_team_id,
            LAG(game_date) OVER (PARTITION BY home_team_id ORDER BY game_date) as home_prev_game,
            LAG(game_date) OVER (PARTITION BY visitor_team_id ORDER BY game_date) as visitor_prev_game
          FROM games
        )
        UPDATE games
        SET 
          home_team_rest_days = CASE 
            WHEN JULIANDAY(games.game_date) - JULIANDAY(previous_games.home_prev_game) > 180 THEN 180
            ELSE MIN(JULIANDAY(games.game_date) - JULIANDAY(previous_games.home_prev_game), 180)
          END,
          visitor_team_rest_days = CASE 
            WHEN JULIANDAY(games.game_date) - JULIANDAY(previous_games.visitor_prev_game) > 180 THEN 180
            ELSE MIN(JULIANDAY(games.game_date) - JULIANDAY(previous_games.visitor_prev_game), 180)
          END
        FROM previous_games
        WHERE games.game_date = previous_games.game_date
          AND games.home_team_id = previous_games.home_team_id
          AND games.visitor_team_id = previous_games.visitor_team_id;
        """,

        # Update win columns
        """
        UPDATE games
        SET 
          home_team_win = CASE WHEN home_pts > visitor_pts THEN 1 ELSE 0 END,
          visitor_team_win = CASE WHEN visitor_pts > home_pts THEN 1 ELSE 0 END;
        """,

        # 1. Point Differential
        "UPDATE games SET point_differential = home_pts - visitor_pts;",

        # 2. Effective Field Goal Percentage (eFG%)
        """
        UPDATE games 
        SET home_efg = (home_fgm + 0.5 * home_fg3m) / home_fga,
            visitor_efg = (visitor_fgm + 0.5 * visitor_fg3m) / visitor_fga;
        """,

        # 3. True Shooting Percentage (TS%)
        """
        UPDATE games 
        SET home_ts = home_pts / (2 * (home_fga + 0.44 * home_fta)),
            visitor_ts = visitor_pts / (2 * (visitor_fga + 0.44 * visitor_fta));
        """,

        # 4. Offensive and Defensive Rebounds
        """
        UPDATE games 
        SET home_treb = home_oreb + home_dreb,
            visitor_treb = visitor_oreb + visitor_dreb;
        """,

        # 5. Assist to Turnover Ratio
        """
        UPDATE games 
        SET home_ast_to_ratio = CASE WHEN home_tov > 0 THEN CAST(home_ast AS FLOAT) / home_tov ELSE NULL END,
            visitor_ast_to_ratio = CASE WHEN visitor_tov > 0 THEN CAST(visitor_ast AS FLOAT) / visitor_tov ELSE NULL END;
        """,

        # 6. Possessions (estimated)
        """
        UPDATE games 
        SET home_possessions = 0.5 * ((home_fga + 0.4 * home_fta - 1.07 * (home_oreb / (home_oreb + visitor_dreb)) * (home_fga - home_fgm) + home_tov) + 
                                      (visitor_fga + 0.4 * visitor_fta - 1.07 * (visitor_oreb / (visitor_oreb + home_dreb)) * (visitor_fga - visitor_fgm) + visitor_tov)),
            visitor_possessions = 0.5 * ((visitor_fga + 0.4 * visitor_fta - 1.07 * (visitor_oreb / (visitor_oreb + home_dreb)) * (visitor_fga - visitor_fgm) + visitor_tov) + 
                                         (home_fga + 0.4 * home_fta - 1.07 * (home_oreb / (home_oreb + visitor_dreb)) * (home_fga - home_fgm) + home_tov));
        """,

        # 7. Offensive and Defensive Rating
        """
        UPDATE games 
        SET home_ortg = (home_pts / home_possessions) * 100,
            home_drtg = (visitor_pts / visitor_possessions) * 100,
            visitor_ortg = (visitor_pts / visitor_possessions) * 100,
            visitor_drtg = (home_pts / home_possessions) * 100;
        """,

        # 8. Pace
        """
        UPDATE games 
        SET pace = 48 * ((home_possessions + visitor_possessions) / (2 * 48));
        """,

        # 9. Four Factors
        """
        UPDATE games 
        SET home_efg_pct = (CAST(home_fgm AS FLOAT) + 0.5 * CAST(home_fg3m AS FLOAT)) / NULLIF(CAST(home_fga AS FLOAT), 0),
            home_tov_pct = CAST(home_tov AS FLOAT) / NULLIF((CAST(home_fga AS FLOAT) + 0.44 * CAST(home_fta AS FLOAT) + CAST(home_tov AS FLOAT)), 0),
            home_orb_pct = CAST(home_oreb AS FLOAT) / NULLIF((CAST(home_oreb AS FLOAT) + CAST(visitor_dreb AS FLOAT)), 0),
            home_ft_rate = CAST(home_fta AS FLOAT) / NULLIF(CAST(home_fga AS FLOAT), 0),
            visitor_efg_pct = (CAST(visitor_fgm AS FLOAT) + 0.5 * CAST(visitor_fg3m AS FLOAT)) / NULLIF(CAST(visitor_fga AS FLOAT), 0),
            visitor_tov_pct = CAST(visitor_tov AS FLOAT) / NULLIF((CAST(visitor_fga AS FLOAT) + 0.44 * CAST(visitor_fta AS FLOAT) + CAST(visitor_tov AS FLOAT)), 0),
            visitor_orb_pct = CAST(visitor_oreb AS FLOAT) / NULLIF((CAST(visitor_oreb AS FLOAT) + CAST(home_dreb AS FLOAT)), 0),
            visitor_ft_rate = CAST(visitor_fta AS FLOAT) / NULLIF(CAST(visitor_fga AS FLOAT), 0);
        """
    ]

    for command in sql_commands:
        cursor.execute(command)
    print("advanced stats updated")

def load_games():
    # Connect to the SQLite database
    conn = sqlite3.connect('nba.sqlite')
    cursor = conn.cursor()

    # Create the team tables
    create_team_tables(conn, cursor)

    # Create the games table
    create_games_table(conn, cursor)

    # Combine the games
    combine_games(conn, cursor)

    # Execute the comprehensive stats update
    execute_comprehensive_stats_update(conn, cursor)

    # Commit the changes and close the connection
    conn.commit()
    conn.close()

load_games()