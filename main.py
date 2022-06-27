import pymongo
import csv

def insert_data(db):
    
    betting_money = db['betting_money']
    betting_spread = db['betting_spread']
    betting_totals = db['betting_totals']
    games = db['games']
    players = db['players']
    players_game_stats = db['players_game_stats']
    teams = db['teams']

    
    with open('data/archive/nba_betting_money_line.csv') as file:
        csv_reader = csv.DictReader(file)
        list_dict = []
        for i in csv_reader:
            new_dict = {}
            for key, value in i.items():
                if key == "price1" or key == "price2":
                    new_dict[key] = float(value)
                else:
                    new_dict[key] = value
            list_dict.append(new_dict)

        betting_money.insert_many(list_dict)
        

    with open('data/archive/nba_betting_spread.csv') as file:
        csv_reader = csv.DictReader(file)
        list_dict = []
        for i in csv_reader:
            new_dict = {}
            for key, value in i.items():
                if key == "price1" or key == "price2" or key == "spread1" or key == "spread2":
                    new_dict[key] = float(value)
                else:
                    new_dict[key] = value
            list_dict.append(new_dict)

        betting_spread.insert_many(list_dict)


    with open('data/archive/nba_betting_totals.csv') as file:
        csv_reader = csv.DictReader(file)
        list_dict = []
        for i in csv_reader:
            new_dict = {}
            for key, value in i.items():
                if key == "price1" or key == "price2" or key == "total1" or key == "total2":
                    new_dict[key] = float(value)
                else:
                    new_dict[key] = value
            list_dict.append(new_dict)

        betting_totals.insert_many(list_dict)
        

    with open('data/archive/nba_games_all.csv') as file:
        csv_reader = csv.DictReader(file)
        list_dict = []
        for i in csv_reader:
            new_dict = {}
            for key, value in i.items():
                if key in ['w_pct', 'fg_pct', 'fg3_pct', 'ft_pct'] and value not in ['', ' ']:
                    new_dict[key] = float(value)
                elif key in ['w', 'l', 'min', 'fgm', 'fga', 'fg3m', 'fg3a', 'ftm', 'fta', 'oreb', 'dreb', 'reb', 'ast', 'stl', 'blk', 'tov', 'pf', 'pts', 'season_year'] and value not in ['', ' ']:
                    new_dict[key] = int(value)
                else:
                    new_dict[key] = value
            list_dict.append(new_dict)

        games.insert_many(list_dict)

    with open('data/archive/nba_players_all.csv') as file:
        csv_reader = csv.DictReader(file)
        list_dict = []
        for i in csv_reader:
            new_dict = {}
            for key, value in i.items():
                if key in ['height'] and value not in ['', ' ']:
                    new_dict[key] = float(value)
                elif key in ['from_year', 'to_year', 'draft_year', 'draft_round', 'draft_num', 'height_feet', 'height_inches', 'weight', 'season_exp'] and value not in ['', ' ']:
                    new_dict[key] = int(value)
                else:
                    new_dict[key] = value
            list_dict.append(new_dict)

        players.insert_many(list_dict)

    with open('data/archive/nba_players_game_stats.csv') as file:
        csv_reader = csv.DictReader(file)
        list_dict = []
        for i in csv_reader:
            new_dict = {}
            for key, value in i.items():
                if key in ['fg_pct', 'fg3_pct', 'ft_pct'] and value not in ['', ' ']:
                    new_dict[key] = float(value)
                elif key in ['min', 'fgm', 'fga', 'fg3m', 'fg3a', 'ftm', 'fta', 'oreb', 'dreb', 'reb', 'ast'] and value not in ['', ' ']:
                    new_dict[key] = int(value)
                else:
                    new_dict[key] = value
            list_dict.append(new_dict)

        players_game_stats.insert_many(list_dict)
        

    with open('data/archive/nba_teams_all.csv') as file:
        csv_reader = csv.DictReader(file)
        list_dict = []
        for i in csv_reader:
            new_dict = {}
            for key, value in i.items():
                if key in ['min_year', 'max_year'] and value not in ['', ' ']:
                    new_dict[key] = int(value)
                else:
                    new_dict[key] = value
            list_dict.append(new_dict)

        teams.insert_many(list_dict)


if __name__ == "__main__":
    client = pymongo.MongoClient("mongodb://localhost:27017/")

    database = client['database']
    insert_data(database)
    print(client.list_database_names())
