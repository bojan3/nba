//season_totals
db.season_totals.insertMany(db.games.aggregate([{
    $group: {"_id": {"season": "$season_year"},
         "points": {$sum: "$pts"},
         "assists": {$sum: "$ast"},
         "rebounds": {$sum: "$reb"},
         "blocks": {$sum: "$blk"},
         "steals": {$sum: "$stl"},
         "3 pointers": {$sum: "$fg3m"},
         "max 3s": {$max: "$fg3m"},
         "games": {$sum: 1}
         }
}, {
    $project: {"_id": 0, "season": "$_id.season", "points": 1, "assists": 1, 
    "rebounds": 1, "blocks": 1, "steals": 1, "3 pointers": 1, "max 3s": 1,
    "games": 1}
}]).toArray());

db.season_totals.find({});

// teams total
db.createView("teams_games_regular", "games", [{
    $match: {"season_type": "Regular Season"}
}, {
    $group: {"_id": {"season": "$season_year", "team_id": "$team_id"}, "poeni": {$sum: "$pts"}, "faulovi": {$sum: "$pf"},
         "odigrane": {$sum: 1}}
}]);

db.createView("teams_wins_regular", "games", [{
    $match: {"season_type": "Regular Season", "wl": "W"}
}, {
    $group: {"_id": {"season": "$season_year", "team_id": "$team_id"}, "pobede": {$sum: 1}}
}]);

db.teams_games_regular.find({});
db.teams_wins_regular.find({});

db.teams_total.insertMany(
db.teams_games_regular.aggregate([{
    $lookup: {
      from: "teams_wins_regular",
      localField: "_id",
      foreignField: "_id",
      as: "wins"    
    }
}, {
    $unwind: "$wins"
}, {
    $project: {_id: 0, season: "$_id.season", team_id: "$_id.team_id", poeni: 1, faulovi: 1, odigrane: 1, pobede: "$wins.pobede"}
}, {
    $lookup: {
      from: "teams",
      localField: "team_id",
      foreignField: "team_id",
      as: "team_info"    
    }
}, {
    $unwind: "$team_info"
}, {$project: {_id: 1, poeni: 1, season: 1, team_id: 1, faulovi: 1, odigrane: 1,
         pobede: 1, naziv: "$team_info.abbreviation"}
}, {
    $unwind: "$naziv"
}]).toArray()
);

db.teams_total.find({});

//player_total_seasons
db.player_total_seasons.insertMany(db.players_game_stats.aggregate({
    $group: {_id: {player_id: "$player_id", player_name: "$player_name", 
    season: "$season_year", 
    season_type: "$season_type"}, 
    total_pts: {$sum: "$pts"}, minutes: {$sum: "$min"}, total_fouls: {$sum: "$pf"}, games_played: {$sum: 1}}
}).toArray());


//
db.createView("bet_spread_filtrirano", "betting_spread", [{
    $match: {"book_name": {$in: ["JustBet", "Heritage"]}}
}]);

db.createView("bet_totals_filtrirano", "betting_totals", [{
    $match: {"book_name": {$in: ["JustBet", "Heritage"]}},
    },
    {
        $project: {
            "book_id": 0,
            "team_id": 0,
            "a_team_id": 0
            }
    }
    ]);

db.bet_spread_filtrirano.find({});
db.bet_totals_filtrirano.find({});

db.games.aggregate([
   {
       $match: { is_home: "t", season_type: "Playoffs" }
   }]);


//games + spread 
db.games_spread.insertMany(db.games.aggregate([
   {
       $match: { is_home: "t", season_type: "Playoffs" }
   },
   {
    $lookup: {
      from: "bet_spread_filtrirano",
      localField: "game_id",
      foreignField: "game_id",
      as: "spread"    
        }
    }, {
        $project: {_id: 0, matchup: 1, date: 1, wl: 1, spread: 1, season_year: 1, game_id: 1}
    }
]).toArray());

db.bet.insertMany(db.games_spread.aggregate([
    {
    $lookup: {
      from: "bet_totals_filtrirano",
      localField: "game_id",
      foreignField: "game_id",
      as: "totals"    
      }
    },
    {
        $project: {_id: 0}
    }
]).toArray());

db.games_spread.find({});
db.bet.find({});