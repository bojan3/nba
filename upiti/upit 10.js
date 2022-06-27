//neoptimizovano: 2.384s
db.players_game_stats.aggregate([{
    $match: {"season_type": "Playoffs", "season_year": {$gt: 2010}}
}, {
    $group: {"_id": {"player_id": "$player_id", "player": "$player_name"},
         "plejof_utakmice": {$sum: 1}}
}, {
    $sort: {"plejof_utakmice": -1}
}]);

//optimizovano: 0.073
db.player_total_seasons.aggregate([{
    $match: {"_id.season_type": "Playoffs", "_id.season": {$gt: 2010}}
}, {
    $group: {_id: {player_id: "$_id.player_id", player_name: "$_id.player_name"}, 
    plejof_utakmice: {$sum: "$games_played"}}
}, {
    $sort: {plejof_utakmice: -1}
}]);


