//neoptimizovano: 9.327
db.players_game_stats.aggregate([
    { $match: {season_type: "Regular Season"}},
    { $group: { _id: { player_id: "$player_id", player_name: "$player_name"},
         avg_points: { $avg: "$pts" } }},
    { $sort: {"avg_points": -1}},
    { $project : {"player_name": "$_id.player_name", _id: 0, avg_points: 1}}
]);

//optimizovano: 0.282
db.player_total_seasons.aggregate([{
    $match: {"_id.season_type" : "Regular Season"}
}, {
    $group: {"_id": {player_id: "$_id.player_id", player_name: "$_id.player_name"}, 
        total_points: {$sum: "$total_pts"}, total_games: {$sum: "$games_played"}}
}, {
    $project: {_id: 0, player_name: "$_id.player_name",
         "avg_points": {$divide: ["$total_points", "$total_games"]}}
}, {
    $sort: {"avg_points": -1}
}]);



