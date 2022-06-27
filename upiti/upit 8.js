// neoptimizovano: 0.610
db.games.aggregate([{
    $group: {"_id": "$season", "prosek_poena": {$avg: "$pts"}}
}, {
    $sort: {"prosek_poena": -1}
}]);

// optimizovano: 0.03
db.season_totals.aggregate([{
    $project: {_id: 0, "points_per_game": {$divide: ["$points", "$games"]}, "season": 1}
}, {
    $sort: {"points_per_game": -1}
}]);


