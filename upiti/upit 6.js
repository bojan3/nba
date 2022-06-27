//neoptimizovano: 0.632
db.games.aggregate([{
    $match: {"season_year": {$gte: 1980}}
}, {
    $group: {"_id": "$season_year", "trojke": {$max: "$fg3m"}}
}, {
    $project: {"_id": 1, "trojke": 1}
}, {
    $sort: {"trojke": -1}
}]);

//optimizovano: 0.005
db.season_totals.aggregate([{
    $match: {"season": {$gte: 1980}}
}, {
    $project: {_id: 0, "season": 1, "max 3s": 1}
}, {
    $sort: {"max 3s": -1}
}]);

