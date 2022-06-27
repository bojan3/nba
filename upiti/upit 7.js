//neoptimizovano: 11.581
db.games.aggregate([{
    $match: {"season_type": "Regular Season", "season_year": {$gte: 2000, $lte: 2015}}
}, {
    $lookup: {
       from: "teams",
       localField: "team_id",
       foreignField: "team_id",
       as: "team_info"
     }
}, {
    $match: {"team_info.abbreviation": "DEN"}
}, {
    $group: {"_id": "$team_info.abbreviation", "prosek_faulova": {$avg: "$pf"}}
}, {
    $unwind: "$_id"
}, {
    $project: {_id: 0, tim: "$_id", prosek_faulova: 1}
}]);

//optimizovano: 0.007
db.teams_total.aggregate([{
    $match: {naziv: "DEN", "season": {$gte: 2000, $lte: 2015}}
}, {
    $group: {_id: {tim_id: "$team_id", naziv: "$naziv"}, suma_faulova: {$sum: "$faulovi"},
         suma_odigranih: {$sum: "$odigrane"}}
}, {
    $project: {_id: 0, naziv: "$_id.naziv",
         "prosek_faulova": {$divide: ["$suma_faulova", "$suma_odigranih"]}}
}]);