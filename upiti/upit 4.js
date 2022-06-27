//neoptimizovano, vreme: 11.742s
db.createView("teams_games_regular_grouped", "games", [{
    $match: {"season_type": "Regular Season"}
}, {
    $group: {"_id": "$team_id", "odigrane": {$sum: 1}}
}]);

db.createView("teams_wins_regular_grouped", "games", [{
    $match: {"season_type": "Regular Season", "wl": "W"}
}, {
    $group: {"_id": "$team_id", "pobede": {$sum: 1}}
}]);

db.teams_games_regular_grouped.aggregate([{
    $lookup: {
      from: "teams_wins_regular_grouped",
      localField: "_id",
      foreignField: "_id",
      as: "wins"    
    }
}, {
    $project: {_id: 1, odigrane: 1, pobede: "$wins.pobede"}
}, {
    $unwind: "$pobede"
}, {
    $lookup: {
      from: "teams",
      localField: "_id",
      foreignField: "team_id",
      as: "team_info"    
    }
}, {
    $project: {odigrane: 1, pobede: 1, name: "$team_info.abbreviation"}
}, {
    $unwind: "$name"
}, {
    $sort: {pobede: -1}
}]);

//optimizovano: 0.008s
db.teams_total.aggregate([{
    $group: {"_id": {team_id: "$team_id", name: "$naziv"}, odigrane: {$sum: "$odigrane"}, pobede: {$sum: "$pobede"}}
}, {
    $project: {_id: 0, id_tima: "$_id.team_id", naziv: "$_id.name", odigrane: 1, pobede: 1}  
}, {
    $sort: {pobede: -1}
}]);
