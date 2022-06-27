//optimizovano: 0.008
db.player_total_seasons.createIndex({"_id.season": 1});

db.player_total_seasons.aggregate([{
    $match: { "_id.season" : 2010, "_id.season_type" : "Regular Season"}
}, {
    $sort: {minutes: -1}
}, {
    $project: {total_pts: 0, total_fouls: 0}
}]);

//neoptimizovano: 4.531s
db.players_game_stats.aggregate([ 
    {
        $match: { season_year : 2010, season_type : "Regular Season"}
    },
    {
        $group: {"_id": {player_id: "$player_id", player_name: "$player_name"},
             broj_utakmica:{ $sum: 1 } ,
              "vreme_odigrano": {$sum: "$min"}}
    },
    {
        $sort: {"vreme_odigrano": -1}
    }
    ]);


