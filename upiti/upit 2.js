//neoptimizovano: 7.181s
db.games.aggregate([
    { $match : {wl: "W", is_home: "t", matchup: "LAL @ BOS", season_type: "Playoffs"} },
    {
        $lookup :
            {
                from: "betting_spread",
                localField: "game_id",
                foreignField: "game_id",
                as: "bet"
            }
    },
    { $unwind: "$bet"},
     { $project: {
                        "bet.book_name": 1,
                        "bet.spread1": 1,
                        "bet.spread2": 1,
                        "greska": { $gt:["$bet.spread2","$bet.spread1"] }
                }},
    { $match: {"bet.book_name":"JustBet", greska: true}},
    { $group: {"_id": null, "ukupno_gresaka": {$sum: 1}} },
    { $project: { "ukupno_gresaka": 1, "_id": 0 }}
     ], {allowDiskUse: true});

//optimizovano: 0.006s
db.bet.aggregate([
    { $unwind: "$spread"  },
    { $match: {wl: "W",  matchup: "LAL @ BOS", "spread.book_name": "JustBet"} },
    { $project: {
                        "spread.book_name": 1,
                        "spread.spread1": 1,
                        "spread.spread2": 1,
                        "greska": { $gt:["$spread.spread2","$spread.spread1"] }
                }},
    { $match: {greska: true}},
    { $group: {"_id": null, "ukupno_gresaka": {$sum: 1}} },
    { $project: { "ukupno_gresaka": 1, "_id": 0 }}
]);