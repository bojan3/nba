//neoptimizovano: 10.59s
db.games.aggregate([
   {
       $match: { is_home: "t", season_type: "Playoffs" }
   },
   {
    $lookup: {
      from: "bet_totals_filtrirano",
      localField: "game_id",
      foreignField: "game_id",
      as: "totals"    
       }
}, {
    $unwind: "$totals"
}, {
    $match: {"totals.price1": {$lt: 0}}
}, {
    $group: {_id: "$totals.book_name", avg: {$avg: "$totals.price1"}}
}]);

//optimizovano: 0.040s
db.bet.aggregate([{
    $unwind: "$totals"
}, {
    $match: {"totals.price1": {$lt: 0}}
}, {
    $group: {_id: "$totals.book_name", avg: {$avg: "$totals.price1"}}
}]);