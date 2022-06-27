//neoptimizovano: 4:47:684
db.games.aggregate([{
    $match: { is_home: "t", season_type: "Playoffs", "wl": "L" }
}, {
    $lookup: {
      from: "bet_spread_filtrirano",
      localField: "game_id",
      foreignField: "game_id",
      as: "spread"    
        }
}, {
    $unwind: "$spread"
}, {
   $match: {"spread.book_name": "Heritage"}
}, {
    $project: {matchup: 1, spread: "$spread.spread1", season: 1}
}, {
    $sort: {"spread": -1}
}, {
    $limit: 5
}]);

//optimizovano: 0.014s
db.bet.aggregate([
    { $unwind: "$spread" },
{
    $match: {wl: "L", "spread.book_name": "Heritage"}
}, {
    $project: {matchup: 1, spread: "$spread.spread1", season: 1}
}, {
    $sort: {"spread": -1}
}, {
    $limit: 5
}]);
