package com.arkadi.ycsb.db;


//public class onlineShopDBClientSpecializedQueries extends onlineShopDBClient {

/*
  *//**
   * db.colR.aggregate([{"$match":{"_id": recommendationBundleID }},
   * {"$unwind": "$recommendations"},
   * {"$match": {"recommendations.likes": {"$gt":recRating}}},
   * {"$project": {"_id":0,"recommendations":1}},
   * {"$sort": {"recommendations.likes":1}},
   * {"$limit": 20}
   * ])
   *//*
  public Status findRecommendationsByRating(int bookID, double recRating) {
    Bson queryDoc = new Document("$match", new Document("_id", bookID));
    Bson queryEmbedDoc = new Document("$match", new Document("recommendations.likes", new Document("$gt", recRating)));
    Bson unwind = new Document("$unwind", "$recommendations");
    Bson project = new Document("_id", 0).append("recommendations", 1)
      .append("$sort", new Document("recommendations.likes", 1))
      .append("$limit", 20);

    Bson[] array = {queryDoc, unwind, queryEmbedDoc, project};
    MongoCollection collection = database.getCollection("recommendations");
    collection.aggregate(new ArrayList<>(Arrays.asList(array)));

    return Status.OK;
  }

  *//**b
   * db.books.ensureIndex({introduction:"text"})
   * db.books.find({$text:{$search:searchText}})
   *//*
  public Status findBooksByTextMatch(String searchText) {
    database.getCollection("books").createIndex(new Document("introduction", "text"));
    database.getCollection("books").find(new Document("$text", new Document("$search", searchText)));

    return Status.OK;
  }

}



   //db.recommendations.aggregate([{"$match":{"_id": 16 }},{"$unwind": "$recommendations"},{"$match": {"recommendations._id": 393}},{"$project": {"_id":0,"recommendations":1}},{"$limit": 20}])

**/
//}