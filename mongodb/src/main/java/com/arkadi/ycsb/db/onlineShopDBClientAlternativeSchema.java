package com.arkadi.ycsb.db;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.workloads.onlineshop.Recommendation;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.*;


public class onlineShopDBClientAlternativeSchema extends onlineShopDBClient {

  @Override
  public Status insertBook(int bookID, String bookTitle, ArrayList<String> genres, String introductionText, String language, HashMap<Integer, String> authors) {
    try {
      //create list of authors Document
      List<Document> array = new ArrayList<>();
      for (Map.Entry<Integer, String> entry : authors.entrySet()) { // author injection
        array.add(new Document("_id", entry.getKey()).append("authorFullName", entry.getValue()));
      }

      Document toInsertBook = new Document("_id", bookID)
        .append("title", bookTitle)
        .append("genres", genres)
        .append("language", language)
        .append("introductionText", introductionText)
        .append("authors", array)
        .append("userCommented", null);


      //Keine möglichkeit buch aus author zu löschen ohne das buch zu löschen und auch anders rum beides soll gleichzeitig passieren also keine extra methode

      //insert book
      database.getCollection("books").insertOne(toInsertBook);

      //insert in author book written by him
      Document update = new Document("$push", new Document("booksPublished", new Document("_id", bookID).append("title", bookTitle)));
      for (Map.Entry<Integer, String> entry : authors.entrySet()) {
        database.getCollection("authors").updateOne(new Document("_id", entry.getKey()), update);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }

    return Status.OK;
  }

  @Override
  public Status insertUser(int userID, String userName, Date birthDate) {


    try {
      MongoCollection<Document> collectionU = database.getCollection("users");

      Document toInsertUser = new Document("_id", userID)
        .append("userName", userName)
        .append("birthDate", birthDate)
        .append("recommendations", null);

      if (batchSize == 1) {
        collectionU.insertOne(toInsertUser);
      } else {
        BULKINSERT_A.add(toInsertUser);
        if (BULKINSERT_A.size() >= batchSize || BULKINSERT_B.size() >= batchSize) {
          collectionU.insertMany(BULKINSERT_A, INSERT_UNORDERED);
        }
      }


    } catch (Exception e) {
      System.err.println("Exception while trying bulk insert with "
        + BULKINSERT_A.size() + BULKINSERT_B.size());
      e.printStackTrace();
      return Status.ERROR;
    }

    return Status.OK;
  }

  @Override
  public Status insertRecommendation(int bookID, int userID, int stars, int likes, String text, Date createTime) {

    Document query = new Document("_id", userID);
    Document toInsertRecommendation = new Document("_id", bookID)
      .append("createTime", createTime)
      .append("stars", stars)
      .append("likes", likes)
      .append("text", text);
    database.getCollection("users").updateOne(query, new Document("$push", new Document("recommendations", toInsertRecommendation)));
    return Status.OK;
  }

  /**
   * db.users.find({_id:userID},{$projection:{_id:0,recommendations:1}})
   */
  @Override
  public Recommendation getUsersRecommendations(int userID) {
    Document query = new Document("_id", userID);
    FindIterable<Document> result = database.getCollection("users").find(query).projection(new Document("_id", 0).append("recommendations", 1));
    Recommendation rec = new Recommendation(Status.OK.getName(), Status.OK.getDescription(), userID);

    return rec;
  }

  @Override
  public Recommendation getAllRecommendations(int bookID) {
    Document query = new Document("_id", bookID);
    Document queryBook = database.getCollection("books").find(query).first();
    int[] userIDs = (int[]) queryBook.get("userCommented");
    final List<AggregateIterable<Document>> bookRecommends = new LinkedList<>();
    Recommendation rec = new Recommendation(Status.OK.getName(), Status.OK.getDescription(), userIDs[0], 1);
    if (userIDs.length != 0) {
      for (int user : userIDs) {
        // alle recommendations for book
        Bson queryUser = new Document("$match", user);
        Bson unwind = new Document("$unwind", "$recommendations");
        Bson queryRecommend = new Document("$match", new Document("recommendations._id", bookID));
        Bson project = new Document("_id", 0).append("recommendations", 1);
        Bson[] array = {queryUser, unwind, queryRecommend, project};

        bookRecommends.add(database.getCollection("recommendations").aggregate(new ArrayList<>(Arrays.asList(array))));
      }
      return rec;
    }
    return rec;
  }

  /**
   * db.recommendations.aggregate([{"$match":{"_id": userID }},{"$unwind": "$recommendations"},{"$match": {"recommendations._id": bookID}},{"$project": {"_id":0,"recommendations":1}},{"$limit": 20}])
   */
  @Override
  public Recommendation getLatestRecommendations(int bookID, int limit) {
    Document query = new Document("_id", bookID);
    Document queryBook = database.getCollection("books").find(query).first();
    int[] userIDs = (int[]) queryBook.get("userCommented");
    final List<AggregateIterable<Document>> bookRecommends = new LinkedList<>();
    Recommendation rec = new Recommendation(Status.OK.getName(), Status.OK.getDescription(), userIDs[0], 1);
    if (userIDs.length != 0) {
      for (int i = 0; i < limit; i++) {
        // anzahl gebrauchter  zu lätzt hinzugefügten einträge
        Bson queryUser = new Document("$match", userIDs[userIDs.length - limit - i]);
        Bson unwind = new Document("$unwind", "$recommendations");
        Bson queryRecommend = new Document("$match", new Document("recommendations._id", bookID));
        Bson project = new Document("_id", 0).append("recommendations", 1);
        Bson[] array = {queryUser, unwind, queryRecommend, project};

        bookRecommends.add(database.getCollection("recommendations").aggregate(new ArrayList<>(Arrays.asList(array))));
      }
      return rec;
    }
    return rec;
  }

  /**
   * db.recommendations.updateOne({_id: 19,"recommendations._id": 312},{$set: {"recommendations.0.stars": 1,"recommendations.0.text":"hallo"}})
   */
  @Override
  public Status updateRecommendation(int bookID, int userID, int stars, String text) {
    Document query = new Document("_id", userID).append("recommendations._id", bookID);
    Document update = new Document("recommendations.0.stars", stars).append("recommendations.0.text", text);
    database.getCollection("users").updateOne(query, (new Document("$set", update)));

    return Status.OK;
  }

}
