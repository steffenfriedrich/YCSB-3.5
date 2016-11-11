package com.yahoo.ycsb.db.onlineshop;

import com.mongodb.client.MongoCollection;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.workloads.onlineshop.Recommendation;
import org.bson.Document;
import java.util.*;


public class MongoDbOnlineShopClient2 extends MongoDbOnlineShopClient {

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
        .append("recommendations", new ArrayList<Document>());

      if (batchSize == 1) {
        collectionU.insertOne(toInsertUser);
      } else {
        bulkInsertB.add(toInsertUser);
        if (bulkInsertB.size() >= batchSize || bulkInsertA.size() >= batchSize) {
          collectionU.insertMany(bulkInsertB, INSERT_UNORDERED);
        }
      }


    } catch (Exception e) {
      System.err.println("Exception while trying bulk insert with "
        + bulkInsertB.size() + bulkInsertA.size());
      e.printStackTrace();
      return Status.ERROR;
    }

    return Status.OK;
  }

  @Override
  public Status insertRecommendation(int bookID, int userID, int stars, int likes, String text, Date createTime) {

    Document user = new Document("_id", userID);
    Document toInsertRecommendation = new Document("_id", bookID)
      .append("createTime", createTime)
      .append("stars", stars)
      .append("likes", likes)
      .append("text", text);
    database.getCollection("users").updateOne(user, new Document("$push", new Document("recommendations", toInsertRecommendation)));
    return Status.OK;
  }

  @Override
  public Recommendation getUsersRecommendations(int userID) {
    Document user = new Document("_id", userID);
    List<Document> userRecommendations = database.getCollection("users").find(user).projection(new Document("_id", 0).append("recommendations", 1)).into(new ArrayList<Document>());

    return new Recommendation(Status.OK.getName(), Status.OK.getDescription(),userRecommendations);
  }

  @Override
  public Recommendation getAllRecommendations(int bookID) {
    Document book = new Document("_id", bookID);
    List<Integer> users = (ArrayList<Integer>) database.getCollection("books").find(book).first().get("userCommented");
    List<Document> userRecommends = new ArrayList<>();

    if (users != null) {
      for (int user : users) {
        Document matchUser =      new Document("$match",  new Document("_id",user));
        Document unwind =         new Document("$unwind", "$recommendations");
        Document matchRecommend = new Document("$match",  new Document("recommendations._id", bookID));
        Document project =        new Document("$project",new Document("_id", 0).append("recommendations", 1));

        userRecommends.add(database.getCollection("users").aggregate(Arrays.asList(matchUser,unwind,matchRecommend,project)).first());
      }

      return new Recommendation(Status.OK.getName(), Status.OK.getDescription(), userRecommends);
    }
    return new Recommendation(Status.OK.getName(), Status.OK.getDescription(), new Document("Status", "no recommendations exists "));
  }

  @Override
  public Recommendation getLatestRecommendations(int bookID, int limit) {
    Document book = new Document("_id", bookID);
    Document projectLimit = new Document("_id", 0).append("userCommented", 1).append("userCommented", new Document("$slice", -limit));
    List<Integer> users = (ArrayList<Integer>) database.getCollection("books").find(book).projection(projectLimit).first().get("userCommented");
    List<Document> latestRecommends = new ArrayList<>();

    if (users != null) {
      for (int user : users) {
        Document matchUser =      new Document("$match",  new Document("_id",user));
        Document unwind =         new Document("$unwind", "$recommendations");
        Document matchRecommend = new Document("$match",  new Document("recommendations._id", bookID));
        Document project =        new Document("$project",new Document("_id", 0).append("recommendations", 1));

        latestRecommends.add(database.getCollection("users").aggregate(Arrays.asList(matchUser,unwind,matchRecommend,project)).first());
      }

      return new Recommendation(Status.OK.getName(), Status.OK.getDescription(), latestRecommends);
    }

    return new Recommendation(Status.OK.getName(), Status.OK.getDescription(), new Document("Status", "no recommendations exists "));
  }

  @Override
  public Status updateRecommendation(int bookID, int userID, int stars, String text) {
    Document user = new Document("_id", userID).append("recommendations._id", bookID);
    Document update = new Document("recommendations.0.stars", stars).append("recommendations.0.text", text);
    database.getCollection("users").updateOne(user, new Document("$set", update));

    return Status.OK;
  }

}
