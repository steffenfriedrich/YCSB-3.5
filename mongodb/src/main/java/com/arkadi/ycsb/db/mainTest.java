package com.arkadi.ycsb.db;

/**
 * Created by Arkad on 18.09.2016.
 */

import com.mongodb.*;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.workloads.onlineshop.Author;
import com.yahoo.ycsb.workloads.onlineshop.Book;
import com.yahoo.ycsb.workloads.onlineshop.Recommendation;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.*;


public class mainTest {


  private static MongoClient mongoClient;
  static MongoDatabase database;


  public static void main(String[] args) {


     mongoClient = new MongoClient( "localhost" , 27017 );
     database = mongoClient.getDatabase("bookStore");

    //MongoCollection<Document> collection = database.getCollection("authors");
    //getAuthorByID();
    //findAuthorByBookID();
    getLatestRecommendations();
  }


//TODO ist richtig
  public static Author getAuthorByID() {
    Document query = new Document("_id", 19);
    Document result = database.getCollection("authors").find(query).first();

    String fullName = result.getString("authorFullName");
    System.out.print(fullName);
    return new Author(Status.OK.getName(), Status.OK.getDescription(), result.getString("authorFullName"), result.getString("gender"), result.getString("resume"));
  }

//TODO falsh
  public static Author findAuthorByBookID() {
    Document query = new Document("_id", 42);
    Document projection = new Document("authors", 1).append("_id", 0);
    Document resultAuthor = database.getCollection("books").find(query).projection(projection).first();
    Document result = database.getCollection("authors").find(resultAuthor).first();

    String fullName = result.getString("authorFullName");
    System.out.print(fullName);

    return new Author(Status.OK.getName(), Status.OK.getDescription(), result.getString("authorFullName"), result.getString("gender"), result.getString("resume"));

  }

//TODO  falsch keine kommentrare drin keine stars
  public static Recommendation getLatestRecommendations() {
    Document query = new Document("_id", 42);
    Document sortKrit = new Document("recommendations._id", -1);
    FindIterable<Document> result = database.getCollection("recommendations").find(query).sort(sortKrit).limit(1);

    final ArrayList<Integer> stars = new ArrayList<>();
    result.forEach(new Block<Document>() {
      @Override
      public void apply(final Document document) {
        System.out.print(document);
        document.getInteger("stars");// teste ob es null hier  drin
      }
    });

    int evStars = 0;
    for (Integer i : stars) {
      evStars = evStars + i;
    }

    return new Recommendation(Status.OK.getName(), Status.OK.getDescription(), 42, evStars / 1);
  }


  public Recommendation getAllRecommendations(int bookID) {
    Document query = new Document("_id", bookID);
    Document recommDoc = database.getCollection("recommendations").find(query).first();

    return new Recommendation(Status.OK.getName(), Status.OK.getDescription(), (String) recommDoc.get("bookTitle"), (int) recommDoc.get("recommendCount"));

  }

  public Author getAuthorByID(int authorID) {
    Document query = new Document("_id", authorID);
    Document result = database.getCollection("authors").find(query).first();

    String fullName = result.getString("authorFullName");
    return new Author(Status.OK.getName(), Status.OK.getDescription(), result.getString("authorFullName"), result.getString("gender"), result.getString("resume"));
  }


  public Recommendation getUsersRecommendations(int userID) {
    Document query = new Document("_id", userID);
    Document booksID = database.getCollection("users").find(query).first();
    ArrayList<Integer> booksIDs = (ArrayList<Integer>) booksID.get("booksRecommended");
    final List<AggregateIterable<Document>> userRecommends = new LinkedList<>();

    // mindestens ein buch wurde kommentiert

    System.out.println("booksIDs = " + booksIDs.size());
    Recommendation rec = new Recommendation(Status.OK.getName(), Status.OK.getDescription(), booksIDs.get(0), 1);

    if (booksIDs.size() != 0) {
      for (int book : booksIDs) {
        Bson querryBook = new Document("$match", (book));
        Bson unwind = new Document("$unwind", "$recommendations");
        Bson querryRecommend = new Document("$match", new Document("recommendations._id", userID));
        Bson project = new Document("_id", 0).append("recommendations", 1);
        Bson[] array = {querryBook, unwind, querryRecommend, project};


        userRecommends.add(database.getCollection("recommendations").aggregate(new ArrayList<>(Arrays.asList(array))));
      }

      return rec;
    }
    return rec;
  }

  public Book findBooksByGenre(String genreList, int limit) {
    //String[] genres = genreList.split(","); not supported
    List<String> genres = Arrays.asList(genreList.split(","));
    Document query = new Document("genres", new Document("$all", genres));
    FindIterable<Document> result = database.getCollection("books").find(query).limit(limit);

    final HashSet books = new HashSet();
    result.forEach(new Block<Document>() {
      @Override
      public void apply(final Document document) {
        books.add(document.getString("title"));
      }
    });


    return new Book(Status.OK.getName(), Status.OK.getDescription(), books);
  }

  public Book findBookByName(String bookName) {
    Document result = database.getCollection("books").find(new Document("title", bookName)).first();

    String title = result.getString("title");
    return new Book(Status.OK.getName(), Status.OK.getDescription(), result.getString("title"), result.getString("introductionText"), result.getString("language"));
  }

  public Author findAuthorByBookID(int bookID) {
    Document query = new Document("_id", bookID);
    Document projection = new Document("authors", 1).append("_id", 0);
    Document resultAuthor = (Document) database.getCollection("books").find(query).projection(projection);
    Document result = database.getCollection("authors").find(resultAuthor).first();

    String fullName = result.getString("authorFullName");
    return new Author(Status.OK.getName(), Status.OK.getDescription(), result.getString("authorFullName"), result.getString("gender"), result.getString("resume"));


  }




}
