package com.arkadi.ycsb.db;

/**
 * Created by Arkad on 18.09.2016.
 */

import com.mongodb.*;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.workloads.onlineshop.Author;
import com.yahoo.ycsb.workloads.onlineshop.Book;
import com.yahoo.ycsb.workloads.onlineshop.Recommendation;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static java.util.Arrays.asList;


public class mainTest {


  private static MongoClient mongoClient;
  static MongoDatabase database;


  static String dbURL;
  static String dbName;
  static Connection database2;
  static String user;
  static String pass;


  public static void main(String[] args) {
    Date birth = randomDate();
    //init2();
      init();
    insertUser2(5,"arkadi",birth);
  }

  public static Status insertUser2(int userID, String userName, Date birthDate) {

    String insertSql = "INSERT INTO bookStore.Users (Id,Name,BirthDate) VALUES " + "(" + userID + ", '" + userName + "', " + birthDate + ");";

    try {
      //prepsInsertProduct = database.prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS);
      Statement prepsInsertProduct = database2.createStatement();
      prepsInsertProduct.executeUpdate(insertSql);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.OK;
  }

  public void init2() {
    mongoClient = new MongoClient("localhost", 27017);
    database = mongoClient.getDatabase("bookStore");

    //MongoCollection<Document> collection = database.getCollection("authors");
    //getAuthorByID();
//    findAuthorsByBookID();
//    findBooksByGenre("horror,drama", 5);
//    findBookByName("book00000000000000000005228908089637460490");
//    getAllRecommendations(9);
//    getLatestRecommendations(14, 1);
//    getUsersRecommendations(172);
  }

  public static void init() {

    String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    String databaseUserName = "Arkadi";
    String databasePassword = "Arkadi";
    String url2 = "jdbc:sqlserver://DESKTOP-7D3D4PO\\MSSQLSERVER_2014;databaseName=YCSB";


    //java.lang.ClassNotFoundException: com.microsoft.sqlserver.jdbc.SQLServerDriver
    //java.sql.SQLException: No suitable driver found for jdbc:sqlserver://localhost:1433;databaseName=YCSB;user=Arkadi;password=Arkadi
    //Java.lang.UnsupportedOperationException: Die Java-Laufzeitumgebung (Java Runtime Environment, JRE), Version 1.8, wird von diesem Treiber nicht unterstützt. Verwenden Sie die Klassenbibliothek 'sqljdbc4.jar', die Unterstützung für JDBC 4.0 bietet.

    try {
      Class.forName(driver);
      //Class.forName(driver).newInstance();
      database2 = DriverManager.getConnection(url2, databaseUserName, databasePassword);
      //database = DriverManager.getConnection(connec tionUrl);
      //database = DriverManager.getConnection("jdbc:sqlserver://localhost;integratedSecurity=true");
      //database = DriverManager.getConnection("jdbc:sqlserver://;servername=DESKTOP-7D3D4PO\\MSSQLSERVER_2014;integratedSecurity=true;");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }



  public static Author getAuthorByID() {
    Document query = new Document("_id", 19);
    Document author = database.getCollection("authors").find(query).first();

    return new Author(Status.OK.getName(), Status.OK.getDescription(), author);
  }

  public static Author findAuthorsByBookID() {
    Document query = new Document("_id", 42);
    ArrayList<Document> bookAuthors = (ArrayList<Document>) database.getCollection("books").find(query).first().get("authors");

    ArrayList<Document> authors = new ArrayList<>();
    for (Document author : bookAuthors) {
      authors.add(database.getCollection("authors").find(new Document("_id", author.getInteger("_id"))).first());
    }

    return new Author(Status.OK.getName(), Status.OK.getDescription(), authors);

  }

  public static Book findBooksByGenre(String genreList, int limit) {
    List<String> genres = asList(genreList.split(","));
    Document query = new Document("genres", new Document("$all", genres));
    List<Document> books = database.getCollection("books").find(query).limit(limit).into(new ArrayList<Document>());

    return new Book(Status.OK.getName(), Status.OK.getDescription(), books);
  }

  public static Book findBookByName(String bookName) {
    Document book = database.getCollection("books").find(new Document("title", bookName)).first();

    return new Book(Status.OK.getName(), Status.OK.getDescription(), book);
  }

  public static Recommendation getAllRecommendations(int bookID) {
    Document query = new Document("_id", bookID);
    List<Document> recommendations = (ArrayList<Document>) database.getCollection("recommendations").find(query).first().get("recommendations");

    return new Recommendation(Status.OK.getName(), Status.OK.getDescription(), recommendations);

  }

  public static Recommendation getLatestRecommendations(int bookID, int limit) {
    Document recommendationBundle = new Document("_id", bookID);
    Document project = new Document("_id", 0).append("recommendations", 1).append("recommendations", new Document("$slice", -limit));
    Document newestRecommendations = database.getCollection("recommendations").find(recommendationBundle).projection(project).first();

    return new Recommendation(Status.OK.getName(), Status.OK.getDescription(), (ArrayList<Document>) newestRecommendations.get("recommendations"));
  }

   // > db.recommendations.aggregate([{"$match":{"_id": 16 }},{"$unwind": "$recommendations"},{"$match": {"recommendations._id": 172}},{"$project": {"_id":0,"recommendations":1}}]).pretty()
  public static Recommendation getUsersRecommendations(int userID) {
    Document user = new Document("_id", userID);
    List<Integer> books = (ArrayList<Integer>) database.getCollection("users").find(user).first().get("bookRecommended");
    List<Document> userRecommends = new ArrayList<>();

    if (books != null) {
      for (int book : books) {
        Document matchBook =      new Document("$match",  new Document("_id",book));
        Document unwind =         new Document("$unwind", "$recommendations");
        Document matchRecommend = new Document("$match",  new Document("recommendations._id", userID));
        Document project =        new Document("$project",new Document("_id", 0).append("recommendations", 1));

        userRecommends.add(database.getCollection("recommendations").aggregate(Arrays.asList(matchBook,unwind,matchRecommend,project)).first());
      }

      return new Recommendation(Status.OK.getName(), Status.OK.getDescription(), userRecommends);
    }
    return new Recommendation(Status.OK.getName(), Status.OK.getDescription(), new Document("Status", "no recommendations exists "));
  }


  public Status getUsersRecommendations2(int userID) {
    Bson match = new Document("$match", new Document("_id", userID));
    Bson match2 = new Document("$match", new Document("recommendations.likes", new Document("$gt", userID)));
    Bson unwind = new Document("$unwind", "$recommendations");
    Bson project = new Document("_id", 0).append("recommendations", 1)
      .append("$sort", new Document("recommendations.likes", 1))
      .append("$limit", 20);

    Bson[] array = {match, unwind, match2, project};
    MongoCollection collection = database.getCollection("recommendations");
    collection.aggregate(new ArrayList<>(Arrays.asList(array)));

    return Status.OK;
  }



  public static Date randomDate() {
    GregorianCalendar gc = new GregorianCalendar();

    int year = randBetween(1900, 2010);

    gc.set(Calendar.YEAR, year);

    int dayOfYear = randBetween(1, gc.getActualMaximum(Calendar.DAY_OF_YEAR));

    gc.set(Calendar.DAY_OF_YEAR, dayOfYear);

    String dateInString = gc.get(Calendar.DAY_OF_MONTH) + "/" + (gc.get(Calendar.MONTH) + 1) + "/" + gc.get(Calendar.YEAR);
    SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
    SimpleDateFormat formatter2 = new SimpleDateFormat("dd/M/yyyy");
    Date date = null;

    try {
      date = formatter.parse(dateInString);
    } catch (ParseException e) {
      try {
        date = formatter2.parse(dateInString);
      } catch (ParseException e1) {
        e1.printStackTrace();
      }
    }
    return date;
  }
  public static int randBetween(int start, int end) {

    return start + (int) Math.round(Math.random() * (end - start));
  }

}
