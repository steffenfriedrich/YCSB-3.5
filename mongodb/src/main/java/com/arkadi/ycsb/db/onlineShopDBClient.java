package com.arkadi.ycsb.db;

import com.mongodb.*;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertManyOptions;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.db.OptionsSupport;
import com.yahoo.ycsb.workloads.onlineshop.Author;
import com.yahoo.ycsb.workloads.onlineshop.Book;
import com.yahoo.ycsb.workloads.onlineshop.Recommendation;
import com.yahoo.ycsb.workloads.onlineshop.onlineShopDB;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;


public class onlineShopDBClient extends onlineShopDB {


  static final Integer INCLUDE = 1;
  static final InsertManyOptions INSERT_UNORDERED = new InsertManyOptions().ordered(false);
  static final AtomicInteger INIT_COUNT = new AtomicInteger(0);
  static String databaseName;
  static MongoDatabase database;
  static MongoClient mongoClient;
  static ReadPreference readPreference;
  static WriteConcern writeConcern;
  static int batchSize;
  static boolean useUpsert;
  List<Document> BULKINSERT_B = new ArrayList<>();
  List<Document> BULKINSERT_A = new ArrayList<>();



  public void init() throws DBException {

    mongoClient = new MongoClient( "localhost" , 27017 );
    database = mongoClient.getDatabase("bookStore");
  }
  /*{
    INIT_COUNT.incrementAndGet();
    synchronized (INCLUDE) {
      if (mongoClient != null) {
        return;
      }

      Properties props = getProperties();

      // Set insert batchsize, default 1 - to be YCSB-original equivalent
      batchSize = Integer.parseInt(props.getProperty("batchsize", "1"));

      // Set is inserts are done as upserts. Defaults to false.
      useUpsert = Boolean.parseBoolean(
        props.getProperty("mongodb.upsert", "false"));

      // Just use the standard connection format URL
      // http://docs.mongodb.org/manual/reference/connection-string/
      // to configure the client.
      String url = props.getProperty("mongodb.url", null);
      boolean defaultedUrl = false;
      if (url == null) {
        defaultedUrl = true;
        url = "mongodb://localhost:27017/ycsb?w=1";
      }

      url = OptionsSupport.updateUrl(url, props);

      if (!url.startsWith("mongodb://")) {
        System.err.println("ERROR: Invalid URL: '" + url
          + "'. Must be of the form "
          + "'mongodb://<host1>:<port1>,<host2>:<port2>/database?options'. "
          + "http://docs.mongodb.org/manual/reference/connection-string/");
        System.exit(1);
      }

      try {
        MongoClientURI uri = new MongoClientURI(url);

        String uriDb = uri.getDatabase();
        if (!defaultedUrl && (uriDb != null) && !uriDb.isEmpty()
          && !"admin".equals(uriDb)) {
          databaseName = uriDb;
        } else {
          // If no database is specified in URI, use "ycsb"
          databaseName = "ycsb";

        }

        readPreference = uri.getOptions().getReadPreference();
        writeConcern = uri.getOptions().getWriteConcern();

        mongoClient = new MongoClient(uri);
        database = mongoClient.getDatabase(databaseName)
            .withReadPreference(readPreference)
            .withWriteConcern(writeConcern);

        System.out.println("mongo client connection created with " + url);
      } catch (Exception e1) {
        System.err
          .println("Could not initialize MongoDB connection pool for Loader: "
            + e1.toString());
        e1.printStackTrace();
        return;
      }
    }
  }*/


 /*----------------------------------------------insert operations----------------------------------------------------*/

  @Override
  public Status insertUser(int userID, String userName, Date birthDate) {


    try {
      MongoCollection<Document> collectionU = database.getCollection("users");

      Document toInsertUser = new Document("_id", userID)
        .append("userName", userName)
        .append("birthDate", birthDate);

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



  /**
   * db.author.insertOne(toInsertAuthor)
   */
  @Override
  public Status insertAuthor(int authorID, String authorFullName, String gender, Date birthDate, String resume) {

    try {
      //MongoCollection<Document> collectionA = database.getCollection("authors");

      Document toInsertAuthor = new Document("_id", authorID)
        .append("authorFullName", authorFullName)
        .append("gender", gender)
        .append("birthDate", birthDate)
        .append("resume", resume);


      if (batchSize == 1) {
        database.getCollection("authors").insertOne(toInsertAuthor);
      } else {
        BULKINSERT_A.add(toInsertAuthor);
        if (BULKINSERT_A.size() >= batchSize || BULKINSERT_B.size() >= batchSize) {
          database.getCollection("authors").insertMany(BULKINSERT_A, INSERT_UNORDERED);
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

  /**
   * db.book.insertOne(toInsertBook)
   */
  @Override
  public Status insertBook(int bookID, String bookTitle, ArrayList<String> genres, String introductionText, String language, HashMap<Integer, String> authors) {
    try {
      List<Document> array = new ArrayList<>();
      for (Map.Entry<Integer, String> entry : authors.entrySet()) { // author injection
        array.add(new Document("_id", entry.getKey()).append("authorFullName", entry.getValue()));
      }

      Document toInsertBook = new Document("_id", bookID)
        .append("title", bookTitle)
        .append("genres", genres)
        .append("language", language)
        .append("introductionText", introductionText)
        .append("authors", array);


      //Keine möglichkeit buch aus author zu löschen ohne das buch zu löschen und auch anders rum beides soll gleichzeitig passieren also keine extra methode

      //System.out.println(toInsertBook);
      database.getCollection("books").insertOne(toInsertBook);
      insertRecommendationBundle(bookID, bookTitle);
      Document update = new Document("$push", new Document("booksPublished", new Document("_id", bookID).append("title", bookTitle)));
      for (Map.Entry<Integer, String> entry : authors.entrySet()) {
        database.getCollection("authors").updateOne(new Document("_id", entry.getKey()), update);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }

    return Status.OK;
  }

//TODO  new getter setter methods for new design

  //wird nur vom client benutzt und kann nicht von ausen angewand werden

  /**
   * db.recommendations.insertOne(toInsertRecSlot)
   */
  private Status insertRecommendationBundle(int bookID, String bookTitle) {

    try {
      MongoCollection<Document> collectionRecommendation = database.getCollection("recommendations");

      Document toInsertRecommendBundle = new Document("_id", bookID)
        .append("recommendCount", 0)
        .append("ratingAverage", 0)
        .append("bookTitle", bookTitle);

      collectionRecommendation.insertOne(toInsertRecommendBundle);
    } catch (Exception e) {
      e.printStackTrace();
    }

    return Status.OK;
  }

  /**
   * db.recommendations.updateOne({_id: recommendationBundleID},{$push:{recommendations:{values}})
   */
  @Override
  public Status insertRecommendation(int bookID, int userID, int stars, int likes, String text, Date createTime) {

    Document query = new Document("_id", bookID);
    Document query2 = new Document("_id", userID);
    Document toInsertRecommendation = new Document("_id", userID)
      .append("createTime", createTime)
      .append("stars", stars)
      .append("likes", likes)
      .append("text", text);
    database.getCollection("recommendations").updateOne(query, new Document("$push", new Document("recommendations", toInsertRecommendation)));
    database.getCollection("users").updateOne(query2, new Document("$push", new Document("bookRecommended", bookID)));
    return Status.OK;
  }


/**
 db.authors.updateOne({_id: authorID},{$push:{bookPublished:{_id: bookID,bookName:bookName}}})

 public Status insertBookReferenceInAuthor(int authorID, int bookID, String bookName) {
 Document query = new Document("_id", authorID);
 Document update = new Document("$push", new Document("bookPublished", new Document("_id", bookID).append("bookName", bookName)));
 UpdateResult result = database.getCollection("authors").updateOne(query, update);

 return Status.OK;
 }
 */

  /*----------------------------------------------get operations -----------------------------------------------------*/

  /**
   * db.recommendations.find({"_id":recommendationBundleID}).sort({"recommendations._id",-1}).limit(amount)
   * find a special amount of  latest recommendations recommendations and calculate the average user rating
   */
  @Override
  public Recommendation getLatestRecommendations(int bookID, int limit) {
    Document query = new Document("_id", bookID);
    Document sortKrit = new Document("recommendations._id", -1);
    FindIterable<Document> result = database.getCollection("recommendations").find(query).sort(sortKrit).limit(limit);

    final ArrayList<Integer> stars = new ArrayList<>();
    result.forEach(new Block<Document>() {
      @Override
      public void apply(final Document document) {
        stars.add(document.getInteger("stars"));
      }
    });

    int evStars = 0;
    for (Integer i : stars) {
      evStars = evStars + i;
    }

    return new Recommendation(Status.OK.getName(), Status.OK.getDescription(), bookID, evStars / limit);
  }

  /**
   * db.recommendations.find({_id: recommendationBundleID}).first()
   */
  @Override
  public Recommendation getAllRecommendations(int bookID) {
    Document query = new Document("_id", bookID);
    Document recommDoc = database.getCollection("recommendations").find(query).first();

    return new Recommendation(Status.OK.getName(), Status.OK.getDescription(), (String) recommDoc.get("bookTitle"), (int) recommDoc.get("recommendCount"));

  }

  /**
   * db.authors.find({_id:authorID}).first()
   */
  @Override
  public Author getAuthorByID(int authorID) {
    Document query = new Document("_id", authorID);
    Document result = database.getCollection("authors").find(query).first();

    String fullName = result.getString("authorFullName");
    return new Author(Status.OK.getName(), Status.OK.getDescription(), result.getString("authorFullName"), result.getString("gender"), result.getString("resume"));
  }

  /**
   * db.books.find({genres:{ $all: [genreList[0],genreList[n]]}}).limit(max)
   */
  @Override
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


  /**
   * db.recommendations.aggregate([{"$match":{"_id": bookID }},{"$unwind": "$recommendations"},{"$match": {"recommendations._id": userID,{"$project": {"_id":0,"recommendations":1}},{"$limit": 20}])
   */
  @Override
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

  /*----------------------------------------------find operations ----------------------------------------------------*/

  /**
   * db.books.find({"name": bName}.first()
   */
  @Override
  public Book findBookByName(String bookName) {
    Document result = database.getCollection("books").find(new Document("title", bookName)).first();

    String title = result.getString("title");
    return new Book(Status.OK.getName(), Status.OK.getDescription(), result.getString("title"), result.getString("introductionText"), result.getString("language"));
  }

  /**
   * Resultauthor  = db.books.find({_id:bookID}{author:1}
   * db.authors.find(Resultauthor)
   */
  @Override
  public Author findAuthorByBookID(int bookID) {
    Document query = new Document("_id", bookID);
    Document projection = new Document("authors", 1).append("_id", 0);
    Document resultAuthor = (Document) database.getCollection("books").find(query).projection(projection);
    Document result = database.getCollection("authors").find(resultAuthor).first();

    String fullName = result.getString("authorFullName");
    return new Author(Status.OK.getName(), Status.OK.getDescription(), result.getString("authorFullName"), result.getString("gender"), result.getString("resume"));


  }
/*----------------------------------------------update operations --------------------------------------------------*/

  /**
   * db.books.updateOne({_id: bookID},{$set:bookValues})
   */
  @Override
  public Status updateBook(int bookID, String title, String language, String introduction) {
    Document query = new Document("_id", bookID);
    Document update = new Document("_id", bookID)
      .append("title", title)
      .append("language", language)
      .append("introduction", introduction);

    database.getCollection("books").updateOne(query, new Document("$set", update));


    return Status.OK;
  }

  /**
   * db.recommendations.updateOne({_id: 19,"recommendations._id": 312},{$set: {"recommendations.0.stars": 1,"recommendations.0.text":"hallo"}})
   */
  @Override
  public Status updateRecommendation(int bookID, int userID, int stars, String text) {
    Document query = new Document("_id", bookID).append("recommendations._id", userID);
    Document update = new Document("recommendations.0.stars", stars).append("recommendations.0.text", text);
    database.getCollection("recommendations").updateOne(query, (new Document("$set", update)));
    //database.getCollection("recommendations").updateOne(query, (new Document("$set", new Document("recommendations.text", text))));

    return Status.OK;
  }

  @Override
  public Status updateAuthor(int authorID, String authorName, String gender, Date birthDate, String resume) {
    Document query = new Document("_id", authorID).append("authorFullName", authorName);
    Document update = new Document("gender", gender).append("birthDate", birthDate).append("resume", resume);
    database.getCollection("authors").updateOne(query, new Document("$set", update));

    return Status.OK;
  }



  /*----------------------------------------------delete Operations ---------------------------------------------------*/


  /**
   * db.books.findOne({_id: _bookID},{author:1})
   * db.colA.updateOne({_id: _authorID,},{$pull:{bookPublished:{_id: bookID}})
   */
  public Status deleteBookReferenceFromAuthor(int bookID) {
    Document queryBook = new Document("_id", bookID);
    Document projection = new Document("authors", 1);
    Document foundAuthor = database.getCollection("books").find(queryBook).projection(projection).first();
    Document pullBook = new Document("$pull", new Document("booksPublished", queryBook));
    database.getCollection("authors").updateOne(foundAuthor, pullBook);

    return Status.OK;

  }


  /**
   * db.books.deleteOne({_id: bookID}}
   */
  @Override
  public Status deleteBook(int bookID) {
    Document queryBook = new Document("_id", bookID);
    database.getCollection("books").deleteOne(queryBook);
    deleteBookReferenceFromAuthorList(bookID);
    return Status.OK;
  }

  /**
   * db.authors.updateOne({_id: _authorID,},{$pull:{bookPublished:{_id: bookID}})
   */

  private Status deleteBookReferenceFromAuthorList(int bookID) {
    Document query = new Document("_id", bookID);
    Document project = new Document("authors._id", 1).append("_id", 0);
    Document authors = database.getCollection("books").find(query).projection(project).first();

    Document update = new Document("$pull", new Document("booksPublished", new Document("_id", bookID)));
    database.getCollection("authors").updateMany(query, update);

    return Status.OK;
  }

  /**
   * db.recommendations.deleteOne({_id: bookID})
   */
  @Override
  public Status deleteAllRecommendationsBelongToBook(int bookID) {
    Document query = new Document("_id", bookID);
    database.getCollection("recommendations").deleteOne(query);

    return Status.OK;
  }

  /**
   * db.authors.deleteOne({_id: authorID})
   */
  @Override
  public Status deleteAuthor(int authorID) {
    Document query = new Document("_id", authorID);
    database.getCollection("authors").deleteOne(query);

    return Status.OK;
  }


  /*----------------------------------------------Deprecated operations-----------------------------------------------*/


  @Override
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    return null;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return null;
  }

  @Override
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    return null;
  }

  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    return null;
  }

  @Override
  public Status delete(String table, String key) {
    return null;
  }
}

