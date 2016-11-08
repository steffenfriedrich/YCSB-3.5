package com.arkadi.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.workloads.onlineshop.Author;
import com.yahoo.ycsb.workloads.onlineshop.Book;
import com.yahoo.ycsb.workloads.onlineshop.Recommendation;
import com.yahoo.ycsb.workloads.onlineshop.onlineShopDB;
import org.bson.Document;

import java.sql.*;
import java.util.*;
import java.util.Date;

public class onlineShopDBClientSQL2 extends onlineShopDB {
  static String serverName;
  static String dbName;
  static Connection database;
  static String user;
  static String pass;
  static String url;
  static String driver;

  public void init() {

    Properties props = getProperties();
    serverName = props.getProperty("serverName");
    dbName = props.getProperty("dbName");
    user = props.getProperty("user");
    pass = props.getProperty("pass");
    url = "jdbc:sqlserver://" + serverName + ";databaseName=" + dbName;
    driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

/*
    String serverName = "DESKTOP-7D3D4PO\\MSSQLSERVER_2014";
    String dbName = "YCSB";
    String databaseUserName = "Arkadi";
    String databasePassword = "Arkadi";
    String url2 = "jdbc:sqlserver://DESKTOP-7D3D4PO\\MSSQLSERVER_2014;databaseName=YCSB";
*/

    try {
      Class.forName(driver);
      database = DriverManager.getConnection(url, user, pass);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }


  @Override
  public Status insertUser(int userID, String userName, Date birthDate) {

    PreparedStatement insertSql = null;
    try {
      insertSql = database.prepareStatement("SET IDENTITY_INSERT bookStore.Users ON;" +
        "INSERT INTO bookStore.Users(Id,Name,BirthDate) VALUES(?,?,?)" +
        "SET IDENTITY_INSERT bookStore.Users OFF");
      insertSql.setInt(1, userID);
      insertSql.setString(2, userName);
      insertSql.setDate(3, new java.sql.Date(birthDate.getTime()));
      insertSql.executeUpdate();

    } catch (SQLException e) {
      e.printStackTrace();
    }

    return Status.OK;
  }

  @Override
  public Status insertAuthor(int authorID, String authorFullName, String gender, Date birthDate, String resume) {

    PreparedStatement insertSql = null;
    try {
      insertSql = database.prepareStatement("SET IDENTITY_INSERT bookStore.Authors ON;" +
        "INSERT INTO bookStore.Authors(Id,Name,BirthDate,Gender,[Resume]) VALUES(?,?,?,?,?)" +
        "SET IDENTITY_INSERT bookStore.Authors OFF");
      insertSql.setInt(1, authorID);
      insertSql.setString(2, authorFullName);
      insertSql.setDate(3, new java.sql.Date(birthDate.getTime()));
      insertSql.setString(4, gender);
      insertSql.setString(5, resume);
      insertSql.executeUpdate();

    } catch (SQLException e) {
      e.printStackTrace();
    }

    return Status.OK;

  }

  @Override
  public Status insertBook(int bookID, String bookTitle, ArrayList<String> genres, String introductionText, String language, HashMap<Integer, String> authors) {
    PreparedStatement insertSql = null;

    try {

      //inserting book
      insertSql = database.prepareStatement("SET IDENTITY_INSERT bookStore.Books ON;" +
        "INSERT INTO bookStore.Books([Id],[Title],[Language],[Resume]) VALUES (?,?,?,?);" +
        "SET IDENTITY_INSERT bookStore.Books OFF");
      insertSql.setInt(1, bookID);
      insertSql.setString(2, bookTitle);
      insertSql.setString(3, language);
      insertSql.setString(4, introductionText);
      insertSql.executeUpdate();

      //inserting genres relationship
      for (String gen : genres) {
        insertSql = database.prepareStatement("INSERT INTO bookStore.Genres VALUES (?,?);");
        insertSql.setString(1, gen);
        insertSql.setInt(2, bookID);
        insertSql.addBatch();
      }
      insertSql.executeBatch();

      //inserting authors relationship
      for (Map.Entry<Integer, String> aut : authors.entrySet()) {
        insertSql = database.prepareStatement("INSERT INTO bookStore.MapAuthorsBooks VALUES (?,?);");
        insertSql.setInt(1, aut.getKey());
        insertSql.setInt(2, bookID);
        insertSql.addBatch();
      }
      insertSql.executeBatch();

    } catch (SQLException e) {
      e.toString();
    }

    return Status.OK;
  }

  @Override
  public Status insertRecommendation(int bookID, int userID, int stars, int likes, String text, Date createTime) {


    PreparedStatement insertSql = null;
    try {
      insertSql = database.prepareStatement("INSERT INTO bookStore.Recommendations ([UserId],[BookId],[CreateDate],[Text],[Stars],[Likes]) VALUES(?,?,?,?,?,?)");
      insertSql.setInt(1, userID);
      insertSql.setInt(2, bookID);
      insertSql.setDate(3, new java.sql.Date(createTime.getTime()));
      insertSql.setString(4, text);
      insertSql.setInt(5, stars);
      insertSql.setInt(6, likes);
      insertSql.executeUpdate();

    } catch (SQLException e) {

    }

    return Status.OK;
  }

  @Override
  public Recommendation getLatestRecommendations(int bookID, int limit) {

    ResultSet rs = null;
    PreparedStatement selectSql = null;
    try {
      selectSql = database.prepareStatement("SELECT TOP( ? ) * FROM bookStore.Recommendations WHERE BookId= ? ORDER BY bookStore.Recommendations.InsertOrder DESC");
      selectSql.setInt(1, limit);
      selectSql.setInt(2, bookID);
      rs = selectSql.executeQuery();

    } catch (SQLException e) {
      e.printStackTrace();
    }

    return new Recommendation(Status.OK.getName(), Status.OK.getDescription(), new Document("SQLresultSet", rs));
  }

  @Override
  public Recommendation getAllRecommendations(int bookID) {


    ResultSet rs = null;
    PreparedStatement selectSql = null;
    try {
      selectSql = database.prepareStatement("SELECT * FROM bookStore.Recommendations WHERE BookId=? ORDER BY bookStore.Recommendations.InsertOrder ASC");
      selectSql.setInt(1, bookID);
      rs = selectSql.executeQuery();

    } catch (SQLException e) {
      e.printStackTrace();
    }

    return new Recommendation(Status.OK.getName(), Status.OK.getDescription(), new Document("SQLresultSet", rs));
  }

  @Override
  public Recommendation getUsersRecommendations(int userID) {

    ResultSet rs = null;
    PreparedStatement selectSql = null;
    try {
      selectSql = database.prepareStatement("SELECT * FROM bookStore.Recommendations WHERE UserID=? ORDER BY bookStore.Recommendations.InsertOrder ASC");
      selectSql.setInt(1, userID);
      rs = selectSql.executeQuery();

    } catch (SQLException e) {
      e.printStackTrace();
    }

    return new Recommendation(Status.OK.getName(), Status.OK.getDescription(), new Document("SQLresultSet", rs));
  }

  @Override
  public Author getAuthorByID(int authorID) {

    ResultSet rs = null;
    PreparedStatement selectSql = null;
    try {
      selectSql = database.prepareStatement("SELECT * FROM bookStore.Authors WHERE Id=?");
      selectSql.setInt(1, authorID);
      rs = selectSql.executeQuery();

    } catch (SQLException e) {
      e.printStackTrace();
    }

    return new Author(Status.OK.getName(), Status.OK.getDescription(), new Document("SQLresultSet", rs));
  }

  @Override
  public Book findBooksByGenre(String genre, int limit) {

    ResultSet rs = null;
    PreparedStatement selectSql = null;
    try {
      selectSql = database.prepareStatement("SELECT TOP(?) B.* FROM bookStore.Genres as G join bookStore.Books as B on G.BookID=B.Id and G.Name =?");
      selectSql.setInt(1, limit);
      selectSql.setString(2, genre);
      rs = selectSql.executeQuery();

    } catch (SQLException e) {
      e.printStackTrace();
    }

    return new Book(Status.OK.getName(), Status.OK.getDescription(), new Document("SQLresultSet", rs));
  }

  @Override
  public Book findBookByName(String bookName) {

    ResultSet rs = null;
    PreparedStatement selectSql = null;
    try {
      selectSql = database.prepareStatement("SELECT * FROM bookStore.Books WHERE [Title]=?");
      selectSql.setString(1, bookName);
      rs = selectSql.executeQuery();

    } catch (SQLException e) {
      e.printStackTrace();
    }

    return new Book(Status.OK.getName(), Status.OK.getDescription(), new Document("SQLresultSet", rs));
  }

  @Override
  public Author findAuthorsByBookID(int bookID) {
    ResultSet rs = null;
    PreparedStatement selectSql = null;
    try {
      selectSql = database.prepareStatement("SELECT A.* FROM bookStore.Authors A join (select M.AuthorID FROM bookStore.MapAuthorsBooks WHERE M.BookID= ?) as author ON author.AuthorID=A.Id");
      selectSql.setInt(1, bookID);
      rs = selectSql.executeQuery();

      return new Author(Status.OK.getName(), Status.OK.getDescription(), new Document("authors", rs));
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return null;
  }


  @Override
  public Status updateBook(int bookID, String title, String language, String introduction) {

    PreparedStatement updateSql = null;
    try {
      updateSql = database.prepareStatement("UPDATE bookStore.Books SET [Title] =?,[Language]=?,[Resume]=? WHERE Id=?");
      updateSql.setString(1, title);
      updateSql.setString(2, language);
      updateSql.setString(3, introduction);
      updateSql.setInt(4, bookID);
      updateSql.executeUpdate();

    } catch (SQLException e) {
      e.printStackTrace();
    }

    return Status.OK;
  }


  @Override
  public Status updateRecommendation(int bookID, int userID, int stars, String text) {

    PreparedStatement updateSql = null;
    try {
      updateSql = database.prepareStatement("UPDATE bookStore.Recommendations SET [Stars] =?,[Text]=? WHERE [BookId]=? AND [UserId]=?");
      updateSql.setInt(1, stars);
      updateSql.setString(2, text);
      updateSql.setInt(3, bookID);
      updateSql.setInt(4, userID);
      updateSql.executeUpdate();

    } catch (SQLException e) {
      e.printStackTrace();
    }

    return Status.OK;
  }


  @Override
  public Status updateAuthor(int authorID, String authorName, String gender, Date birthDate, String resume) {

    PreparedStatement updateSql = null;
    try {
      updateSql = database.prepareStatement("UPDATE bookStore.Authors SET [Name] =?,[Gender]=?,[Resume]=?,[BirthDate] =? WHERE [Id]=?");
      updateSql.setString(1, authorName);
      updateSql.setString(2, gender);
      updateSql.setString(3, resume);
      updateSql.setDate(4, new java.sql.Date(birthDate.getTime()));
      updateSql.setInt(5, authorID);
      updateSql.executeUpdate();

    } catch (SQLException e) {
      e.printStackTrace();
    }

    return Status.OK;
  }

  //TODO
  @Override
  public Status deleteBook(int bookID) {
    PreparedStatement deleteSql = null;
    try {
      deleteSql = database.prepareStatement("DELETE FROM bookStore.Books WHERE Id=?");
      deleteSql.setInt(1, bookID);
      deleteSql.executeUpdate();

    } catch (SQLException e) {
      e.printStackTrace();
    }

    return Status.OK;
  }

  @Override
  public Status deleteAllRecommendationsBelongToBook(int bookID) {
    PreparedStatement deleteSql = null;
    try {
      deleteSql = database.prepareStatement("DELETE FROM bookStore.Recommendations WHERE BookId=?");
      deleteSql.setInt(1, bookID);
      deleteSql.executeUpdate();

    } catch (SQLException e) {
      e.printStackTrace();
    }

    return Status.OK;
  }

  @Override
  public Status deleteAuthor(int authorID) {
    PreparedStatement deleteSql = null;
    try {
      deleteSql = database.prepareStatement("DELETE FROM bookStore.Authors WHERE Id=?");
      deleteSql.setInt(1, authorID);
      deleteSql.executeUpdate();

    } catch (SQLException e) {
      e.printStackTrace();
    }

    return Status.OK;
  }


  //-------------------------------------------deprecated--------------------------------------------------------//
  @Override
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    return null;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<
    String> fields, Vector<HashMap<String, ByteIterator>> result) {
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