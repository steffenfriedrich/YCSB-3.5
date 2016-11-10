package com.yahoo.ycsb.db.onlineshop;

import java.sql.*;
import java.util.*;
import java.util.Date;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.workloads.onlineshop.Author;
import com.yahoo.ycsb.workloads.onlineshop.Book;
import com.yahoo.ycsb.workloads.onlineshop.Recommendation;
import com.yahoo.ycsb.workloads.onlineshop.OnlineShopDB;
import org.bson.Document;

public class JdbcOnlineShopDBClient extends OnlineShopDB {
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
      insertSql = database.prepareStatement("SET IDENTITY_INSERT bookStore.Users ON;INSERT INTO bookStore.Users(Id,Name,BirthDate) VALUES(?,?,?)");
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
      insertSql = database.prepareStatement("SET IDENTITY_INSERT bookStore.Authors ON;INSERT INTO bookStore.Authors(Id,Name,BirthDate,Gender,[Resume]) VALUES(?,?,?,?,?) ");
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

    String authorsList = "";
    for (Map.Entry<Integer, String> entry : authors.entrySet()) { // author injection
      authorsList = authorsList + entry.getValue() + " ";
    }
    String genreList = "";
    for (String gen : genres) {
      genreList = genreList + gen + " ";
    }

    PreparedStatement insertSql = null;
    try {
      insertSql = database.prepareStatement("SET IDENTITY_INSERT bookStore.Books ON;INSERT INTO bookStore.Books([Id],[Title],[Language],[Resume],[Genres],[Authors]) VALUES (?,?,?,?,?,?)");
      insertSql.setInt(1, bookID);
      insertSql.setString(2, bookTitle);
      insertSql.setString(3, language);
      insertSql.setString(4, introductionText);
      insertSql.setString(5, genreList);
      insertSql.setString(6, authorsList);
      insertSql.executeUpdate();

    } catch (SQLException e) {
      e.printStackTrace();
    }

    return Status.OK;
  }

  @Override
  public Status insertRecommendation(int bookID, int userID, int stars, int likes, String text, Date createTime) {


    PreparedStatement insertSql = null;
    try {
      insertSql = database.prepareStatement("SET IDENTITY_INSERT bookStore.Authors ON;INSERT INTO bookStore.Recommendations ([UserId],[BookId],[CreateDate],[Text],[Stars],[Likes]) VALUES(?,?,?,?,?,?)");
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
      selectSql = database.prepareStatement("SELECT TOP( ? ) * FROM bookStore.Books WHERE CONTAINS([Genres],?)");
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
    ArrayList<Document> ListAuthors = new ArrayList<>();
    ResultSet rs = null;
    PreparedStatement selectSql = null;
    try {
      selectSql = database.prepareStatement("SELECT * FROM bookStore.Books WHERE [Id]=?");
      selectSql.setInt(1, bookID);
      rs = selectSql.executeQuery();
      String[] authors = rs.getString("Authors").split(" ");

      for (String aut : authors) {

        selectSql = database.prepareStatement("SELECT * FROM bookStore.Authors WHERE [Id]=?");
        selectSql.setInt(1, Integer.parseInt(aut));
        rs = selectSql.executeQuery();

        ListAuthors.add(new Document("id", rs.getString("Id")).
          append("Name", rs.getString("[Name[")).
          append("BirthDate", rs.getString("BirthDate")).
          append("Gender", rs.getString("Gender")).
          append("Resume", rs.getString("[Resume]")));
      }
      return new Author(Status.OK.getName(), Status.OK.getDescription(), ListAuthors);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

//TODO
  @Override
  public Status updateBook(int bookID, String title, String language, String introduction) {
    String update = "UPDATE bookStore.Books SET [Title] ='" + title + "',[Language]='" + language + "',[Resume]='" + introduction + "' WHERE Id=" + bookID;

    try {
      Statement stmt = database.createStatement();
      Boolean brs = stmt.execute(update);
      return new Book(Status.OK.getName(), Status.OK.getDescription(), new Document("SQLresultSet", brs));
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }


  @Override
  public Status updateRecommendation(int bookID, int userID, int stars, String text) {

    String update = "UPDATE bookStore.Recommendations SET [Stars] =" + stars + ",[Text]='" + text + "' WHERE BookId=" + bookID + "AND UserId";

    try {
      Statement stmt = database.createStatement();
      Boolean rs = stmt.execute(update);
      return new Book(Status.OK.getName(), Status.OK.getDescription(), new Document("SQLresultSet", rs));
    } catch (Exception e) {
      e.printStackTrace();
    }

    return null;

  }


  @Override
  public Status updateAuthor(int authorID, String authorName, String gender, Date birthDate, String resume) {
    String update = "UPDATE bookStore.Authors SET [Name] ='" + authorName + "',Gender='" + gender + "',[Resume]='" + resume + "',BirthDate =" + birthDate + " WHERE Id=" + authorID;

    try {
      Statement stmt = database.createStatement();
      Boolean rs = stmt.execute(update);
      return new Book(Status.OK.getName(), Status.OK.getDescription(), new Document("SQLresultBool", rs));
    } catch (Exception e) {
      e.printStackTrace();
    }

    return null;
  }

  @Override
  public Status deleteBook(int bookID) {
    String delete = "DELETE FROM bookStore.Books WHERE Id=" + bookID;

    try {
      Statement stmt = database.createStatement();
      Boolean rs = stmt.execute(delete);
      return new Book(Status.OK.getName(), Status.OK.getDescription(), new Document("SQLresultBool", rs));
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public Status deleteAllRecommendationsBelongToBook(int bookID) {
    String delete = "DELETE FROM bookStore.Recommendations WHERE Id=" + bookID;

    try {
      Statement stmt = database.createStatement();
      Boolean rs = stmt.execute(delete);
      return new Book(Status.OK.getName(), Status.OK.getDescription(), new Document("SQLresultBool", rs));
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public Status deleteAuthor(int authorID) {
    String delete = "DELETE FROM bookStore.Authors WHERE Id=" + authorID;

    try {
      Statement stmt = database.createStatement();
      Boolean rs = stmt.execute(delete);
      return new Book(Status.OK.getName(), Status.OK.getDescription(), new Document("SQLresultBool", rs));
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;

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

