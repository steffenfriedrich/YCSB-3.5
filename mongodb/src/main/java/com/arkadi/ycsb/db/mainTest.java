package com.arkadi.ycsb.db;

/**
 * Created by Arkad on 18.09.2016.
 */

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.workloads.onlineshop.Author;
import org.bson.Document;

import java.util.List;
import java.util.Set;



public class mainTest {


  private static MongoClient mongoClient;
  static MongoDatabase database;


  public static void main(String[] args) {


     mongoClient = new MongoClient( "localhost" , 27017 );
     database = mongoClient.getDatabase("bookStore");

    MongoCollection<Document> collection = database.getCollection("authors");
    getAuthorByID();

  }



  public static Author getAuthorByID() {
    Document query = new Document("_id", 19);
    Document result = database.getCollection("authors").find(query).first();

    String fullName = result.getString("authorFullName");
    System.out.print(fullName);
    return new Author(Status.OK.getName(), Status.OK.getDescription(), result.getString("authorFullName"), result.getString("gender"), result.getString("resume"));
  }


}
