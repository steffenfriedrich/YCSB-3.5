package com.yahoo.ycsb.workloads.onlineshop;

import com.yahoo.ycsb.Status;
import java.util.ArrayList;
import org.bson.Document;

/**
 * Created by adaschkewitsch on 12.08.2016.
 */
public class Author extends Status {


  Document author;
  ArrayList<Document> authors;

  public Author(String name, String description, Document author) {
    super(name, description);
    this.author = author;
  }
  public Author(String name, String description, ArrayList<Document> authors) {
    super(name, description);
    this.authors = authors;
  }


  public String getFullName() {
    return author.getString("authorFullName");
  }

  public String getGender() {
    return author.getString("gender");
  }

  public String getResume() {
    return author.getString("resume");
  }

  public int getCount(){
    return authors.size();
  }

}
