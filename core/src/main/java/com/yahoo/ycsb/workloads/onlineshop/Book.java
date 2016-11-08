package com.yahoo.ycsb.workloads.onlineshop;

import com.yahoo.ycsb.Status;
import org.bson.Document;

import java.util.List;


public class Book extends Status {


  private List<Document> books;
  private Document book;

  public Book(String name, String description, List<Document> books) {
    super(name, description);
    this.books = books;
  }

  public Book(String name, String description, Document books) {
    super(name, description);
    this.book = book;
  }

  public Document getBook() {

    return book;
  }

  public List<Document> getBooks() {

    return books;
  }

  public String getTitle() {
    return book.getString("title");
  }

  public String getIntroText() {
    return book.getString("introductionText");
  }

  public String getLanguage() {
    return book.getString("language");
  }

  public int getBooksCount() {
    return books.size();
  }


}
