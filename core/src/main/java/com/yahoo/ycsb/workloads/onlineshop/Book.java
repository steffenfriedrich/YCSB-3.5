package com.yahoo.ycsb.workloads.onlineshop;

import com.yahoo.ycsb.Status;

import java.util.HashSet;


public class Book extends Status {

  private String title;
  private HashSet bookTitles;
  private String introText;
  private String language;

  public Book(String name, String description,String title,String introText,String language ) {
    super(name, description);
    this.title=title;
    this.introText=introText;
    this.language=language;
  }

  public Book(String name, String description, HashSet bookTitles ) {
    super(name, description);
    this.bookTitles=bookTitles;
  }

  public String getTitle() {
    return title;
  }

  public String getIntroText() {
    return introText;
  }

  public String getLanguage() {
    return language;
  }
}
