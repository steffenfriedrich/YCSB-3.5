package com.yahoo.ycsb.workloads.onlineshop;

import com.yahoo.ycsb.Status;
import org.bson.Document;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;


public class Recommendation extends Status {


  List<Document> recommendations;
  Document recommendation;

  public Recommendation(String name, String description, Document recommendation) {
    super(name, description);
    this.recommendation = recommendation;
  }

  public Recommendation(String name, String description, List<Document> recommendations) {
    super(name, description);
    this.recommendations = recommendations;
  }


  public String getBookTitle() {
    return recommendation.getString("bookTitle");
  }

  public int getUserID() {
    return recommendation.getInteger("_id");
  }

  public int getStars() {
    return recommendation.getInteger("stars");
  }

  public int getCount() {
    return recommendations.size();
  }

  public int evgRating() {
    int rating;
    int stars = 0;
    if (recommendations != null) {
      for (Document recommend : recommendations) {
        if (recommend.getInteger("stars") != null) {
          stars = stars + recommend.getInteger("stars");
        }
      }
      rating = stars / recommendations.size();
    } else rating = 0;


    return rating;
  }
}
