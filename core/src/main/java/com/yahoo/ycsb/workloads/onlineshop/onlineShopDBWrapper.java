/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.workloads.onlineshop;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.measurements.Measurements;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;

import java.util.*;

/**
 * Wrapper around a "real" DB that measures latencies and counts return codes.
 * Also reports latency separately between OK and failed operations.
 */
public class OnlineShopDBWrapper extends OnlineShopDB {
  private static final String REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY = "reportlatencyforeacherror";
  private static final String REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY_DEFAULT = "false";
  private static final String LATENCY_TRACKED_ERRORS_PROPERTY = "latencytrackederrors";
  private final OnlineShopDB _db;
  private final Measurements _measurements;


  private final Tracer _tracer;

  private final String SCOPE_STRING_INSERT_BOOK;
  private final String SCOPE_STRING_INSERT_AUTHOR;
  private final String SCOPE_STRING_INSERT_RECOMMENDATION;
  private final String SCOPE_STRING_INSERT_USER;
  private final String SCOPE_STRING_GET_LATEST_RECOMMENDATION;
  private final String SCOPE_STRING_GET_ALL_RECOMMENDATION;
  private final String SCOPE_STRING_GET_USERS_RECOMMENDATION;
  private final String SCOPE_STRING_GET_AUTHOR_BY_ID;
  private final String SCOPE_STRING_GET_BOOKS_BY_GENRE;
  private final String SCOPE_STRING_FIND_BOOKS_NAME;
  private final String SCOPE_STRING_FIND_AUTHOR_BY_BOOK;
  private final String SCOPE_STRING_UPDATE_AUTHOR;
  private final String SCOPE_STRING_UPDATE_BOOK;
  private final String SCOPE_STRING_UPDATE_RECOMMENDATION;
  private final String SCOPE_STRING_DELETE_BOOK;
  private final String SCOPE_STRING_DELETE_ALL_RECOMMENDATIONS_BELONG_TO_BOOK;
  private final String SCOPE_STRING_DELETE_AUTHOR;
  private final String SCOPE_STRING_INIT;

  private boolean reportLatencyForEachError = false;
  private HashSet<String> latencyTrackedErrors = new HashSet<>();

  public OnlineShopDBWrapper(final OnlineShopDB db, final Tracer tracer) {
    _db = db;
    _measurements = Measurements.getMeasurements();
    _tracer = tracer;
    final String simple = db.getClass().getSimpleName();
    SCOPE_STRING_INIT = simple + "#init";
    SCOPE_STRING_INSERT_BOOK = simple + "#insertBook";
    SCOPE_STRING_INSERT_AUTHOR = simple + "#inserAuthor";
    SCOPE_STRING_INSERT_RECOMMENDATION = simple + "#insertRecommendation";
    SCOPE_STRING_INSERT_USER = simple + "#insertUser";
    SCOPE_STRING_GET_LATEST_RECOMMENDATION = simple + "#getLastRecommendation";
    SCOPE_STRING_GET_ALL_RECOMMENDATION = simple + "#getAllRecommendations";
    SCOPE_STRING_GET_USERS_RECOMMENDATION = simple + "#getUsersRecommendations";
    SCOPE_STRING_GET_AUTHOR_BY_ID = simple + "#getAuthorByID";
    SCOPE_STRING_GET_BOOKS_BY_GENRE = simple + "#findBooksByGenre";
    SCOPE_STRING_FIND_BOOKS_NAME = simple + "#findBooksName";
    SCOPE_STRING_FIND_AUTHOR_BY_BOOK = simple + "#findAuthorByBook";
    SCOPE_STRING_UPDATE_AUTHOR = simple + "#updateAuthor";
    SCOPE_STRING_UPDATE_BOOK = simple + "#updateBook";
    SCOPE_STRING_UPDATE_RECOMMENDATION = simple + "#updateRecommendation";
    SCOPE_STRING_DELETE_BOOK = simple + "#deleteBook";
    SCOPE_STRING_DELETE_ALL_RECOMMENDATIONS_BELONG_TO_BOOK = simple + "#deleteAllRecommendationsBelongToBook";
    SCOPE_STRING_DELETE_AUTHOR = simple + "#deleteAuthor";

  }

  /**
   * Get the set of properties for this DB.
   */
  public Properties getProperties() {
    return _db.getProperties();
  }

  /**
   * Set the properties for this DB.
   */
  public void setProperties(Properties p) {
    _db.setProperties(p);
  }

  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  public void init() throws DBException {
    try (final TraceScope span = _tracer.newScope(SCOPE_STRING_INIT)) {
      _db.init();
      this.reportLatencyForEachError = Boolean.parseBoolean(getProperties().getProperty(REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY,
        REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY_DEFAULT));

      if (!reportLatencyForEachError) {
        String latencyTrackedErrors = getProperties().getProperty(LATENCY_TRACKED_ERRORS_PROPERTY, null);
        if (latencyTrackedErrors != null) {
          this.latencyTrackedErrors = new HashSet<>(Arrays.asList(latencyTrackedErrors.split(",")));
        }
      }
      System.err.println("OnlineShopDBWrapper: report latency for each error is "
        + this.reportLatencyForEachError
        + " and specific error codes to track"
        + " for latency are: "
        + this.latencyTrackedErrors.toString());
    }
  }


  private void measure(String op, Status result, long intendedStartTimeNanos, long startTimeNanos, long endTimeNanos) {
    String measurementName = op;
    if (!result.getName().equals(Status.OK.getName())) {
      if (this.reportLatencyForEachError ||
        this.latencyTrackedErrors.contains(result.getName())) {
        measurementName = op + "-" + result.getName();
      } else {
        measurementName = op + "-FAILED";
      }
    }
    _measurements.measure(measurementName,
      (int) ((endTimeNanos - startTimeNanos) / 1000));
    _measurements.measureIntended(measurementName,
      (int) ((endTimeNanos - intendedStartTimeNanos) / 1000));
  }


  @Override
  public Status insertUser(int userID, String userName, Date birthDate) {
    {
      try (final TraceScope span = _tracer.newScope(SCOPE_STRING_INSERT_USER)) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        Status res = _db.insertUser(userID, userName, birthDate);
        long en = System.nanoTime();
        measure("InsertUser", res, ist, st, en);
        measure("Insert", res, ist, st, en);
        measure("All", res, ist, st, en);
        _measurements.reportStatus("InsertUser", res);
        _measurements.reportStatus("Insert", res);
        _measurements.reportStatus("All", res);
        return res;
      }
    }
  }

  @Override
  public Status insertAuthor(int authorID, String authorFullName, String gender, Date birthDate, String resume) {
    {
      try (final TraceScope span = _tracer.newScope(SCOPE_STRING_INSERT_AUTHOR)) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        Status res = _db.insertAuthor(authorID, authorFullName, gender, birthDate, resume);
        long en = System.nanoTime();
        measure("InsertAuthor", res, ist, st, en);
        measure("Insert", res, ist, st, en);
        measure("All", res, ist, st, en);
        _measurements.reportStatus("InsertAuthor", res);
        _measurements.reportStatus("Insert", res);
        _measurements.reportStatus("All", res);
        return res;
      }
    }
  }

  @Override
  public Status insertBook(int bookID, String bookTitle, ArrayList<String> genres, String introductionText, String language, HashMap<Integer,String> authors) {
    {
      try (final TraceScope span = _tracer.newScope(SCOPE_STRING_INSERT_BOOK)) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        Status res = _db.insertBook(bookID, bookTitle, genres, introductionText, language, authors);
        long en = System.nanoTime();
        measure("InsertBook", res, ist, st, en);
        measure("Insert", res, ist, st, en);
        measure("All", res, ist, st, en);
        _measurements.reportStatus("InsertBook", res);
        _measurements.reportStatus("Insert", res);
        _measurements.reportStatus("All", res);
        return res;
      }
    }
  }


  @Override
  public Status insertRecommendation(int bookID, int userID, int stars, int likes, String text, Date createTime) {
    {
      try (final TraceScope span = _tracer.newScope(SCOPE_STRING_INSERT_RECOMMENDATION)) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        Status res = _db.insertRecommendation(bookID, userID, stars, likes, text, createTime);
        long en = System.nanoTime();
        measure("InsertRecommendations", res, ist, st, en);
        measure("Insert", res, ist, st, en);
        measure("All", res, ist, st, en);
        _measurements.reportStatus("InsertRecommendations", res);
        _measurements.reportStatus("Insert", res);
        _measurements.reportStatus("All", res);
        return res;
      }
    }
  }

  @Override
  public Recommendation getLatestRecommendations(int bookID, int limit) {
    {
      try (final TraceScope span = _tracer.newScope(SCOPE_STRING_GET_LATEST_RECOMMENDATION)) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        Recommendation res = _db.getLatestRecommendations(bookID, limit);
        long en = System.nanoTime();
        measure("GetLatestRecommendations", res, ist, st, en);
        measure("Read", res, ist, st, en);
        measure("All", res, ist, st, en);
        _measurements.reportStatus("GetLatestRecommendations", res);
        _measurements.reportStatus("Read", res);
        _measurements.reportStatus("All", res);
        return res;
      }
    }
  }

  @Override
  public Recommendation getAllRecommendations(int bookID) {
    {
      try (final TraceScope span = _tracer.newScope(SCOPE_STRING_GET_ALL_RECOMMENDATION)) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        Recommendation res = _db.getAllRecommendations(bookID);
        long en = System.nanoTime();
        measure("GetAllRecommendations", res, ist, st, en);
        measure("Read", res, ist, st, en);
        measure("All", res, ist, st, en);
        _measurements.reportStatus("GetAllRecommendations", res);
        _measurements.reportStatus("Read", res);
        _measurements.reportStatus("All", res);
        return res;
      }
    }
  }

  @Override
  public Recommendation getUsersRecommendations(int userID) {
    {
      try (final TraceScope span = _tracer.newScope(SCOPE_STRING_GET_USERS_RECOMMENDATION)) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        Recommendation res = _db.getUsersRecommendations(userID);
        long en = System.nanoTime();
        measure("GetUsersRecommendations", res, ist, st, en);
        measure("Read", res, ist, st, en);
        measure("All", res, ist, st, en);
        _measurements.reportStatus("GetUsersRecommendations", res);
        _measurements.reportStatus("Read", res);
        _measurements.reportStatus("All", res);
        return res;
      }
    }
  }

  @Override
  public Author getAuthorByID(int authorID) {
    {
      try (final TraceScope span = _tracer.newScope(SCOPE_STRING_GET_AUTHOR_BY_ID)) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        Author res = _db.getAuthorByID(authorID);
        long en = System.nanoTime();
        measure("GetAuthorByID", res, ist, st, en);
        measure("Read", res, ist, st, en);
        measure("All", res, ist, st, en);
        _measurements.reportStatus("GetAuthorByID", res);
        _measurements.reportStatus("Read", res);
        _measurements.reportStatus("All", res);
        return res;
      }
    }
  }

  @Override
  public Book findBooksByGenre(String genreList, int limit) {
    {
      try (final TraceScope span = _tracer.newScope(SCOPE_STRING_GET_BOOKS_BY_GENRE)) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        Book res = _db.findBooksByGenre(genreList, limit);
        long en = System.nanoTime();
        measure("FindBooksByGenre", res, ist, st, en);
        measure("Read", res, ist, st, en);
        measure("All", res, ist, st, en);
        _measurements.reportStatus("FindBooksByGenre", res);
        _measurements.reportStatus("Read", res);
        _measurements.reportStatus("All", res);
        return res;
      }
    }
  }

  @Override
  public Book findBookByName(String bookName) {
    {
      try (final TraceScope span = _tracer.newScope(SCOPE_STRING_FIND_BOOKS_NAME)) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        Book res = _db.findBookByName(bookName);
        long en = System.nanoTime();
        measure("FindBooksName", res, ist, st, en);
        measure("Read", res, ist, st, en);
        measure("All", res, ist, st, en);
        _measurements.reportStatus("FindBooksName", res);
        _measurements.reportStatus("Read", res);
        _measurements.reportStatus("All", res);
        return res;
      }
    }
  }

  @Override
  public Author findAuthorsByBookID(int bookID) {
    {
      try (final TraceScope span = _tracer.newScope(SCOPE_STRING_FIND_AUTHOR_BY_BOOK)) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        Author res = _db.findAuthorsByBookID(bookID);
        long en = System.nanoTime();
        measure("FindAuthorByBookID", res, ist, st, en);
        measure("Read", res, ist, st, en);
        measure("All", res, ist, st, en);
        _measurements.reportStatus("FindAuthorByBookID", res);
        _measurements.reportStatus("Read", res);
        _measurements.reportStatus("All", res);
        return res;
      }
    }
  }

  @Override
  public Status updateAuthor(int authorID, String authorName, String gender, Date birthDate, String resume) {
    {
      try (final TraceScope span = _tracer.newScope(SCOPE_STRING_UPDATE_AUTHOR)) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        Status res = _db.updateAuthor(authorID, authorName, gender, birthDate, resume);
        long en = System.nanoTime();
        measure("UpdateAuthor", res, ist, st, en);
        measure("Update", res, ist, st, en);
        measure("All", res, ist, st, en);
        _measurements.reportStatus("UpdateAuthor", res);
        _measurements.reportStatus("Update", res);
        _measurements.reportStatus("All", res);
        return res;
      }
    }
  }

  @Override
  public Status updateBook(int bookID, String title, String language, String introduction) {
    {
      try (final TraceScope span = _tracer.newScope(SCOPE_STRING_UPDATE_BOOK)) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        Status res = _db.updateBook(bookID, title, language, introduction);
        long en = System.nanoTime();
        measure("UpdateBook", res, ist, st, en);
        measure("Update", res, ist, st, en);
        measure("All", res, ist, st, en);
        _measurements.reportStatus("UpdateBook", res);
        _measurements.reportStatus("Update", res);
        _measurements.reportStatus("All", res);
        return res;
      }
    }
  }

  @Override
  public Status updateRecommendation(int bookID, int userID, int stars, String text) {
    {
      try (final TraceScope span = _tracer.newScope(SCOPE_STRING_UPDATE_RECOMMENDATION)) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        Status res = _db.updateRecommendation(bookID, userID, stars, text);
        long en = System.nanoTime();
        measure("UpdateRecommendation", res, ist, st, en);
        measure("Update", res, ist, st, en);
        measure("All", res, ist, st, en);
        _measurements.reportStatus("UpdateRecommendation", res);
        _measurements.reportStatus("Update", res);
        _measurements.reportStatus("All", res);
        return res;
      }
    }
  }

  @Override
  public Status deleteBook(int bookID) {
    {
      try (final TraceScope span = _tracer.newScope(SCOPE_STRING_DELETE_BOOK)) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        Status res = _db.deleteBook(bookID);
        long en = System.nanoTime();
        measure("DeleteBook", res, ist, st, en);
        measure("Delete", res, ist, st, en);
        measure("All", res, ist, st, en);
        _measurements.reportStatus("DeleteBook", res);
        _measurements.reportStatus("Delete", res);
        _measurements.reportStatus("All", res);
        return res;
      }
    }
  }

  @Override
  public Status deleteAllRecommendationsBelongToBook(int bookID) {
    {
      try (final TraceScope span = _tracer.newScope(SCOPE_STRING_DELETE_ALL_RECOMMENDATIONS_BELONG_TO_BOOK)) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        Status res = _db.deleteAllRecommendationsBelongToBook(bookID);
        long en = System.nanoTime();
        measure("DeleteAllRecommendationsBelongToBook", res, ist, st, en);
        measure("Delete", res, ist, st, en);
        measure("All", res, ist, st, en);
        _measurements.reportStatus("DeleteAllRecommendationsBelongToBook", res);
        _measurements.reportStatus("Delete", res);
        _measurements.reportStatus("All", res);
        return res;
      }
    }
  }

  @Override
  public Status deleteAuthor(int authorID) {
    {
      try (final TraceScope span = _tracer.newScope(SCOPE_STRING_DELETE_AUTHOR)) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        Status res = _db.deleteAuthor(authorID);
        long en = System.nanoTime();
        measure("DeleteAuthor", res, ist, st, en);
        measure("Delete", res, ist, st, en);
        measure("All", res, ist, st, en);
        _measurements.reportStatus("DeleteAuthor", res);
        _measurements.reportStatus("Delete", res);
        _measurements.reportStatus("All", res);
        return res;
      }
    }
  }

  // TODO deleteUser(int userID)

   /**
   * Deprecated methods
   */
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
