package com.yahoo.ycsb.workloads.onlineshop;

import com.yahoo.ycsb.Status;

/**
 * Created by adaschkewitsch on 12.08.2016.
 */
public class Author extends Status {

  private String fullName;
  private String gender;
  private String resume;

  public Author(String name, String description,String fullName,String gender, String resume) {
    super(name, description);
    this.fullName=fullName;
    this.gender=gender;
    this.resume=resume;
  }

  public String getFullName() {
    return fullName;
  }

  public String getGender() {
    return gender;
  }

  public String getResume() {
    return resume;
  }
}
