package com.shrinivas.documentindexer.document;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import com.shrinivas.documentindexer.pojo.DbStatistic;

@Document(collection = "index")
public class Index {

	@Id
	private String word;

	private DbStatistic dbStatistic;

	public Index() {
		// TODO Auto-generated constructor stub
	}

	public Index(String word, DbStatistic dbStatistic) {
		super();
		this.word = word;
		this.dbStatistic = dbStatistic;
	}

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public DbStatistic getDbStatistic() {
		return dbStatistic;
	}

	public void setDbStatistic(DbStatistic dbStatistic) {
		this.dbStatistic = dbStatistic;
	}

	@Override
	public String toString() {
		return "Index [word=" + word + ", dbStatistic=" + dbStatistic + "]";
	}

}
