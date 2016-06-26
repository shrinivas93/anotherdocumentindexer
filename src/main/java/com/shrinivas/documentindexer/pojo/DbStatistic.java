package com.shrinivas.documentindexer.pojo;

import java.util.List;

public class DbStatistic {
	private String word;
	private double idf;
	private List<Document> documents;

	public DbStatistic() {
		// TODO Auto-generated constructor stub
	}

	public DbStatistic(String word, double idf, List<Document> documents) {
		super();
		this.word = word;
		this.idf = idf;
		this.documents = documents;
	}

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public double getIdf() {
		return idf;
	}

	public void setIdf(double idf) {
		this.idf = idf;
	}

	public List<Document> getDocuments() {
		return documents;
	}

	public void setDocuments(List<Document> documents) {
		this.documents = documents;
	}

	@Override
	public String toString() {
		return "Statistic [word=" + word + ", idf=" + idf + ", documents=" + documents + "]";
	}

}
