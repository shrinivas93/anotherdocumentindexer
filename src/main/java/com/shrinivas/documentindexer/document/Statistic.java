package com.shrinivas.documentindexer.document;

import java.util.Map;

public class Statistic {
	private String word;
	private double idf;
	private Map<String, Document> documents;

	public Statistic() {
		// TODO Auto-generated constructor stub
	}

	public Statistic(String word, double idf, Map<String, Document> documents) {
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

	public Map<String, Document> getDocuments() {
		return documents;
	}

	public void setDocuments(Map<String, Document> documents) {
		this.documents = documents;
	}

	@Override
	public String toString() {
		return "Statistic [word=" + word + ", idf=" + idf + ", documents=" + documents + "]";
	}

}
