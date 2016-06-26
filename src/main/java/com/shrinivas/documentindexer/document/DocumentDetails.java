package com.shrinivas.documentindexer.document;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "documentdetail")
public class DocumentDetails {

	@Id
	private String filePath;

	private double wordCount;

	public DocumentDetails() {
		// TODO Auto-generated constructor stub
	}

	public DocumentDetails(String filePath, double wordCount) {
		super();
		this.filePath = filePath;
		this.wordCount = wordCount;
	}

	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	public double getWordCount() {
		return wordCount;
	}

	public void setWordCount(double wordCount) {
		this.wordCount = wordCount;
	}

	@Override
	public String toString() {
		return "DocumentDetails [filePath=" + filePath + ", wordCount=" + wordCount + "]";
	}

}
