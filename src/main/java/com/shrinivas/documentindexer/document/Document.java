package com.shrinivas.documentindexer.document;

public class Document{
	private String path;
	private double tf;

	public Document() {
		// TODO Auto-generated constructor stub
	}

	public Document(String path, double tf) {
		super();
		this.path = path;
		this.tf = tf;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public double getTf() {
		return tf;
	}

	public void setTf(double tf) {
		this.tf = tf;
	}

	@Override
	public String toString() {
		return "Document [path=" + path + ", tf=" + tf + "]";
	}

}
