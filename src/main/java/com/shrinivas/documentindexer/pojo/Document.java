package com.shrinivas.documentindexer.pojo;

public class Document {
	private String path;
	private double tf;
	private double tfIdf;

	public Document() {
		// TODO Auto-generated constructor stub
	}

	public Document(String path, double tf, double tfIdf) {
		super();
		this.path = path;
		this.tf = tf;
		this.tfIdf = tfIdf;
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

	public double getTfIdf() {
		return tfIdf;
	}

	public void setTfIdf(double tfIdf) {
		this.tfIdf = tfIdf;
	}

	@Override
	public String toString() {
		return "Document [path=" + path + ", tf=" + tf + ", tfIdf=" + tfIdf + "]";
	}

	public void incrementTf() {
		this.tf++;
	}

}
