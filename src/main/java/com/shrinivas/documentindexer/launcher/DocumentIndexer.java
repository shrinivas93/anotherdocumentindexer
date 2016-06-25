package com.shrinivas.documentindexer.launcher;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.shrinivas.documentindexer.document.DbStatistic;
import com.shrinivas.documentindexer.document.Document;
import com.shrinivas.documentindexer.document.Index;
import com.shrinivas.documentindexer.document.Statistic;
import com.shrinivas.documentindexer.repository.IndexRepository;

@Component
public class DocumentIndexer {

	private static final Logger LOGGER = LogManager.getLogger(DocumentIndexer.class);

	@Autowired
	private IndexRepository indexRepository;

	@Value("${source.file}")
	private String sourceFile;

	public void process() throws IOException {
		List<File> indexableFiles = getSourceFiles();
		LOGGER.info("Found " + indexableFiles.size() + " indexable files.");
		Map<String, Statistic> index = convertIndexToMap(indexRepository.findAll());
		index = startIndexing(indexableFiles, index);
		LOGGER.info("Indexed " + index.size() + " words");
		List<Index> indices = convertMapToDocument(index);
		LOGGER.info("Storing indices to DB");
		indexRepository.save(indices);
	}

	private Map<String, Statistic> convertIndexToMap(List<Index> findAll) {
		Map<String, Statistic> indices = new TreeMap<>();
		for (Index index : findAll) {
			DbStatistic dbStatistic = index.getDbStatistic();
			Map<String, Document> documents = new TreeMap<>();
			for (Document document : dbStatistic.getDocuments()) {
				documents.put(document.getPath(), document);
			}
			Statistic statistic = new Statistic(dbStatistic.getWord(), dbStatistic.getIdf(), documents);
			indices.put(index.getWord(), statistic);
		}
		return indices;
	}

	private List<Index> convertMapToDocument(Map<String, Statistic> index) {
		List<Index> indices = new ArrayList<>();
		Set<String> words = index.keySet();
		for (String word : words) {
			Statistic statistic = index.get(word);
			List<Document> dbDocuments = new ArrayList<>(statistic.getDocuments().values());
			DbStatistic dbStatistic = new DbStatistic(statistic.getWord(), statistic.getIdf(), dbDocuments);
			indices.add(new Index(word, dbStatistic));
		}
		return indices;
	}

	private Map<String, Statistic> startIndexing(List<File> indexableFiles, Map<String, Statistic> index)
			throws IOException {
		BufferedReader br;
		for (File file : indexableFiles) {
			String filePath = file.getAbsolutePath();
			try {
				FileReader fr = new FileReader(file);
				br = new BufferedReader(fr);
				String str;
				while ((str = br.readLine()) != null) {
					String[] words = str.split(" ");
					for (String word : words) {
						if (!index.containsKey(word)) {
							index.put(word, new Statistic(word, 0, new TreeMap<String, Document>()));
						}
						index.get(word).getDocuments().put(filePath, new Document(filePath, 0));
					}
				}
			} catch (FileNotFoundException ex) {
				LOGGER.error(ex.getMessage());
			}
		}
		return index;
	}

	private List<File> getSourceFiles() {
		List<File> sourceFilePath = null;
		try {
			sourceFilePath = getSourceFileContents();
		} catch (IOException e) {
			LOGGER.fatal("Unable to open the file '" + sourceFile + "'");
			LOGGER.error(e);
		}
		List<File> indexableFiles = new ArrayList<>();
		return getIndexableFiles(sourceFilePath, indexableFiles);
	}

	private List<File> getIndexableFiles(List<File> sourceFilePath, List<File> indexableFiles) {
		for (File file : sourceFilePath) {
			if (file.isFile()) {
				indexableFiles.add(file);
			} else {
				File[] filesArr = file.listFiles(new FilenameFilter() {
					@Override
					public boolean accept(File dir, String name) {
						File file = new File(dir, name);
						if (file.isFile()) {
							String extension = FilenameUtils.getExtension(file.getAbsolutePath());
							return extension.equalsIgnoreCase("txt");
						}
						return true;
					}
				});
				if (filesArr != null) {
					List<File> files = Arrays.asList(filesArr);
					getIndexableFiles(files, indexableFiles);
				}
			}
		}
		return indexableFiles;
	}

	private List<File> getSourceFileContents() throws IOException {
		List<File> sourceFilePath = new ArrayList<>();
		FileReader fr = new FileReader(sourceFile);
		BufferedReader br = new BufferedReader(fr);
		String str;
		while ((str = br.readLine()) != null) {
			sourceFilePath.add(new File(str));
		}
		br.close();
		return sourceFilePath;
	}
}
