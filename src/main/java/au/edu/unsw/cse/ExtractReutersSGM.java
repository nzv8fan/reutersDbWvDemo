package au.edu.unsw.cse;

/**
 * Copyright 2017 Bradford Heap
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class to extract the Reuters-21578 dataset from SGM files and write them out to a sql database.
 * 
 * The dataset can be downloaded from: http://www.daviddlewis.com/resources/testcollections/reuters21578/
 * 
 * This approach for extracting the SGM files is based off: https://github.com/manishkanadje/reuters-21578/blob/master/ExtractReuters.java
 * 
 * This approach requires Java 8+
 * 
 * The database table is constructed as:
 * CREATE TABLE reuters_sgm (
 *   row_index_pk serial PRIMARY KEY,
 *   category text default NULL,
 *   title text NOT NULL,
 *   body text NOT NULL
 * );
 * 
 * @author Bradford Heap
 *
 */
public class ExtractReutersSGM {

	private static String TABLE_NAME = "reuters_sgm";
	
	private java.sql.Connection pconnection;
	
	/** 
	 * Inner class POD to store a document that we have extracted before writing it to the database.
	 * Despite the fields inside this class being private we can access them directly from the enclosing class. 
	 */
	private class Document {
		private String title;
		private String body;
		private List<String> topics;
		
		/**
		 * Method to write the document to the database, the document is duplicated if there are multiple topics, once for each topic.
		 * @throws SQLException thrown if there is an sql exception during the insertion of the data
		 */
		public void writeToDb() throws SQLException {

			// if there is no topic just write the document once
			if (topics == null) {
				String sql = "INSERT INTO " + TABLE_NAME + " (title, body) VALUES (?,?)";
				PreparedStatement pstmt = pconnection.prepareStatement(sql);
				pstmt.setString(1, title);
				pstmt.setString(2, body);
				pstmt.execute();

			} else {

				String sql = "INSERT INTO " + TABLE_NAME + " (category, title, body) VALUES (?,?,?)";
				for (String s : topics) {	// duplicate the document, once for each topic
					PreparedStatement pstmt = pconnection.prepareStatement(sql);
					pstmt.setString(1, s);
					pstmt.setString(2, title);
					pstmt.setString(3, body);
					pstmt.execute();
				}
			}
		}
	}
	
	/**
	 * Constructor for our main class
	 * @param jdbcUrl the database server connection URL
	 * @param jdbcDriverClassName the class name of the database server
	 * @throws ClassNotFoundException thrown if the database driver class is not found
	 * @throws SQLException thrown if there is an sql exception during the opening of the database connection
	 */
	public ExtractReutersSGM(String jdbcUrl, String jdbcDriverClassName) throws ClassNotFoundException, SQLException {
		// Get the jdbc driver
        Class.forName(jdbcDriverClassName);

        // connect to the server.
        pconnection = DriverManager.getConnection(jdbcUrl);
	}

	/**
	 * The main method for extracting the SGM files and writing them to the database. 
	 * @param args the program arguments
	 * @throws ClassNotFoundException thrown if the database driver class is not found
	 * @throws SQLException thrown if there is an sql exception
	 * @throws IOException thrown if an issue arises during the reading of the sgm files
	 * @throws FileNotFoundException thrown if the sgm files are not found. 
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException, FileNotFoundException, IOException {
		
		if (args.length < 1) {
			System.err.println("USAGE: java ExtractReutersSGM 'jdbc:postgresql://server.domain:port/database?user=username&password=password' <sgmFilesPath> <jdbcClassName>");
			return;
		}

		String serverUrl = args[0];

        String sgmFilesDirectory = ".";
        if (args.length > 1) {
            sgmFilesDirectory = args[1];
        }

		String jdbcDriverClassName = "org.postgresql.Driver";
		if (args.length > 2) {
			jdbcDriverClassName = args[2];
		}
		
		File reutersDir = new File(sgmFilesDirectory);
		if (!reutersDir.exists()) {
			System.err.println("Cannot find Path to Reuters SGM files (" + reutersDir + ")");
			return;
		}
		
		// create a list of the sgm files in the directory given
		File[] sgmFiles = reutersDir.listFiles(new FileFilter() {
			public boolean accept(File file) {
				return file.getName().endsWith(".sgm");
			}
		});
		
		ExtractReutersSGM main = null;
		try {
			// setup the non-static context to execute the extraction of SGM files in. 
			main = new ExtractReutersSGM(serverUrl,jdbcDriverClassName);
			
			if (sgmFiles != null && sgmFiles.length > 0) {
				for (File sgmFile : sgmFiles) {
					main.extractFile(sgmFile);
				}
			} else {
				System.err.println("No .sgm files in " + reutersDir);
			}

		} finally {
			// clean up before exiting. 
			if (main != null && main.pconnection != null) main.pconnection.close();
		}
	}
	
	/**
	 * Method to extract data from the SGM File
	 * @param sgmFile the file to extract data from
	 * @throws IOException thrown if an issue arises during the reading of the sgm files
	 * @throws FileNotFoundException thrown if the sgm file is not found
	 * @throws SQLException thrown if there is an sql exception during the insertion of the data
	 */
	private void extractFile(File sgmFile) throws FileNotFoundException, IOException, SQLException {
		try (BufferedReader reader = new BufferedReader(new FileReader(sgmFile))) {

			List<Pattern> patterns = new ArrayList<>();
			List<Pattern> multivaluedPatterns = new ArrayList<>();

			Pattern TITLE_PATTERN = Pattern.compile("<TITLE>(.*?)</TITLE>");
			patterns.add(TITLE_PATTERN);

			Pattern DATE_PATTERN = Pattern.compile(" <DATE>(.*?)</DATE>");
			patterns.add(DATE_PATTERN);

			Pattern BODY_PATTERN = Pattern.compile("<BODY>(.*?)</BODY>");
			patterns.add(BODY_PATTERN);

			Pattern DATELINE_PATTERN = Pattern
					.compile("<DATELINE>(.*?)</DATELINE>");
			patterns.add(DATELINE_PATTERN);

			Pattern UNKNOWN_PATTERN = Pattern
					.compile("<UNKNOWN>(.*?)</UNKNOWN>");
			patterns.add(UNKNOWN_PATTERN);

			Pattern ORGS_PATTERN = Pattern.compile("<ORGS>(.*?)</ORGS>");
			multivaluedPatterns.add(ORGS_PATTERN);

			Pattern TOPICS_PATTERN = Pattern.compile("<TOPICS>(.*?)</TOPICS>");
			multivaluedPatterns.add(TOPICS_PATTERN);

			Pattern PLACES_PATTERN = Pattern.compile("<PLACES>(.*?)</PLACES>");
			multivaluedPatterns.add(PLACES_PATTERN);

			Pattern PEOPLE_PATTERN = Pattern.compile("<PEOPLE>(.*?)</PEOPLE>");
			multivaluedPatterns.add(PEOPLE_PATTERN);

			Pattern EXCHANGES_PATTERN = Pattern
					.compile("<EXCHANGES>(.*?)</EXCHANGES>");
			multivaluedPatterns.add(EXCHANGES_PATTERN);

			Pattern COMPANIES_PATTERN = Pattern
					.compile("<COMPANIES>(.*?)</COMPANIES>");
			multivaluedPatterns.add(COMPANIES_PATTERN);

			Pattern D_PATTERN = Pattern.compile("<D>(.*?)</D>");

			StringBuilder buffer = new StringBuilder(10024);

			String line = null;
			Document document = null;

			// loop over the lines in the SGM file. 
			while ((line = reader.readLine()) != null) {

				// if we are not at the end of a reuters document
				if (line.indexOf("</REUTERS") == -1) {

					buffer.append(line).append(' '); // accumulate the strings

					// if the line is the start of a new reuters document. 
					if (line.contains("<REUTERS")) {
						
						// create a new document object 
						document = new Document();
						
						// the following block is retained from the original source code
						// this allows information to be extracted from the fields in the header tag
//						line = line.replace("<REUTERS", "");
//						line = line.replace(">", "");
//						String[] headers = line.trim().split(" ");
//						for (String header : headers) {
//							String head[] = header.split("=");
//							System.out.println("Key: " + head[0] + " Value: " + head[1].replace("\"", ""));
//						}
					}

				} else {  // if we are at the end of the reuters document then process the accumulated strings. 

					// loop over each of the patterns that may be found in the sgm document
					for (Pattern pattern : patterns) {
						Matcher matcher = pattern.matcher(buffer);
						while (matcher.find()) {
							for (int i = 1; i <= matcher.groupCount(); i++) {
								if (matcher.group(i) != null) {
									String string = pattern.pattern();
									String field = string.substring(
											string.indexOf("<") + 1,
											string.indexOf(">")).trim();
									
									if (field.startsWith("TITLE")) { // set the document title 
										document.title = matcher.group(i).replaceAll("\\s+", " ").trim().replaceAll("&lt;","<");
									} else if (field.startsWith("BODY")) { // set the document body
										document.body = matcher.group(i).replaceAll("&#127;", " ").replaceAll("\\s+", " ").trim().replaceAll("&lt;","<").replaceAll("Reuter &#3;","").replaceAll("REUTER &#3;","");
									}
								}
							}
						}
					}

					// loop over the patterns that are related to groups
					for (Pattern pattern : multivaluedPatterns) {
						Matcher matcher = pattern.matcher(buffer);
						while (matcher.find()) {
							for (int i = 1; i <= matcher.groupCount(); i++) {
								if (matcher.group(i) != null) {
									String text = matcher.group(i);
									String string = pattern.pattern();
									String field = string.substring(
											string.indexOf("<") + 1,
											string.indexOf(">")).trim();
									if (field.startsWith("TOPICS")) { // set the topics associated with a document. 
										if (text != null && !text.trim().isEmpty()) {
											List<String> values = new ArrayList<>();
	
											Matcher mat = D_PATTERN.matcher(text);
											while (mat.find()) {
												for (int j = 1; j <= mat
														.groupCount(); j++) {
													if (mat.group(j) != null) {
														values.add(mat.group(j));
													}
												}
											}
											document.topics = values;
										}
									}
								}
							}
						}
					}

					// if the document isn't empty then write it out to the database. 
					if (document.body != null) {
						document.writeToDb();
					}

					// reset the string buffer we are accumulating sgm data into for a new document. 
					buffer = new StringBuilder(10024);
				}
			}	
		}
	}

}

