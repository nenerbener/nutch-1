/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.indexer.youtubeccreader;

import java.lang.invoke.MethodHandles;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.lang.StringBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.parse.Parse;
import org.apache.tika.parser.ner.corenlp.CoreNLPNERecogniser;

import java.io.File;
import joptsimple.OptionException;
import joptsimple.ValueConversionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import static joptsimple.util.RegexMatcher.*;
import com.nenerbener.driverSRT.DriverSRT;
import com.google.common.base.*;
import org.jdom.Document;

/**
 * YoutubeCCReader performs multiple tasks. This monolithic implementation was due to the ease of accessing the NutchDocument object.
 * 1) Performs NER on parse data (static webpage's main content).
 * 2) identifies Youtube pages with embedded video and downloads the default closed caption data where available.
 * 3) The method then applies NER on the CC data. 
 * 4) Parse NER data, Youtube closed caption data, closed caption NER data and cc timing data are then added to the
 * {@link NutchDocument} and sent to an external indexer (the default Solr).
 * 
 * The NER recognizer ({@link CoreNLPNERecogniser}) returns Map<String, Set,String>> which
 * is a map containing entries of key-value pairs: key=("PERSON"|"ORGANIZATION",..),
 * value (list of persons|list of organizations,...).The lists of persons,..,organizations 
 * are extracted from the parsed text.
 * 
 * To send to Solr through REST API, Nutch conf/schema.xml must be copied to Solr nutch core conf directory/managed-schema file
 * 
 * @see IndexingFilter#filter
 * @return doc
 * @param parse
 * @param datum
 * @param inLinks
 */
public class YoutubeCCReader implements IndexingFilter {

	DriverSRT dsrt; //Youtube closed caption processor
	String regexInputFile; //Youtube.com page regex
	String regexOutputDir; //Regex to avoid mkdir to make non-alphabet starting output dir
	OptionSet options; //post-parsed options
	String outputDirDefault; //default CC directory. Use /tmp after integration with solr
//	String parsedInputFile; 
	String inputFile;
	private String outputDir;
	private File fileOutputDir;
	private Boolean settingDebugOption; // debug option default (set in method setConf())
	private Boolean settingIncludeTitleOption; // include title option default (set in method setConf())
	private Boolean settingIncludeTrackTitleOption; // include track title option default (set in method setConf())
	private Boolean settingRemoveTimingSubtitleOption; // remove timing subtitles option default (set in method setConf())

	private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
	private static final String CONF_PROPERTY = "youtubeccreader.tags";
	// private static String[] urlMetaTags;
	private Configuration conf;

	CoreNLPNERecogniser ner = null;
//	CLIJOptSimple cli = null;
	
	Document domDoc; // DOM document returned from retrieveSRT

	class FileCreateException extends Exception
	{
		// Parameterless Constructor
		public FileCreateException() {}

		// Constructor that accepts a message
		public FileCreateException(String message)
		{
			super(message);
		}
	}
	
	public NutchDocument filter(NutchDocument doc, Parse parse, Text url, CrawlDatum datum, Inlinks inlinks) {
		
		//parse.getText() returns the main body of unstructured text containing person and entity names
		String content = parse.getText();
		
		//initialize CoreNLPNERecogniser and load dictionaries (FIX: dictionary name is hardcoded)
		if (ner == null) {
			ner = new CoreNLPNERecogniser();
			if (ner == null) {
				LOG.error("CoreNLPNERegcognizer not initialized successfully - terminating program");
				System.exit(-1); //exit if fail
			}
		}

		// call NER recognize() on parse content returning the map of key-value pairs described above.
		Map<String, Set<String>> names = ner.recognise(content);
		
		//loop through NER results, flatten to Map<String>,String> using StringBuilder and add map entries to doc
		Set<Map.Entry<String,Set<String>>> es = names.entrySet();
		
		//iterate through keys
		Iterator<Map.Entry<String,Set<String>>> iterator = es.iterator();
		while (iterator.hasNext()) {
			Map.Entry<String,Set<String>> me = iterator.next();
			String key = me.getKey();
			
			//iterate through values
			Set<String> esv = me.getValue();
			
			//iterate through names, entity set
			Iterator<String> setIterator = esv.iterator();
			String strofnames = new String();
			while (setIterator.hasNext()) {
				String value = setIterator.next();
				
				//build appended string of names, entities,...
				strofnames = new StringBuilder(strofnames).append(value).toString();
				
				//add " : " separator, do not add at end of appended string
				if (setIterator.hasNext()) strofnames = new StringBuilder(strofnames).append(" : ").toString();
			}
			System.out.println("key val: " + key + ": " + strofnames);
			
			// add key, entities to doc
			doc.add(key, strofnames);
		}

		// This is the URL to check if Youtube video, if valid check if there is an attached default CC and download
		// If not, just continue. Will want to add exception and case handling at a later time.
		String[] inputFileArgs = {"--inputFile", url.toString()};
		String[] outputDirArgs = {"--outputDir", outputDir};
	
		//create optionParser (arguments and characters template to parse against)
		OptionParser optionParser = new OptionParser();
		optionParser.accepts("inputFile").withRequiredArg().withValuesConvertedBy(regex(regexInputFile));

//		if (!controller.processInputURL()) {
//		LOG.info("Method exited abnormally. URL has not attached closed caption track.");
//		return;
//	} 
//	if (!controller.convertSubtitlesTracks()) {
//		LOG.info("Method abnormally. Could not download closed caption data successfully.");
//		return;
//	}
		//perform parsing of args against created optionParser
		try {
			options = optionParser.parse(inputFileArgs);
			inputFile = (String) options.valueOf("inputFile");
			optionParser = new OptionParser();
			optionParser.accepts("outputDir").withRequiredArg().withValuesConvertedBy(regex(regexOutputDir))
			.defaultsTo(outputDirDefault);
			try {
				options = optionParser.parse(outputDirArgs);
				outputDir = (String) options.valueOf("outputDir");
			} catch (OptionException e) {
				LOG.warn(e.getMessage());
				LOG.warn("error creating ouputDir. Defaulting to " + outputDirDefault);
				outputDir=outputDirDefault;
				throw new FileCreateException();
			}
			fileOutputDir = new File(outputDir.toString());
			if (!fileOutputDir.exists())  {
				if (fileOutputDir.mkdirs()) {
					LOG.info("Output directory is created: " + fileOutputDir);
				} 
				else {
					outputDir = outputDirDefault;
					LOG.warn("error creating ouputDir. Defaulting to " + outputDirDefault);
				} 
			} //from this point inputFile and outputDir are valid and method to DriverSRT call below
		} catch (OptionException e) {
			LOG.error(e.getMessage());
			return doc;
		} catch (NullPointerException e) {
			LOG.error(e.getMessage());
			return doc;
		} catch (FileCreateException e) {
		} //at this point, the inputFile (URL) is regex'ed correctly and the outputDir is created or set to default
		
		// call Youtube CC reader
		dsrt= new DriverSRT(
				inputFile,
				outputDir,
				settingDebugOption,
				settingIncludeTitleOption,
				settingIncludeTrackTitleOption,
				settingRemoveTimingSubtitleOption);

		//        JSONObject jNames = new JSONObject(names);
		//        System.out.println(jNames.toString(2));

		// retrieve the closed caption DOM Document, if exists
//		try {
			domDoc = dsrt.retrieveSRT();
//		} 
		if(domDoc == null) {
			return doc; //return Nutch Document without any Youtube CC addition
		}
		
		// process variable domDoc (DOM Document) and add to variable doc (Nutch Document)
		String ccString = dsrt.processSRT();
		doc.add("closedcaption",ccString);
		
		//NER for Youtube Closed Caption (This will add additional Solr tags
		//cc-ner-location, cc-ner-organization, cc-ner-date, cc-ner-money, cc-ner-person, cc-ner-time, cc-ner-percent
		//parse.getText() returns the main body of unstructured text containing person and entity names
		
		//initialize CoreNLPNERecogniser and load dictionaries (FIX: dictionary name is hardcoded)
		if (ner == null) {
			ner = new CoreNLPNERecogniser();
			if (ner == null) {
				LOG.error("CoreNLPNERegcognizer not initialized successfully - terminating program");
				System.exit(-1); //exit if fail
			}
		}

		// call NER recognize() on parse content returning the map of key-value pairs described above.
		Map<String, Set<String>> ccnames = ner.recognise(ccString);
		
		//loop through NER results, flatten to Map<String>,String> using StringBuilder and add map entries to doc
		Set<Map.Entry<String,Set<String>>> cces = ccnames.entrySet();
		
		//iterate through keys
		Iterator<Map.Entry<String,Set<String>>> cciterator = cces.iterator();
		while (cciterator.hasNext()) {
			Map.Entry<String,Set<String>> me = cciterator.next();
			String key = me.getKey();
			String ccKey = "CC-" + key;
			
			//iterate through values
			Set<String> esv = me.getValue();
			
			//iterate through names, entity set
			Iterator<String> setIterator = esv.iterator();
			String strofnames = new String();
			while (setIterator.hasNext()) {
				String value = setIterator.next();
				
				//build appended string of names, entities,...
				strofnames = new StringBuilder(strofnames).append(value).toString();
				
				//add " : " separator, do not add at end of appended string
				if (setIterator.hasNext()) strofnames = new StringBuilder(strofnames).append(" : ").toString();
			}
			System.out.println("key val: " + ccKey + ": " + strofnames);
			
			// add key, entities to doc
			doc.add(ccKey, strofnames);
		}

		return doc; // return Nutch Document with Youtube CC addition
//		catch (Exception e) {
//			return doc; //return Nutch Document without any Youtube CC addition
//		}
//		
//		return doc; // return Nutch Document with Youtube CC addition
	}

	/**
	 * getConf - return ContentNER configuration
	 * @return conf - configuration used by ContentNER
	 */
	public Configuration getConf() {
		return conf;
	}

	/**
	 * setConf - set configuration
	 * @param conf - set configuration used by ContentNER
	 */
	public void setConf(Configuration conf) {
		this.conf = conf;

		// regex and defaults for Youtube video pages
		this.regexInputFile = conf.get("indexer.setting.youtubeccreader.regex.input.file",
				"^https?://(www.)?youtube.com/watch\\?v=[\\w-=]{11}$");
		this.regexOutputDir = "^[^-+&@#%?=~|!:,;].+"; //Regex to avoid mkdir to make non-alphabet starting output dir
		this.outputDirDefault = System.getProperty( "java.io.tmpdir" ); //returns static, is this legal?
		this.outputDir = conf.get("indexer.setting.youtubeccreader.output.dir.option",outputDirDefault); // output directory option (default to /tmp inside readCLI() method)

		// read parameters from nutch configuration files (nutch-default.xml or nutch-site.xml)
		this.settingDebugOption = conf.getBoolean("indexer.setting.debug.option", false); // debug option
		this.settingIncludeTitleOption = conf.getBoolean("indexer.setting.include.title.option", false); // include title option
		this.settingIncludeTrackTitleOption = conf.getBoolean("indexer.setting.include.track.title.option", false); // include track title option
		this.settingRemoveTimingSubtitleOption = conf.getBoolean("indexer.setting.remove.timing.subtitle.option", true); // remove timing subtitles option

//		String regexInputFileOrig = "^https?://(www.)?youtube.com/watch\\?v=[\\w-=]{11}$"; //Youtube.com page regex
//		Boolean bool = regexInputFile.equals(regexInputFileOrig);
//		regexInputFile = "^https?://(www.)?youtube.com/watch\\?v=[\\w-=]{11}$"; //Youtube.com page regex
		LOG.info("className: " + MethodHandles.lookup().lookupClass());
		LOG.info("regexInputFile: " + regexInputFile);
		LOG.info("regexOutputDir: " + regexOutputDir);
		LOG.info("outputDirDefault: " + outputDirDefault);
	
	}
}
