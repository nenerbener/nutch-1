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
import com.nenerbener.CLIJOptSimple;
import joptsimple.OptionException;
import joptsimple.ValueConversionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import static joptsimple.util.RegexMatcher.*;
// import org.json.JSONObject;

/**
 * YoutubeCCReader performs multiple tasks should be separated into individual tasks in a later design.
 * 1) Performs NER on parse data (main free text content).
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
 * @see IndexingFilter#filter
 * @return doc
 * @param parse
 * @param datum
 * @param inLinks
 */
public class YoutubeCCReader implements IndexingFilter {

	// process commandline parameters
	private String inputFile;
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
	CLIJOptSimple cli = null;
	
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

		//convert conf parameters to String[] args for JoptSimple commandline parsing.
		//initialize CLIJOptSimple 
		if (cli == null) {
			cli = new CLIJOptSimple();
			if (cli == null) {
				LOG.error("CLIOptSimple not initialized successfully - terminating program");
				System.exit(-1); //exit if fail
			}
		}
		
		//create string and split between spaces
		inputFile = url.toString(); // convert URL from Hadoop Text to String class
		StringBuilder sbarg = new StringBuilder("-i ").append(inputFile)
				.append(" ").append("-o ").append(outputDir);
		if (settingDebugOption)
			sbarg.append(" ").append("-d");
		if (settingIncludeTitleOption)
			sbarg.append(" ").append("-t");
		if (settingIncludeTrackTitleOption)
			sbarg.append(" ").append("-r");
		if (settingRemoveTimingSubtitleOption)
			sbarg.append(" ").append("-m");
		String[] args = sbarg.toString().split(" ");

		try {
			Boolean bl=cli.readCLI(args);
			LOG.info("Successfully read youtube input file, outputdir and other conf params");
		} catch (OptionException e) {
			LOG.error(e.getMessage());
			System.exit(0);
		} catch (NullPointerException e) {
			LOG.error(e.getMessage());
			System.exit(0);
		}
		
		
		
		//        JSONObject jNames = new JSONObject(names);
		//        System.out.println(jNames.toString(2));

		return doc;
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
		// process commandline parameters
		this.outputDir = conf.get("indexer.setting.output.dir.option"); // output directory option (default to /tmp inside readCLI() method)
		this.settingDebugOption = conf.getBoolean("indexer.setting.debug.option", false); // debug option
		this.settingIncludeTitleOption = conf.getBoolean("indexer.setting.include.title.option", false); // include title option
		this.settingIncludeTrackTitleOption = conf.getBoolean("indexer.setting.include.track.title.option", false); // include track title option
		this.settingRemoveTimingSubtitleOption = conf.getBoolean("indexer.setting.remove.timing.subtitle.option", true); // remove timing subtitles option
	}
}
