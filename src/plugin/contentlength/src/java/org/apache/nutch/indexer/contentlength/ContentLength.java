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

package org.apache.nutch.indexer.contentlength;

import java.lang.invoke.MethodHandles;

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

/**
 * Plugin to add content length to Index
*/
public class ContentLength implements IndexingFilter {

	private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
	private static final String CONF_PROPERTY = "contentlength.tags";
	// private static String[] urlMetaTags;
	private Configuration conf;

	/**
	 * This will take the metatags that you have listed in your "urlmeta.tags"
	 * property, and looks for them inside the CrawlDatum object. If they exist,
	 * this will add it as an attribute inside the NutchDocument.
	 * 
	 * @see IndexingFilter#filter
	 */
	// implements the filter-method which gives you access to important Objects like
	// NutchDocument
	public NutchDocument filter(NutchDocument doc, Parse parse, Text url, CrawlDatum datum, Inlinks inlinks) {
		String content = parse.getText();
		// adds the new field to the document
		doc.add("contentlength", content.length());
		return doc;
	}

	/** Boilerplate */
	public Configuration getConf() {
		return conf;
	}

	//Boilerplate
	public void setConf(Configuration conf) {
		this.conf = conf;
	}
}
