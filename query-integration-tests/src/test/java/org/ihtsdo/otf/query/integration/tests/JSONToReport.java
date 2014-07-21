/*
 * Copyright 2014 International Health Terminology Standards Development Organisation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ihtsdo.otf.query.integration.tests;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang3.StringUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * Loads a {@link java.util.Map} of results.
 *
 * @author dylangrald
 */
public class JSONToReport {

    private final String fileName;
    private final Map<String, Object> resultMap;

    public JSONToReport(String fileName) {
        this.fileName = fileName;
        this.resultMap = new HashMap<>();
    }

    public void parseFile() {
        String line;
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
            while ((line = reader.readLine()) != null) {
                this.loadMap(line);
            }
        } catch (FileNotFoundException ex) {
            Logger.getLogger(JSONToReport.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(JSONToReport.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Load both set cardinality reports and reports for the specific
     * {@link java.util.Set} values.
     *
     * @param jsonLine
     */
    private void loadMap(String jsonLine) {
        try {
            JSONObject json = (JSONObject) new JSONParser().parse(jsonLine);
            for (Object key : json.keySet()) {
                String value = json.get(key).toString();
                if (value.matches("[0-9]+")) {
                    this.resultMap.put(key.toString(), Integer.parseInt(value));
                } else if (value.matches("\\[[0-9]+.*\\]")) {
                    Set<Long> longSet = new HashSet<>();
                    for (String l : value.split(",")) {
                        l = StringUtils.remove(l, '[');
                        l = StringUtils.remove(l, ']');
                        l = l.trim();
                        if (l.matches("[0-9]+")) {
                            longSet.add(Long.parseLong(l));
                        }
                    }
                    this.resultMap.put(key.toString(), longSet);
                }

            }
        } catch (ParseException ex) {
            Logger.getLogger(JSONToReport.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public int getQueryCount(String key) {
        Object value = this.resultMap.get(key);
        if (value instanceof Integer) {
            return (Integer) value;
        } else {
            throw new IllegalArgumentException("Can't find a value for the key: " + key);
        }
    }

    public Set<Long> getQuerySet(String key) {
        Object value = this.resultMap.get(key);
        if (value instanceof Set) {
            return (Set) value;
        } else {
            throw new IllegalArgumentException("Can't find a value for the key: " + key);
        }
    }
}
