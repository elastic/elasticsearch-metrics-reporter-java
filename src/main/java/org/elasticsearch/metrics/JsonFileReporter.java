package org.elasticsearch.metrics;

import com.codahale.metrics.Clock;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Reporter producing metrics in Logstash/JSON format store them in files (one metric per line).
 */
public class JsonFileReporter extends BaseJsonReporter {

    public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }

    public static class Builder extends BaseJsonReporter.Builder<JsonFileReporter, JsonFileReporter.Builder> {
        private File directory;
        private String file = "metrics";
        private String fileDateFormat = "yyyy-MM";
        private String fileExtension = "json";

        private Builder(MetricRegistry registry) {
            super(registry);
        }

        /**
         * The directory containing files
         */
        public Builder directory(File directory) {
            this.directory = directory;
            return this;
        }

        /**
         * The file prefix
         */
        public Builder file(String file) {
            this.file = file;
            return this;
        }

        /**
         * The index date format used for rolling files
         * This is appended to the index name, split by a '-'
         */
        public Builder fileDateFormat(String fileDateFormat) {
            this.fileDateFormat = fileDateFormat;
            return this;
        }

        /**
         * The file extension
         */
        public Builder fileExtension(String fileExtension) {
            this.fileExtension = fileExtension;
            return this;
        }

        public JsonFileReporter build() throws IOException {
            return new JsonFileReporter(registry,
                    directory,
                    file,
                    fileDateFormat,
                    fileExtension,
                    clock,
                    prefix,
                    rateUnit,
                    durationUnit,
                    filter,
                    timestampFieldname,
                    additionalFields);
        }

    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchReporter.class);

    /**
     * Directory where files will be written
     */
    private final File directory;
    private final String file;
    private final DateFormat fileDateFormat;
    private final String fileExtension;

    public JsonFileReporter(MetricRegistry registry, File directory, String file, String fileDateFormat, String fileExtension, Clock clock, String prefix, TimeUnit rateUnit, TimeUnit durationUnit, MetricFilter filter, String timestampFieldname, Map<String, Object> additionalFields) {
        super(registry, "json-file-reporter", clock, prefix, rateUnit, durationUnit, filter, timestampFieldname, additionalFields);
        this.directory = directory;
        this.file = file;
        if (fileDateFormat != null && fileDateFormat.length() > 0) {
            this.fileDateFormat = new SimpleDateFormat(fileDateFormat);
        } else {
            this.fileDateFormat = null;
        }
        this.fileExtension = fileExtension;
    }

    private class Report implements BaseJsonReporter.Report {
        FileOutputStream out;

        public Report(String fileName) throws FileNotFoundException {
            out = new FileOutputStream(new File(directory, fileName), true);
        }

        @Override
        public void add(JsonMetrics.JsonMetric jsonMetric, AtomicInteger entriesWritten) throws IOException {
            writer.writeValue(out, jsonMetric);
            out.write("\n".getBytes());

            out.flush();
        }

        @Override
        public void close() throws IOException {
            out.close();
        }
    }

    @Override
    protected Report startReport(long timestamp) {
        String fileName = file;
        if (fileDateFormat!= null) {
            fileName += "-" + fileDateFormat.format(new Date(timestamp * 1000));
        }
        fileName += "." + fileExtension;
        try {
            return new Report(fileName);
        } catch (FileNotFoundException e) {
            LOGGER.error("Could not connect write file {}", fileName);
            return null;
        }
    }

}