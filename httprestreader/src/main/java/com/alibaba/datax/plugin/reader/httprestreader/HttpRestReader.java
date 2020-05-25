package com.alibaba.datax.plugin.reader.httprestreader;

import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.httprestreader.utils.JSONReader2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class HttpRestReader extends Reader {

    public static class Job extends Reader.Job {

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originConfig = null;

        private String table;

        private String url;

        private String listNode;

        private String primaryKey;

        @Override
        public void init() {
            this.originConfig = this.getPluginJobConf();
            this.table = originConfig.getString(Key.TABLE);
            this.url = originConfig.getString(Key.URL);
            this.listNode = originConfig.getString(Key.LIST_NODE);
            this.primaryKey = originConfig.getString(Key.PRIMARYKEY);

            this.validateParameter();
        }

        private void validateParameter() {

        }

        @Override
        public void prepare() {
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            LOG.debug("split() begin...");
            List<Configuration> readerSplitConfigs = new ArrayList<Configuration>();

            Configuration splitedConfig = this.originConfig.clone();
            splitedConfig.set(Constant.TABLE, this.table);
            splitedConfig.set(Constant.URL, this.url);
            splitedConfig.set(Constant.LIST_NODE, this.listNode);
            splitedConfig.set(Constant.PRIMARYKEY, this.primaryKey);

            readerSplitConfigs.add(splitedConfig);

            return readerSplitConfigs;
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }

        private <T> List<List<T>> splitSourceFiles(final List<T> sourceList, int adviceNumber) {
            List<List<T>> splitedList = new ArrayList<List<T>>();
            int averageLength = sourceList.size() / adviceNumber;
            averageLength = averageLength == 0 ? 1 : averageLength;

            for (int begin = 0, end = 0; begin < sourceList.size(); begin = end) {
                end = begin + averageLength;
                if (end > sourceList.size()) {
                    end = sourceList.size();
                }
                splitedList.add(sourceList.subList(begin, end));
            }
            return splitedList;
        }
    }

    public static class Task extends Reader.Task {
        private static Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration readerSliceConfig;
        private String table;
        private String url;
        private String listNode;
        private String primaryKey;

        @Override
        public void init() {
            this.readerSliceConfig = this.getPluginJobConf();
            this.table = this.readerSliceConfig.getString(Constant.TABLE);
            this.url = this.readerSliceConfig.getString(Constant.URL);
            this.listNode = this.readerSliceConfig.getString(Constant.LIST_NODE);
            this.primaryKey = this.readerSliceConfig.getString(Constant.PRIMARYKEY);
        }

        @Override
        public void prepare() {
        }

        @lombok.SneakyThrows
        @Override
        public void startRead(RecordSender recordSender) {
            LOG.debug("start read source files...");

            JSONReader2.readFromUrl(url, table, listNode, primaryKey, readerSliceConfig, recordSender);
            recordSender.flush();
            LOG.debug("end read source files...");
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }
    }

}
