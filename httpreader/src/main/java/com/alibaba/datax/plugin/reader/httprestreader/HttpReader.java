package com.alibaba.datax.plugin.reader.httprestreader;

import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.httprestreader.utils.JSONReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class HttpReader extends Reader {

    public static class Job extends Reader.Job {

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originConfig = null;

        private String url;

        private String table;

        @Override
        public void init() {
            this.originConfig = this.getPluginJobConf();
            this.url = originConfig.getString(Key.URL);
            this.table = originConfig.getString(Key.TABLE);

            this.validateParameter();
        }

        private void validateParameter() {

//            String encoding = this.originConfig
//                    .getString(
//                            com.alibaba.datax.plugin.unstructuredstorage.reader.Key.ENCODING,
//                            com.alibaba.datax.plugin.unstructuredstorage.reader.Constant.DEFAULT_ENCODING);
//            if (StringUtils.isBlank(encoding)) {
//                this.originConfig
//                        .set(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.ENCODING,
//                                com.alibaba.datax.plugin.unstructuredstorage.reader.Constant.DEFAULT_ENCODING);
//            } else {
//                try {
//                    encoding = encoding.trim();
//                    this.originConfig
//                            .set(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.ENCODING,
//                                    encoding);
//                    Charsets.toCharset(encoding);
//                } catch (UnsupportedCharsetException uce) {
//                    throw DataXException.asDataXException(
//                            HttpReaderErrorCode.ILLEGAL_VALUE,
//                            String.format("不支持您配置的编码格式 : [%s]", encoding), uce);
//                } catch (Exception e) {
//                    throw DataXException.asDataXException(
//                            HttpReaderErrorCode.CONFIG_INVALID_EXCEPTION,
//                            String.format("编码配置异常, 请联系我们: %s", e.getMessage()),
//                            e);
//                }
//            }
        }

        @Override
        public void prepare() {
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            LOG.debug("split() begin...");
            List<Configuration> readerSplitConfigs = new ArrayList<Configuration>();

            Configuration splitedConfig = this.originConfig.clone();
            splitedConfig.set(Constant.URL, this.url);
            splitedConfig.set(Constant.TABLE, this.table);
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
        private String url;


        @Override
        public void init() {
            this.readerSliceConfig = this.getPluginJobConf();
            this.url = this.readerSliceConfig.getString(Constant.URL);
        }

        @Override
        public void prepare() {
        }

        @lombok.SneakyThrows
        @Override
        public void startRead(RecordSender recordSender) {
            LOG.debug("start read source files...");

            JSONReader.readFromUrl(url, this.readerSliceConfig, recordSender);
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
