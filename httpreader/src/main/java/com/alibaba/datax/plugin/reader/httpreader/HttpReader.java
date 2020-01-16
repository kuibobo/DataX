package com.alibaba.datax.plugin.reader.httpreader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.httpreader.utils.WebClient;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderUtil;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class HttpReader extends Reader {

    public static class Job extends Reader.Job {

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originConfig = null;

        private List<String> sourceFiles;


        @Override
        public void init() {
            this.originConfig = this.getPluginJobConf();
            this.sourceFiles = originConfig.getList(Key.URLS, String.class);

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

            // warn:每个slice拖且仅拖一个文件,
            // int splitNumber = adviceNumber;
            int splitNumber = this.sourceFiles.size();
            if (0 == splitNumber) {
                throw DataXException.asDataXException(HttpReaderErrorCode.EMPTY_DIR_EXCEPTION,
                        String.format("未能找到待读取的文件,请确认您的配置项path: %s", this.originConfig.getString(Key.URLS)));
            }

            List<List<String>> splitedSourceFiles = this.splitSourceFiles(
                    this.sourceFiles, splitNumber);
            for (List<String> files : splitedSourceFiles) {
                Configuration splitedConfig = this.originConfig.clone();
                splitedConfig.set(Constant.SOURCE_FILES, files);
                readerSplitConfigs.add(splitedConfig);
            }
            LOG.debug("split() ok and end...");
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
        private List<String> sourceFiles;


        @Override
        public void init() {
            this.readerSliceConfig = this.getPluginJobConf();
            this.sourceFiles = this.readerSliceConfig.getList(Constant.SOURCE_FILES, String.class);
        }

        @Override
        public void prepare() {
        }

        @lombok.SneakyThrows
        @Override
        public void startRead(RecordSender recordSender) {
            LOG.debug("start read source files...");

            WebClient client = new WebClient();
            for (String url : this.sourceFiles) {
                LOG.info(String.format("reading url : [%s]", url));

                InputStream inputStream = null;

                inputStream = new ByteArrayInputStream(client.get(url));

                UnstructuredStorageReaderUtil.readFromStream(inputStream, url, this.readerSliceConfig,
                        recordSender, this.getTaskPluginCollector());
                recordSender.flush();
            }
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
