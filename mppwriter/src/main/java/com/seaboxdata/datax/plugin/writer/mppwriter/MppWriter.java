package com.seaboxdata.datax.plugin.writer.mppwriter;

import cn.hutool.core.io.FileUtil;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.alibaba.datax.plugin.unstructuredstorage.writer.UnstructuredStorageWriterUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;

public class MppWriter extends Writer {
    private static final DataBaseType DATABASE_TYPE = DataBaseType.RDBMS;

    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originalConfig = null;
        private CommonRdbmsWriter.Job commonRdbmsWriterMaster;

        private String scfdistPath = null;
        private String scfdistURL = null;
        private List<String> columns = null;
        List<JSONObject> connections = null;
        protected String username = null;
        protected String password = null;


        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();

            String writeMode = this.originalConfig.getString(Key.WRITE_MODE);
            if (null != writeMode) {
                throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,
                        String.format("写入模式(writeMode)配置有误. 因为RDBMS不支持配置参数项 writeMode: %s, RDBMS仅使用insert sql 插入数据. 请检查您的配置并作出修改.", writeMode));
            }

            this.commonRdbmsWriterMaster = new CommonRdbmsWriter.Job(DATABASE_TYPE);
            this.commonRdbmsWriterMaster.init(this.originalConfig);

            this.scfdistPath = this.originalConfig.getString("scfdistPATH");
            this.scfdistURL = this.originalConfig.getString("scfdistURL");
            this.columns = this.originalConfig.getList(Key.COLUMN, String.class);

            this.username = this.originalConfig.getString(Key.USERNAME);
            this.password = this.originalConfig.getString(Key.PASSWORD);
            this.connections = (List) this.originalConfig.getList("connection");
        }

        @Override
        public void prepare() {
            this.commonRdbmsWriterMaster.prepare(this.originalConfig);
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> configurationList = this.commonRdbmsWriterMaster.split(this.originalConfig, mandatoryNumber);

            for (Configuration config : configurationList) {
                config.set("scfdistURL", originalConfig.getString("scfdistURL"));
                config.set("scfdistPATH", originalConfig.getString("scfdistPATH"));
                config.set("fieldDelimiter", "|");
                config.set("encoding", "utf-8");
                config.set("fileFormat", "csv");

                String fileFullPath = UUID.randomUUID().toString().replace('-', '_');
                config.set("filename", fileFullPath);
            }
            return configurationList;
        }

        @Override
        public void post() {
            for (JSONObject con : connections) {
                JSONArray tables = con.getJSONArray("table");
                for (int i = 0; i < tables.size(); i++) {
                    String table = tables.getString(i);
                    String extTable = table + "_ext";
                    String jdbcUrl = con.getString("jdbcUrl");

                    this.fixCSV(this.scfdistPath, table, extTable);
                    this.external2Table(table, extTable, jdbcUrl);
                    this.clearFile(this.scfdistPath, table, extTable);
                }
            }
            this.commonRdbmsWriterMaster.post(this.originalConfig);


        }

        private void fixCSV(String fileFullPath, String table, String extTable) {
            File path = new File(Paths.get(fileFullPath, table, "temp").toString());
            File[] files = path.listFiles();
            String line = null;
            InputStream inputStream = null;
            BufferedReader reader = null;
            StringBuilder sb = null;

            Log.info("fix {} csv", files.length);
            for (int i = 0; i < files.length; i++) {
                File file = files[i];
                sb = new StringBuilder(40960);
                try {
                    LOG.info(String.format("read to file : [%s]", file.toString()));
                    inputStream = new FileInputStream(file);
                    reader = new BufferedReader(new InputStreamReader(inputStream,
                            "utf-8"), 1024);
                    while ((line = reader.readLine()) != null) {
                        sb.append(line);
                        sb.append("\n");
                    }
                } catch (Exception ex) {
                    LOG.error("fix csv error", ex);
                } finally {
                    IOUtils.closeQuietly(reader);
                }
                file.delete();

                if (i == files.length)
                    sb.deleteCharAt(sb.length() - 1);

                BufferedWriter writer = null;
                FileOutputStream outputStream = null;
                LOG.info(String.format("write to file : [%s]", Paths.get(fileFullPath, extTable).toString()));
                try {
                    outputStream = new FileOutputStream(Paths.get(fileFullPath, extTable).toString(), true);
                    writer = new BufferedWriter(new OutputStreamWriter(outputStream,
                            "utf-8"), 1024);
                    writer.write(sb.toString());
                } catch (Exception ex) {
                    LOG.error("fix csv error", ex);
                } finally {
                    IOUtils.closeQuietly(writer);
                }
            }
        }

        private void external2Table(String table, String extTable, String jdbcUrl) {
            // 创建外部表并插入到本地表中
            StringBuilder createTableSQL = new StringBuilder();
            createTableSQL.append("create external table ");
            createTableSQL.append(table);
            createTableSQL.append("_ext (");
            for (String column : columns) {
                createTableSQL.append(column);
                createTableSQL.append(" text,");
            }
            createTableSQL.deleteCharAt(createTableSQL.length() - 1);
            createTableSQL.append(") location ( '");
            createTableSQL.append(this.scfdistURL);
            createTableSQL.append("/");
            createTableSQL.append(extTable);
            createTableSQL.append("') Format 'TEXT' (delimiter as '|' null as '' escape '\\')");

            LOG.info("建表语句:{}", createTableSQL.toString());
            String insertSQL = "INSERT INTO " + table + " SELECT * from " + extTable;
            String deleteExtTableSQL = "drop external table if exists " + table + "_ext";

            Connection connection = DBUtil.getConnection(DataBaseType.RDBMS,
                    jdbcUrl, username, password);
            DBUtil.dealWithSessionConfig(connection, originalConfig,
                    DataBaseType.RDBMS, "INFO");
            Statement statement = null;
            try {
                connection.setAutoCommit(false);
                statement = connection.createStatement();

                statement.addBatch(deleteExtTableSQL);
                statement.addBatch(createTableSQL.toString());
                statement.addBatch(insertSQL);
                statement.addBatch(deleteExtTableSQL);

                statement.executeBatch();
                connection.commit();
            } catch (SQLException e) {
                throw DataXException.asDataXException(
                        DBUtilErrorCode.WRITE_DATA_ERROR, e);
            } finally {
                DBUtil.closeDBResources(statement, null);
            }
        }

        private void clearFile(String fileFullPath, String table, String extTable) {
            FileUtil.del(Paths.get(fileFullPath, table).toString());

            File file = new File(Paths.get(fileFullPath, extTable).toString());
            if (file.exists())
                file.delete();
        }

        @Override
        public void destroy() {
            this.commonRdbmsWriterMaster.destroy(this.originalConfig);
        }

    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private String scfdistPath;
        private String filename;
        protected String username;
        protected String password;
        protected String jdbcUrl;
        protected String table;
        protected String extTable;
        protected List<String> columns;

        private Configuration writerSliceConfig;
        private CommonRdbmsWriter.Task commonRdbmsWriterSlave;

        @Override
        public void init() {
            this.writerSliceConfig = super.getPluginJobConf();
            this.commonRdbmsWriterSlave = new CommonRdbmsWriter.Task(DATABASE_TYPE) {
                @Override
                public String calcValueHolder(String columnType) {
                    if ("serial".equalsIgnoreCase(columnType)) {
                        return "?::int";
                    } else if ("bit".equalsIgnoreCase(columnType)) {
                        return "?::bit varying";
                    }
                    return "?::" + columnType;
                }
            };
            this.commonRdbmsWriterSlave.init(this.writerSliceConfig);

            this.scfdistPath = this.writerSliceConfig.getString("scfdistPATH");
            this.filename = this.writerSliceConfig.getString("filename");
            this.table = this.writerSliceConfig.getString(Key.TABLE);
            this.columns = writerSliceConfig.getList(Key.COLUMN, String.class);
            this.extTable = this.table + "_ext";

            this.username = writerSliceConfig.getString(Key.USERNAME);
            this.password = writerSliceConfig.getString(Key.PASSWORD);
            this.jdbcUrl = writerSliceConfig.getString(Key.JDBC_URL);
        }

        @Override
        public void prepare() {
            this.commonRdbmsWriterSlave.prepare(this.writerSliceConfig);
        }

        public void startWrite(RecordReceiver recordReceiver) {
            LOG.info("begin do write...");
            String fileFullPath = this.buildFilePath();
            LOG.info(String.format("write to file : [%s]", fileFullPath));

            OutputStream outputStream = null;
            try {
                File newFile = new File(fileFullPath);
                newFile.createNewFile();
                outputStream = new FileOutputStream(newFile);
                UnstructuredStorageWriterUtil.writeToStream(recordReceiver,
                        outputStream, this.writerSliceConfig, this.extTable,
                        this.getTaskPluginCollector());
            } catch (SecurityException se) {
                throw DataXException.asDataXException(
                        TxtFileWriterErrorCode.SECURITY_NOT_ENOUGH,
                        String.format("您没有权限创建文件  : [%s]", this.extTable));
            } catch (IOException ioe) {
                throw DataXException.asDataXException(
                        TxtFileWriterErrorCode.Write_FILE_IO_ERROR,
                        String.format("无法创建待写文件 : [%s]", this.extTable), ioe);
            } finally {
                IOUtils.closeQuietly(outputStream);
            }
            LOG.info("end do write");
        }

        private String buildFilePath() {
            String filepath = Paths.get(this.scfdistPath, this.table, "temp", this.filename).toString();
            String tmp = Paths.get(this.scfdistPath, this.table, "temp").toString();

            File file = new File(tmp);
            if (!file.exists()) {
                file.mkdirs();
            }
            return filepath;
        }

        @Override
        public void post() {
            this.commonRdbmsWriterSlave.post(this.writerSliceConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsWriterSlave.destroy(this.writerSliceConfig);
        }

    }

}
