package com.alibaba.datax.plugin.writer.hdfsfilewriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public  class HdfsHelper {
    public static final Logger LOG = LoggerFactory.getLogger(HdfsFileWriter.Job.class);
    public FileSystem fileSystem = null;
    public JobConf conf = null;
    public org.apache.hadoop.conf.Configuration hadoopConf = null;
    public static final String HADOOP_SECURITY_AUTHENTICATION_KEY = "hadoop.security.authentication";
    public static final String HDFS_DEFAULTFS_KEY = "fs.defaultFS";

    // Kerberos
    private Boolean haveKerberos = false;
    private String  kerberosKeytabFilePath;
    private String  kerberosPrincipal;

    public void getFileSystem(String defaultFS, Configuration taskConfig){
        hadoopConf = new org.apache.hadoop.conf.Configuration();

        Configuration hadoopSiteParams = taskConfig.getConfiguration(Key.HADOOP_CONFIG);
        JSONObject hadoopSiteParamsAsJsonObject = JSON.parseObject(taskConfig.getString(Key.HADOOP_CONFIG));
        if (null != hadoopSiteParams) {
            Set<String> paramKeys = hadoopSiteParams.getKeys();
            for (String each : paramKeys) {
                hadoopConf.set(each, hadoopSiteParamsAsJsonObject.getString(each));
            }
        }
        hadoopConf.set(HDFS_DEFAULTFS_KEY, defaultFS);

        //是否有Kerberos认证
        this.haveKerberos = taskConfig.getBool(Key.HAVE_KERBEROS, false);
        if(haveKerberos){
            this.kerberosKeytabFilePath = taskConfig.getString(Key.KERBEROS_KEYTAB_FILE_PATH);
            this.kerberosPrincipal = taskConfig.getString(Key.KERBEROS_PRINCIPAL);
            hadoopConf.set(HADOOP_SECURITY_AUTHENTICATION_KEY, "kerberos");
        }
        this.kerberosAuthentication(this.kerberosPrincipal, this.kerberosKeytabFilePath);
        conf = new JobConf(hadoopConf);
        try {
            fileSystem = FileSystem.get(conf);
        } catch (IOException e) {
            String message = String.format("获取FileSystem时发生网络IO异常,请检查您的网络是否正常!HDFS地址：[%s]",
                    "message:defaultFS =" + defaultFS);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }catch (Exception e) {
            String message = String.format("获取FileSystem失败,请检查HDFS地址是否正确: [%s]",
                    "message:defaultFS =" + defaultFS);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }

        if(null == fileSystem || null == conf){
            String message = String.format("获取FileSystem失败,请检查HDFS地址是否正确: [%s]",
                    "message:defaultFS =" + defaultFS);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, message);
        }
    }

    private void kerberosAuthentication(String kerberosPrincipal, String kerberosKeytabFilePath){
        if(haveKerberos && StringUtils.isNotBlank(this.kerberosPrincipal) && StringUtils.isNotBlank(this.kerberosKeytabFilePath)){
            UserGroupInformation.setConfiguration(this.hadoopConf);
            try {
                UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytabFilePath);
            } catch (Exception e) {
                String message = String.format("kerberos认证失败,请确定kerberosKeytabFilePath[%s]和kerberosPrincipal[%s]填写正确",
                        kerberosKeytabFilePath, kerberosPrincipal);
                LOG.error(message);
                throw DataXException.asDataXException(HdfsWriterErrorCode.KERBEROS_LOGIN_ERROR, e);
            }
        }
    }

    /**
     *获取指定目录先的文件列表
     * @param dir
     * @return
     * 拿到的是文件全路径，
     * eg：hdfs://10.101.204.12:9000/user/hive/warehouse/writer.db/text/test.textfile
     */
    public String[] hdfsDirList(String dir){
        Path path = new Path(dir);
        String[] files = null;
        try {
            FileStatus[] status = fileSystem.listStatus(path);
            files = new String[status.length];
            for(int i=0;i<status.length;i++){
                files[i] = status[i].getPath().toString();
            }
        } catch (IOException e) {
            String message = String.format("获取目录[%s]文件列表时发生网络IO异常,请检查您的网络是否正常！", dir);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
        return files;
    }

    public boolean isPathexists(String filePath) {
        Path path = new Path(filePath);
        boolean exist = false;
        try {
            exist = fileSystem.exists(path);
        } catch (IOException e) {
            String message = String.format("判断文件路径[%s]是否存在时发生网络IO异常,请检查您的网络是否正常！",
                    "message:filePath =" + filePath);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
        return exist;
    }

    public boolean isPathDir(String filePath) {
        Path path = new Path(filePath);
        boolean isDir = false;
        try {
            isDir = fileSystem.isDirectory(path);
        } catch (IOException e) {
            String message = String.format("判断路径[%s]是否是目录时发生网络IO异常,请检查您的网络是否正常！", filePath);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
        return isDir;
    }

    public void deleteDir(Path path){
        LOG.info(String.format("start delete tmp dir [%s] .",path.toString()));
        try {
            if(isPathexists(path.toString())) {
                fileSystem.delete(path, true);
            }
        } catch (Exception e) {
            String message = String.format("删除临时目录[%s]时发生IO异常,请检查您的网络是否正常！", path.toString());
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
        LOG.info(String.format("finish delete tmp dir [%s] .",path.toString()));
    }

    public void renameFile(HashSet<String> tmpFiles, HashSet<String> endFiles){
        Path tmpFilesParent = null;
        if(tmpFiles.size() != endFiles.size()){
            String message = String.format("临时目录下文件名个数与目标文件名个数不一致!");
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.HDFS_RENAME_FILE_ERROR, message);
        }else{
            try{
                for (Iterator it1=tmpFiles.iterator(),it2=endFiles.iterator();it1.hasNext()&&it2.hasNext();){
                    String srcFile = it1.next().toString();
                    String dstFile = it2.next().toString();
                    Path srcFilePah = new Path(srcFile);
                    Path dstFilePah = new Path(dstFile);
                    if(tmpFilesParent == null){
                        tmpFilesParent = srcFilePah.getParent();
                    }
                    LOG.info(String.format("start rename file [%s] to file [%s].", srcFile,dstFile));
                    boolean renameTag = false;
                    long fileLen = fileSystem.getFileStatus(srcFilePah).getLen();
                    if(fileLen>0){
                        renameTag = fileSystem.rename(srcFilePah,dstFilePah);
                        if(!renameTag){
                            String message = String.format("重命名文件[%s]失败,请检查您的网络是否正常！", srcFile);
                            LOG.error(message);
                            throw DataXException.asDataXException(HdfsWriterErrorCode.HDFS_RENAME_FILE_ERROR, message);
                        }
                        LOG.info(String.format("finish rename file [%s] to file [%s].", srcFile,dstFile));
                    }else{
                        LOG.info(String.format("文件［%s］内容为空,请检查写入是否正常！", srcFile));
                    }
                }
            }catch (Exception e) {
                String message = String.format("重命名文件时发生异常,请检查您的网络是否正常！");
                LOG.error(message);
                throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
            }finally {
                deleteDir(tmpFilesParent);
            }
        }
    }

    //关闭FileSystem
    public void closeFileSystem(){
        try {
            fileSystem.close();
        } catch (IOException e) {
            String message = String.format("关闭FileSystem时发生IO异常,请检查您的网络是否正常！");
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
    }


    //textfile格式文件
    public  FSDataOutputStream getOutputStream(String path){
        Path storePath = new Path(path);
        FSDataOutputStream fSDataOutputStream = null;
        try {
            fSDataOutputStream = fileSystem.create(storePath);
        } catch (IOException e) {
            String message = String.format("Create an FSDataOutputStream at the indicated Path[%s] failed: [%s]",
                    "message:path =" + path);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.Write_FILE_IO_ERROR, e);
        }
        return fSDataOutputStream;
    }

    public void fileStartWrite(RecordReceiver lineReceiver, Configuration config, String fileName,
                               TaskPluginCollector taskPluginCollector) {
        Record record = null;

        while ((record = lineReceiver.getFromReader()) != null) {
            Column columnGenerated = record.getColumn(0);

            try (FSDataOutputStream out = this.getOutputStream(fileName)){
                out.write((byte[]) columnGenerated.getRawData());
            } catch (Exception e) {
                String message = String.format("写文件文件[%s]时发生IO异常,请检查您的网络是否正常！", fileName);
                LOG.error(message);
                Path path = new Path(fileName);
                deleteDir(path.getParent());
                throw DataXException.asDataXException(HdfsWriterErrorCode.Write_FILE_IO_ERROR, e);
            }
        }
    }
}
