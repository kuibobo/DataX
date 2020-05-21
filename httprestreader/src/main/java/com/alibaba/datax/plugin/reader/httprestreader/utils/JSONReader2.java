package com.alibaba.datax.plugin.reader.httprestreader.utils;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.httprestreader.Key;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class JSONReader2 {

    private static final Logger LOG = LoggerFactory
            .getLogger(JSONReader2.class);

    private enum Type {
        STRING, INT, LONG, BOOLEAN, DOUBLE, DATE, BIGINT, DECIMAL
    }

    public static List<ColumnEntry> getListColumnEntry(
            Configuration configuration, final String path) {
        List<JSONObject> lists = configuration.getList(path, JSONObject.class);
        if (lists == null) {
            return null;
        }
        List<ColumnEntry> result = new ArrayList();
        for (JSONObject object : lists) {
            result.add(JSON.parseObject(object.toJSONString(),
                    ColumnEntry.class));
        }
        return result;
    }

    public static void readFromUrl2(String url, String listNode,
                                   Configuration readerSliceConfig, RecordSender recordSender) {
        WebClient client = new WebClient();
        JSONObject json = null;
        JSONArray jarr = null;
        List<ColumnEntry> columns = null;

        json = client.getJSONObject(url);
        jarr = json.getJSONArray(listNode);
        columns = getListColumnEntry(readerSliceConfig, Key.COLUMN);

        for (int i = 0; i < jarr.size(); i++) {
            transportOneRecord(recordSender, columns, jarr.getJSONObject(i));
        }

    }

    public static void transportOneRecord(RecordSender recordSender, List<ColumnEntry> columns,
                                          JSONObject json) {
        Record record = recordSender.createRecord();

        for (ColumnEntry columnConfig : columns) {
            Column columnGenerated = null;
            String columnValue = json.getString(columnConfig.getName());
            String columnType = columnConfig.getType().toUpperCase();

            switch (Type.valueOf(columnType)) {
                case STRING:
                    columnGenerated = new StringColumn(columnValue);
                    break;
                case INT:
                case BIGINT:
                case LONG:
                    try {
                        columnGenerated = new LongColumn(columnValue);
                    } catch (Exception e) {
                        throw new IllegalArgumentException(String.format(
                                "类型转换错误, 无法将[%s] 转换为[%s]", columnValue,
                                "LONG"));
                    }
                    break;
                case DECIMAL:
                case DOUBLE:
                    try {
                        columnGenerated = new DoubleColumn(columnValue);
                    } catch (Exception e) {
                        throw new IllegalArgumentException(String.format(
                                "类型转换错误, 无法将[%s] 转换为[%s]", columnValue,
                                "DOUBLE"));
                    }
                    break;
                case BOOLEAN:
                    try {
                        columnGenerated = new BoolColumn(columnValue);
                    } catch (Exception e) {
                        throw new IllegalArgumentException(String.format(
                                "类型转换错误, 无法将[%s] 转换为[%s]", columnValue,
                                "BOOLEAN"));
                    }

                    break;
                case DATE:
                    try {
                        if (columnValue == null) {
                            Date date = null;
                            columnGenerated = new DateColumn(date);
                        } else {
                            String formatString = columnConfig.getFormat();
                            //if (null != formatString) {
                            if (StringUtils.isNotBlank(formatString)) {
                                // 用户自己配置的格式转换, 脏数据行为出现变化
                                DateFormat format = columnConfig
                                        .getDateFormat();
                                columnGenerated = new DateColumn(
                                        format.parse(columnValue));
                            } else {
                                // 框架尝试转换
                                columnGenerated = new DateColumn(
                                        new StringColumn(columnValue)
                                                .asDate());
                            }
                        }
                    } catch (Exception e) {
                        throw new IllegalArgumentException(String.format(
                                "类型转换错误, 无法将[%s] 转换为[%s]", columnValue,
                                "DATE"));
                    }
                    break;
                default:
                    String errorMessage = String.format(
                            "您配置的列类型暂不支持 : [%s]", columnType);
                    LOG.error(errorMessage);
                    throw DataXException
                            .asDataXException(
                                    JSONReaderErrorCode.NOT_SUPPORT_TYPE,
                                    errorMessage);
            }

            record.addColumn(columnGenerated);
        }

        recordSender.sendToWriter(record);
    }
}
