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
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

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

    public static void readFromUrl(String url, String table, String listNode, String primaryKey,
                                   Configuration readerSliceConfig, RecordSender recordSender) {
        WebClient client = new WebClient();
        JSONObject json = null;
        JSONArray jarr = null;
        List<ColumnEntry> columns = null;
        String[] nodes = listNode.split("/");
        String content = null;

        content = client.getString(url);
        if (nodes.length == 0 || StringUtils.isEmpty(listNode)) {
            jarr = JSON.parseArray(content);
        } else {
            json = JSON.parseObject(content);
            for(int i=0; i<nodes.length-1; i++)
                json = json.getJSONObject(nodes[i]);

            jarr = json.getJSONArray(nodes[nodes.length - 1]);
        }

        columns = getListColumnEntry(readerSliceConfig, Key.COLUMN);

        for (int i = 0; i < jarr.size(); i++) {
            if ("table_1".equals(table)) {
                transportOneRecord(recordSender, columns, jarr.getJSONObject(i));
            } else {
                json = jarr.getJSONObject(i);
                List<String> keys = json.keySet().stream().sorted(Comparator.naturalOrder()).collect(Collectors.toList());
                int count = 1;
                String curTable = null;

                for(String key:keys) {
                    String val = json.getString(key);
                    JSONArray childrens = null;

                    if (!org.apache.commons.lang.StringUtils.isEmpty(val)) {
                        if (val.charAt(0) == '{') {
                            JSONObject children = json.getJSONObject(key);// JSON.parseObject(val);

                            childrens = new JSONArray();
                            childrens.add(children);
                        } else if (val.charAt(0) == '[') {
                            JSONArray jsonArray = json.getJSONArray(key);// JSON.parseArray(val);
                            if (jsonArray.size() != 0)
                                childrens = jsonArray;
                        }
                    }

                    if (childrens != null && childrens.size() != 0) {
                        curTable = "table_" + (++count);
                        if (curTable.equals(table)) {
                            for(int j=0; j<childrens.size(); j++) {
                                JSONObject children = childrens.getJSONObject(j);

                                if (!StringUtils.isEmpty(primaryKey)) {
                                    children.put(primaryKey + "_parent_id", json.getString(primaryKey));
                                }
                                transportOneRecord(recordSender, columns, children);
                            }

                        }
                    }
                }
            }
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
