package com.alibaba.datax.plugin.unstructuredstorage.writer;

import java.io.IOException;
import java.io.Writer;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.csvreader.CsvWriter;

public class TextCsvWriterManager {
    public static UnstructuredWriter produceUnstructuredWriter(
            String fileFormat, char fieldDelimiter, char lineSeparrator, String filterString, Writer writer) {
        // warn: false means plain text(old way), true means strict csv format
        if (Constant.FILE_FORMAT_TEXT.equals(fileFormat)) {
            return new TextWriterImpl(writer, fieldDelimiter, lineSeparrator, filterString);
        } else {
            return new CsvWriterImpl(writer, fieldDelimiter, lineSeparrator, filterString);
        }
    }
}

class CsvWriterImpl implements UnstructuredWriter {
    private static final Logger LOG = LoggerFactory
            .getLogger(CsvWriterImpl.class);
    // csv 严格符合csv语法, 有标准的转义等处理
    private char lineSeparrator;
    private char fieldDelimiter;
    private String filterString;
    private CsvWriter csvWriter;

    public CsvWriterImpl(Writer writer, char fieldDelimiter, char lineSeparrator, String filterString) {
        this.fieldDelimiter = fieldDelimiter;
        this.lineSeparrator = lineSeparrator;
        this.filterString = filterString;
        this.csvWriter = new CsvWriter(writer, this.fieldDelimiter);
        this.csvWriter.setTextQualifier('"');
        this.csvWriter.setUseTextQualifier(true);
        // warn: in linux is \n , in windows is \r\n
        //this.csvWriter.setRecordDelimiter(IOUtils.LINE_SEPARATOR.charAt(0));
        this.csvWriter.setRecordDelimiter(lineSeparrator);
    }

    @Override
    public void writeOneRecord(List<String> splitedRows) throws IOException {
        if (splitedRows.isEmpty()) {
            LOG.info("Found one record line which is empty.");
        }
        if (!StringUtils.isEmpty(filterString)) {
            for (int i = 0; i < splitedRows.size(); i++) {
                String raw = splitedRows.get(i);
                splitedRows.set(i, raw.replace(filterString, ""));
            }
        }
        this.csvWriter.writeRecord((String[]) splitedRows
                .toArray(new String[0]));
    }

    @Override
    public void flush() throws IOException {
        this.csvWriter.flush();
    }

    @Override
    public void close() throws IOException {
        this.csvWriter.close();
    }

}

class TextWriterImpl implements UnstructuredWriter {
    private static final Logger LOG = LoggerFactory
            .getLogger(TextWriterImpl.class);
    // text StringUtils的join方式, 简单的字符串拼接
    private char lineSeparrator;
    private char fieldDelimiter;
    private String filterString;
    private Writer textWriter;

    public TextWriterImpl(Writer writer, char fieldDelimiter, char lineSeparrator, String filterString) {
        this.lineSeparrator = lineSeparrator;
        this.fieldDelimiter = fieldDelimiter;
        this.filterString = filterString;
        this.textWriter = writer;
    }

    @Override
    public void writeOneRecord(List<String> splitedRows) throws IOException {
        if (splitedRows.isEmpty()) {
            LOG.info("Found one record line which is empty.");
        }
//        this.textWriter.write(String.format("%s%s",
//                StringUtils.join(splitedRows, this.fieldDelimiter),
//                IOUtils.LINE_SEPARATOR));


        if (!StringUtils.isEmpty(filterString)) {
            for (int i = 0; i < splitedRows.size(); i++) {
                String raw = splitedRows.get(i);
                splitedRows.set(i, raw.replace(filterString, ""));
            }
        }

        this.textWriter.write(String.format("%s%s",
                StringUtils.join(splitedRows, this.fieldDelimiter),
                this.lineSeparrator));
    }

    @Override
    public void flush() throws IOException {
        this.textWriter.flush();
    }

    @Override
    public void close() throws IOException {
        this.textWriter.close();
    }

}
