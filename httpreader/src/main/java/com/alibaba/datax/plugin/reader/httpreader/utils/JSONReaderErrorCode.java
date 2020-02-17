package com.alibaba.datax.plugin.reader.httpreader.utils;

import com.alibaba.datax.common.spi.ErrorCode;

public enum JSONReaderErrorCode implements ErrorCode {
    CONFIG_INVALID_EXCEPTION("JSONReaderErrorCode-00", "您的参数配置错误."),
    NOT_SUPPORT_TYPE("JSONReaderErrorCode-01","您配置的列类型暂不支持.");

    private final String code;
    private final String description;

    private JSONReaderErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s].", this.code,
                this.description);
    }
}
