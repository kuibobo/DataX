package com.alibaba.datax.plugin.reader.httpreader.utils;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.UnsupportedEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WebClient {

    protected Logger logger = LoggerFactory.getLogger(getClass());
    private Map<String, String> heads = new HashMap() {{
        put("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36");
    }};

    private String charset = "utf-8";

    public static final RequestConfig defaultRequestConfig = RequestConfig.custom().setSocketTimeout(5000).setConnectTimeout(5000)
            .setConnectionRequestTimeout(5000).build();

    private static CloseableHttpClient httpclient = null;

    static {
        try {
            SSLContext ctx = SSLContext.getInstance("TLS");
            X509TrustManager tm = new X509TrustManager() {
                public X509Certificate[] getAcceptedIssuers() {
                    return null;
                }

                public void checkClientTrusted(X509Certificate[] arg0,
                                               String arg1) throws CertificateException {
                }

                public void checkServerTrusted(X509Certificate[] arg0,
                                               String arg1) throws CertificateException {
                }
            };
            ctx.init(null, new TrustManager[] { tm }, null);
            SSLConnectionSocketFactory ssf = new SSLConnectionSocketFactory(
                    ctx, NoopHostnameVerifier.INSTANCE);
            httpclient = HttpClients.custom()
                    .setSSLSocketFactory(ssf).build();;
        } catch (Exception e) {
            httpclient = HttpClients.createDefault();
        }
    }

    public String getCharset() {
        return charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    public void addHead(String key, String value) {
        heads.put(key, value);
    }

    public byte[] get(String url) {
        byte[] data = null;
        HttpGet request = new HttpGet(url);
        heads.forEach((k, v) -> {
            if (!StringUtils.isEmpty(v)) {
                request.addHeader(k, v);
            }
        });

        try (CloseableHttpResponse response = httpclient.execute(request)) {
            HttpEntity entity = response.getEntity();
            if (null != entity) {
                data = EntityUtils.toByteArray(entity);
                EntityUtils.consume(entity);
            }
        } catch (Exception ex) {
            logger.error("get error", ex);
            new RuntimeException(ex);
        }
        return data;
    }

    public String getString(String url) {
        String data = null;
        HttpGet request = new HttpGet(url);
        heads.forEach((k, v) -> {
            if (!StringUtils.isEmpty(v)) {
                request.addHeader(k, v);
            }
        });

        try (CloseableHttpResponse response = httpclient.execute(request)) {
            HttpEntity entity = response.getEntity();
            if (null != entity) {
                data = EntityUtils.toString(entity, charset);
                EntityUtils.consume(entity);
            }
        } catch (Exception ex) {
            logger.error("get error", ex);
            new RuntimeException(ex);
        }
        return data;
    }

    public JSONObject getJSONObject(String url) {
        String content = getString(url);

        return JSONObject.parseObject(content);
    }

    public Map<String, String> getMap(String url) {
        String content = getString(url);
        return MapUtil.xmlToMap(content);
    }

    public byte[] post(String url, Map<String, String> params) {
        byte[] data = null;
        HttpPost request = new HttpPost(url);
        List<NameValuePair> nvps = new ArrayList<>();

        params.forEach((k, v) -> {
            if (!StringUtils.isEmpty(v)) {
                nvps.add(new BasicNameValuePair(k, v));
            }
        });
        heads.forEach((k, v) -> {
            if (!StringUtils.isEmpty(v)) {
                request.addHeader(k, v);
            }
        });

        try {
            request.setEntity(new UrlEncodedFormEntity(nvps, charset));
            CloseableHttpResponse response = httpclient.execute(request);
            HttpEntity entity = response.getEntity();
            if (null != entity) {
                data = EntityUtils.toByteArray(entity);
                EntityUtils.consume(entity);
            }
        } catch (Exception ex) {
            logger.error("post error", ex);
            new RuntimeException(ex);
        }

        return data;
    }

    public byte[] post(String url, String content) {
        byte[] data = null;
        HttpPost request = new HttpPost(url);
        heads.forEach((k, v) -> {
            if (!StringUtils.isEmpty(v)) {
                request.addHeader(k, v);
            }
        });

        try {
            StringEntity s = new StringEntity(content, "utf-8");
            s.setContentType("application/json");//发送json数据需要设置contentType
            request.setEntity(s);
            CloseableHttpResponse response = httpclient.execute(request);
            HttpEntity entity = response.getEntity();
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                data = EntityUtils.toByteArray(entity);
                EntityUtils.consume(entity);
            }
        } catch (Exception e) {
            logger.error("post error", e);
            new RuntimeException(e);
        }
        return data;
    }

    public String post2(String url, Map<String, String> params) {
        byte[] bytes = post(url, params);

        try {
            return new String(bytes, charset);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public String post2(String url, String content) {
        byte[] bytes = post(url, content);
        try {
            return new String(bytes, charset);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public JSONObject post3(String url, Map<String, String> params) {
        String content = post2(url, params);

        return JSONObject.parseObject(content);
    }

    public JSONObject post3(String url, String content) {
        String result = post2(url, content);

        return JSONObject.parseObject(result);
    }
}
