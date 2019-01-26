package org.janusgraph.diskstorage.couchbase;

import org.apache.http.HttpHost;
import org.apache.http.NameValuePair;
import org.apache.http.StatusLine;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.AuthenticationException;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.TemporaryBackendException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BucketHelper {

    private CloseableHttpClient httpclient;
    private Credentials credentials;
    private String url;

    public BucketHelper(String host, int port, String username, String password) {
        final HttpHost httpHost = new HttpHost(host, port);
        final CredentialsProvider credsProvider = new BasicCredentialsProvider();
        credentials = new UsernamePasswordCredentials(username, password);
        credsProvider.setCredentials(
            new AuthScope(httpHost),
            credentials);

        httpclient = HttpClients.custom()
            .setDefaultCredentialsProvider(credsProvider)
            .build();
        url = httpHost.toURI() + "/pools/default/buckets";
    }

    public void close() throws IOException {
        httpclient.close();
    }

    public void create(String bucketName, String bucketType, int ramQuotaMB) throws BackendException {
        final HttpPost httpPost = new HttpPost(url);
        final List<NameValuePair> params = new ArrayList<>(3);

        params.add(new BasicNameValuePair("name", bucketName));
        params.add(new BasicNameValuePair("ramQuotaMB", String.valueOf(ramQuotaMB)));
        params.add(new BasicNameValuePair("bucketType", bucketType));

        try {
            httpPost.addHeader(new BasicScheme().authenticate(credentials, httpPost, null));
            httpPost.setEntity(new UrlEncodedFormEntity(params));

            try(final CloseableHttpResponse response = httpclient.execute(httpPost)) {
                final StatusLine statusLine = response.getStatusLine();

                if (statusLine.getStatusCode() == 202)
                    return;
                else
                    throw new IOException(statusLine.toString());
            }
        } catch (IOException e) {
            throw new TemporaryBackendException(e);
        } catch (AuthenticationException e) {
            throw new TemporaryBackendException(e);
        }
    }

    public boolean exists(String bucketName) throws BackendException {
        final HttpGet httpGet = new HttpGet(url + "/" + bucketName);

        try (final CloseableHttpResponse response = httpclient.execute(httpGet)) {
            final StatusLine statusLine = response.getStatusLine();

            if (statusLine.getStatusCode() == 200)
                return true;
            else if (statusLine.getStatusCode() == 404)
                return false;
            else
                throw new IOException(statusLine.toString());
        } catch (IOException e) {
            throw new TemporaryBackendException(e);
        }
    }

    public void drop(String bucketName) throws BackendException {
        final HttpDelete httpDelete = new HttpDelete(url + "/" + bucketName);

        try (final CloseableHttpResponse response = httpclient.execute(httpDelete)) {
            final StatusLine statusLine = response.getStatusLine();

            if (statusLine.getStatusCode() == 200 || statusLine.getStatusCode() == 404)
                return;
            else
                throw new IOException(statusLine.toString());
        } catch (IOException e) {
            throw new TemporaryBackendException(e);
        }
    }


}
