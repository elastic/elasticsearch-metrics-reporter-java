package de.spinscale.logfile;

import org.elasticsearch.metrics.JsonMetrics;
import org.elasticsearch.metrics.percolation.Notifier;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

public class HttpNotifier implements Notifier {

    @Override
    public void notify(JsonMetrics.JsonMetric jsonMetric, String percolateMatcher) {
        System.out.println("Sending notification for metric " + jsonMetric.name());

        try {
            URL url = new URL("http://localhost:3000/notify");
            HttpURLConnection connection = getUrlConnection(url);
            StringBuilder sb = new StringBuilder("metricName=");
            sb.append(jsonMetric.name());
            sb.append("&percolatorId=");
            sb.append(percolateMatcher);
            connection.getOutputStream().write(sb.toString().getBytes());

            if (connection.getResponseCode() != 200) {
                System.out.println("Got broken response code " + connection.getResponseCode() +
                        " for metric " + jsonMetric.name());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private HttpURLConnection getUrlConnection(URL url) throws IOException {
        HttpURLConnection connection = ( HttpURLConnection ) url.openConnection();
        connection.setRequestMethod("POST");
        connection.setUseCaches(false);
        connection.setDoOutput(true);
        connection.connect();
        return connection;
    }
}
