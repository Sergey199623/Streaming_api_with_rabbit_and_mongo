import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import com.vk.api.sdk.client.TransportClient;
import com.vk.api.sdk.client.VkApiClient;
import com.vk.api.sdk.client.actors.ServiceActor;
import com.vk.api.sdk.exceptions.ApiException;
import com.vk.api.sdk.exceptions.ClientException;
import com.vk.api.sdk.httpclient.HttpTransportClient;
import com.vk.api.sdk.objects.streaming.responses.GetServerUrlResponse;
import com.vk.api.sdk.streaming.clients.VkStreamingApiClient;
import com.vk.api.sdk.streaming.clients.actors.StreamingActor;
import com.vk.api.sdk.streaming.exceptions.StreamingApiException;
import com.vk.api.sdk.streaming.exceptions.StreamingClientException;
import com.vk.api.sdk.streaming.objects.StreamingRule;
import com.vk.api.sdk.streaming.objects.responses.StreamingGetRulesResponse;
import com.vk.api.sdk.streaming.objects.responses.StreamingResponse;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

public class App {

    private static String tags;
    static String value = "ok";

    // Здесь используется выборка по тэгам из БД MySQL

    private static final ConfigReader configReader = ConfigReader.getInstance();
    private static final String url = configReader.getProp("mysqlUrl");
    private static final String user = configReader.getProp("mysqlUser");
    private static final String password = configReader.getProp("mysqlPassword");

    final static TransportClient transportClient = new HttpTransportClient();
    final static VkStreamingApiClient streamingClient = new VkStreamingApiClient(transportClient);
    final static VkApiClient vkClient = new VkApiClient(transportClient);
    static Integer appId = Integer.valueOf(configReader.getProp("appId"));
    static String accessToken = configReader.getProp("accessToken");
    final static ServiceActor serviceActor = new ServiceActor(appId, accessToken);


    private static Connection con;
    private static Statement stmt;
    private static ResultSet rs;

    // Таймер делает выборку по ключевым словам из БД MySQL раз в 15 минут

    public static void timer() {
        ScheduledExecutorService ses = Executors.newScheduledThreadPool(1);
        Runnable pinger = () -> {

            String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
            System.out.println("Timer started at " + timeStamp);

            List<String> lst = new ArrayList<>();
            lst.addAll(select(tags));

            GetServerUrlResponse getServerUrlResponse = null;
            try {
                getServerUrlResponse = vkClient.streaming().getServerUrl(serviceActor).execute();
            } catch (ApiException | ClientException e) {
                e.printStackTrace();
            }

            StreamingActor streamingActor = new StreamingActor(getServerUrlResponse.getEndpoint(), getServerUrlResponse.getKey());

            StreamingResponse response;
            StreamingGetRulesResponse responses;

            try {

                responses = streamingClient.rules().get(streamingActor).execute();

                for (StreamingRule rule : responses.getRules()) {
                    try {
                        String tag = rule.getTag();
                        streamingClient.rules().delete(streamingActor, rule.getValue()).execute();
                    } catch (Exception ex) {
                        Integer x = 2;
                    }

                }

            } catch (StreamingClientException | StreamingApiException e) {
                e.printStackTrace();
            }


            for (String tag : lst) {

                try {

                    System.out.println("В select(tag) сейчас лежит: " + tag);
                    System.out.println("В value сейчас лежит: " + value);

                    response = streamingClient.rules().add(streamingActor, tag, value).execute();

                } catch (StreamingClientException | StreamingApiException e) {
                    // e.printStackTrace();
                }

                WebsoketPrinter websoketPrinter = new WebsoketPrinter();
                websoketPrinter.print();
            }

        };
        ses.scheduleAtFixedRate(pinger, 5, 900, TimeUnit.SECONDS);
    }


    public static List<String> select(String result) {
        String query = "SELECT id, tags, status  FROM test.task";

        List<String> lst = new ArrayList<>();
        try {

            // Подключеник к MySQL серверу
            Class.forName("com.mysql.cj.jdbc.Driver");
            con = DriverManager.getConnection(url, user, password);

            stmt = con.createStatement();
            rs = stmt.executeQuery(query);
            rs.findColumn("status");
            while (rs.next()) {

                int id = rs.getInt("id");
                int status = rs.getInt("status"); // Считывание значения Integer из колонки "status"
                String tag = rs.getString("tags");

                switch (status) {
                    case 1:
                        lst.add(tag);
                        Statement s = con.createStatement();
                        s.execute("UPDATE task set status= 2 where id = " + id);
                        s.close();
                        break;
                    case 0:
                        lst.remove(tag);
                        break;
                    case 2:
                        lst.add(tag);
                        break;
                }
            }
        } catch (SQLException | ClassNotFoundException ex) {
            ex.printStackTrace();
        } finally {

            //close connection ,stmt and resultset here
            try {
                con.close();
            } catch (SQLException se) { /*can't do anything */ }
            try {
                stmt.close();
            } catch (SQLException se) { /*can't do anything */ }
            try {
                rs.close();
            } catch (SQLException se) { /*can't do anything */ }
        }

        System.out.println(result);
        return lst;
    }

    public static void main(String[] args) {
      timer();
    }

}
