import com.mongodb.MongoClientURI;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.InsertOneResult;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import com.vk.api.sdk.client.TransportClient;
import com.vk.api.sdk.client.VkApiClient;
import com.vk.api.sdk.client.actors.ServiceActor;
import com.vk.api.sdk.exceptions.ApiException;
import com.vk.api.sdk.exceptions.ClientException;
import com.vk.api.sdk.httpclient.HttpTransportClient;
import com.vk.api.sdk.objects.streaming.responses.GetServerUrlResponse;
import com.vk.api.sdk.streaming.clients.StreamingEventHandler;
import com.vk.api.sdk.streaming.clients.VkStreamingApiClient;
import com.vk.api.sdk.streaming.clients.actors.StreamingActor;
import com.vk.api.sdk.streaming.objects.StreamingCallbackMessage;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.bson.Document;

public class WebsoketPrinter {

    private final ConfigReader configReader = ConfigReader.getInstance();
    private final String exchangeName = configReader.getProp("amqpExchange");
    private final String routingKey = configReader.getProp("amqpRoute");
    private VkStreamingApiClient streamingClient;
    private StreamingActor streamingActor;  //был public
    private VkApiClient vkClient;

//    private static String tags = ", 1, 2, баррель, рубль, доллар, коронавирус, коронавирус, заболело, погибло, " +
//            "коронавирус, пандемия, эпидемия, болезнь, вирус, коронавирус, пандемия, эпидемия, болезнь, вирус, заражение, " +
//            "сидимдома, изоляция, самоизоляция, коронавирус, пандемия, эпидемия, болезнь, вирус, заражение, сидимдома, " +
//            "изоляция, самоизоляция, COVID, COVID-19, коронавирус, пандемия, эпидемия, болезнь, вирус, заражение, " +
//            "сидимдома, изоляция, самоизоляция, COVID, COVID-19, апокалипсис, короновирус, сидимдома, карантин, апрель]}}";

    WebsoketPrinter() {
        TransportClient transportClient = new HttpTransportClient();
        streamingClient = new VkStreamingApiClient(transportClient);
        vkClient = new VkApiClient(transportClient);
        streamingActor = new StreamingActor(this.getServerUrlResponse().getEndpoint(), this.getServerUrlResponse().getKey());
    }

    private GetServerUrlResponse getServerUrlResponse() {
        ConfigReader configReader = ConfigReader.getInstance();
        Integer appId = Integer.valueOf(configReader.getProp("appId"));
        String accessToken = configReader.getProp("accessToken");
        ServiceActor serviceActor = new ServiceActor(appId, accessToken);
        GetServerUrlResponse getServerUrlResponse = null;

        try {
            getServerUrlResponse = vkClient.streaming().getServerUrl(serviceActor).execute();
        } catch (ApiException | ClientException e) {
            e.printStackTrace();
        }
        return getServerUrlResponse;
    }

    public void print() {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setUsername(configReader.getProp("amqpUser"));
            factory.setPassword(configReader.getProp("amqpPassword"));
            factory.setVirtualHost("/");
            factory.setHost(configReader.getProp("amqpHost"));
            factory.setPort(Integer.parseInt(configReader.getProp("amqpPort")));
            Connection conn = factory.newConnection();
            Channel channel = conn.createChannel();

            streamingClient.stream().get(streamingActor, new StreamingEventHandler() {
                @Override
                public void handle(StreamingCallbackMessage message) {

                    try {
                        MongoClient mongoClient = new MongoClient(new MongoClientURI(configReader.getProp("mongoUrl")));
                        MongoDatabase database = mongoClient.getDatabase(configReader.getProp("mongoDatabase"));
                        MongoCollection<Document> coll = database.getCollection(configReader.getProp("mongoCollection"));

                        Document document = new Document("message", message.getEvent().getText());
                        InsertOneResult result = coll.insertOne(document);
                        String url = String.format(
                                "%s/%s/%s/%s",
                                configReader.getProp("mongoRestUrl"),
                                configReader.getProp("mongoDatabase"),
                                configReader.getProp("mongoCollection"),
                                document.get("_id")
                        );
                        System.out.println("Сообщение от стриминга получено");
                        mongoClient.close();

                        byte[] messageBodyBytes = (url).getBytes();
                        channel.basicPublish(exchangeName, routingKey, MessageProperties.PERSISTENT_TEXT_PLAIN, messageBodyBytes);
                    }
                    catch (Exception e){
                        e.printStackTrace();
                    }
                }
            }).execute();

        } catch (NoSuchMethodError | ExecutionException | InterruptedException | TimeoutException | IOException e) {
            e.printStackTrace();
        }
    }

}
