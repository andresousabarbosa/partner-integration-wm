package pi.pulsar;

// -----( IS Java Code Template v1.2

import com.wm.data.*;
import com.wm.util.Values;
import com.wm.app.b2b.server.Service;
import com.wm.app.b2b.server.ServiceException;
// --- <<IS-START-IMPORTS>> ---
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
// --- <<IS-END-IMPORTS>> ---

public final class java

{
	// ---( internal utility methods )---

	final static java _instance = new java();

	static java _newInstance() { return new java(); }

	static java _cast(Object o) { return (java)o; }

	// ---( server methods )---




	public static final void consumeFromPulsar (IData pipeline)
        throws ServiceException
	{
		// --- <<IS-START(consumeFromPulsar)>> ---
		// @sigtype java 3.5
		// [i] field:0:required serviceUrl
		// [i] field:0:required topic
		// [i] field:0:required subscriptionName
		// [i] field:0:required maxMessages
		// [i] field:0:required timeoutMs
		// [o] field:0:required status
		// [o] field:0:required statusMessage
		// [o] field:0:required totalConsumed
		// [o] field:1:required messages
		IDataCursor c = pipeline.getCursor();
		    String serviceUrl       = IDataUtil.getString(c, "serviceUrl");
		    String topic            = IDataUtil.getString(c, "topic");
		    String subscriptionName = IDataUtil.getString(c, "subscriptionName");
		    String maxMessagesStr   = IDataUtil.getString(c, "maxMessages");
		    String timeoutMsStr     = IDataUtil.getString(c, "timeoutMs");
		    c.destroy();
		
		    if (serviceUrl       == null) serviceUrl       = "pulsar://localhost:6650";
		    if (topic            == null) topic            = "persistent://vli/integration/order-events";
		    if (subscriptionName == null) subscriptionName = "pi-partner-integration-sub";
		
		    PulsarClient client       = null;
		    Consumer<byte[]> consumer = null;
		    List<String> consumed     = new ArrayList<>();
		
		    try {
		        // Parsing dentro do try para capturar NumberFormatException
		        int maxMessages = (maxMessagesStr != null) ? Integer.parseInt(maxMessagesStr) : 10;
		        int timeoutMs   = (timeoutMsStr   != null) ? Integer.parseInt(timeoutMsStr)   : 3000;
		
		        client = PulsarClient.builder()
		            .serviceUrl(serviceUrl)
		            .operationTimeout(5, TimeUnit.SECONDS)
		            .connectionTimeout(5, TimeUnit.SECONDS)
		            .build();
		
		        consumer = client.newConsumer()
		            .topic(topic)
		            .subscriptionName(subscriptionName)
		            .subscriptionType(SubscriptionType.Shared)
		            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
		            .subscribe();
		
		        for (int i = 0; i < maxMessages; i++) {
		            Message<byte[]> msg = consumer.receive(timeoutMs, TimeUnit.MILLISECONDS);
		            if (msg == null) break;
		
		            String msgPayload = new String(msg.getData(), StandardCharsets.UTF_8);
		            consumed.add(msgPayload);
		            consumer.acknowledge(msg);
		            // Para reprocessar em vez de confirmar: consumer.negativeAcknowledge(msg);
		        }
		
		        String[] messages = consumed.toArray(new String[0]);
		
		        IDataCursor out = pipeline.getCursor();
		        IDataUtil.put(out, "status",        consumed.isEmpty() ? "NO_MESSAGE" : "SUCCESS");
		        IDataUtil.put(out, "statusMessage", consumed.isEmpty()
		            ? "PULSAR_NO_MESSAGE_AVAILABLE"
		            : "Messages consumed from Pulsar");
		        IDataUtil.put(out, "totalConsumed", String.valueOf(consumed.size()));
		        IDataUtil.put(out, "messages",      messages);
		        out.destroy();
		
		    } catch (Exception e) {
		        IDataCursor out = pipeline.getCursor();
		        IDataUtil.put(out, "status",        "FAILED");
		        IDataUtil.put(out, "statusMessage", "PULSAR_CONSUME_FAILED: " + e.getMessage());
		        IDataUtil.put(out, "totalConsumed", "0");
		        out.destroy();
		        // Sem throw \u2014 o wrapper pi.messaging:consumeMessage decide o tratamento de erro
		
		    } finally {
		        try { if (consumer != null) consumer.close(); } catch (Exception ignored) {}
		        try { if (client   != null) client.close();   } catch (Exception ignored) {}
		    }
		// --- <<IS-END>> ---

                
	}



	public static final void publishToPulsar (IData pipeline)
        throws ServiceException
	{
		// --- <<IS-START(publishToPulsar)>> ---
		// @sigtype java 3.5
		// [i] field:0:required serviceUrl
		// [i] field:0:required topic
		// [i] field:0:required payload
		// [i] field:0:required correlationId
		// [i] field:0:required messageType
		// [o] field:0:required status
		// [o] field:0:required statusMessage
		// [o] field:0:required messageId
		// [o] field:0:required topic
		// [o] field:0:required correlationId
		IDataCursor c = pipeline.getCursor();
		    String serviceUrl    = IDataUtil.getString(c, "serviceUrl");
		    String topic         = IDataUtil.getString(c, "topic");
		    String payload       = IDataUtil.getString(c, "payload");
		    String correlationId = IDataUtil.getString(c, "correlationId");
		    String messageType   = IDataUtil.getString(c, "messageType");
		    c.destroy();
		
		    if (serviceUrl    == null) serviceUrl    = "pulsar://localhost:6650";
		    if (topic         == null) topic         = "persistent://vli/integration/order-events";
		    if (correlationId == null) correlationId = "UNKNOWN";
		    if (messageType   == null) messageType   = "GENERIC";
		
		    PulsarClient client       = null;
		    Producer<byte[]> producer = null;
		
		    try {
		        client = PulsarClient.builder()
		            .serviceUrl(serviceUrl)
		            .operationTimeout(5, TimeUnit.SECONDS)
		            .connectionTimeout(5, TimeUnit.SECONDS)
		            .build();
		
		        producer = client.newProducer()
		            .topic(topic)
		            .create();
		            // Op\u00E7\u00E3o B \u2014 nome rastre\u00E1vel sem colis\u00E3o:
		            // .producerName("pi-producer-" + java.util.UUID.randomUUID().toString())
		
		        MessageId messageId = producer.newMessage()
		            .key(correlationId)
		            .property("correlationId", correlationId)
		            .property("messageType", messageType)
		            .value(payload != null
		                ? payload.getBytes(StandardCharsets.UTF_8)
		                : "{}".getBytes(StandardCharsets.UTF_8))
		            .send();
		
		        IDataCursor out = pipeline.getCursor();
		        IDataUtil.put(out, "status",        "SUCCESS");
		        IDataUtil.put(out, "statusMessage", "Message published to Pulsar");
		        IDataUtil.put(out, "messageId",     messageId.toString());
		        IDataUtil.put(out, "topic",         topic);
		        IDataUtil.put(out, "correlationId", correlationId);
		        out.destroy();
		
		    } catch (Exception e) {
		        IDataCursor out = pipeline.getCursor();
		        IDataUtil.put(out, "status",        "FAILED");
		        IDataUtil.put(out, "statusMessage", "PULSAR_PUBLISH_FAILED: " + e.getMessage());
		        IDataUtil.put(out, "correlationId", correlationId);
		        out.destroy();
		        // Sem throw \u2014 o wrapper pi.messaging:publishMessage decide o tratamento de erro
		
		    } finally {
		        try { if (producer != null) producer.close(); } catch (Exception ignored) {}
		        try { if (client   != null) client.close();   } catch (Exception ignored) {}
		    }
		// --- <<IS-END>> ---

                
	}
}

