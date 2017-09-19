package uk.gov.justice;

import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;
import static javax.management.MBeanServerInvocationHandler.newProxyInstance;
import static org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration.getDefaultJmxDomain;
import static org.apache.activemq.artemis.api.jms.ActiveMQJMSClient.createQueue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;
import org.apache.activemq.artemis.api.jms.management.JMSQueueControl;
import org.apache.activemq.artemis.jms.client.ActiveMQQueueConnectionFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

//to run this test from IDE start artemis first by executing ./target/server0/bin/artemis run
public class JmxArtemisQueueBrowserIT {

    private static final ActiveMQQueueConnectionFactory JMS_CF = new ActiveMQQueueConnectionFactory("tcp://localhost:61616");
    private static QueueConnection JMS_CONNECTION;
    private static QueueSession JMS_SESSION;
    private static Map<String, Queue> QUEUES = new HashMap<>();
    private static Map<String, MessageConsumer> CONSUMERS = new HashMap<>();
    private static Map<String, MessageProducer> PRODUCERS = new HashMap<>();

    private static final String JMX_URL = "service:jmx:rmi:///jndi/rmi://%s:%s/jmxrmi";

    @BeforeClass
    public static void beforeClass() throws JMSException {
        openJmsConnection();
    }

    @AfterClass
    public static void afterClass() throws JMSException {
        closeJmsConnection();
    }

    @Test
    public void shouldReturnMessagesFromQueue() throws Exception {
        final String queue = "DLQ";

        cleanQueue(queue);

        putInQueue(queue, "{\"key1\":\"value123\"}");
        putInQueue(queue, "{\"key1\":\"valueBB\"}");

        final List<String> messageData = messagesOf("localhost", "3000", "0.0.0.0", queue);
        assertThat(messageData.size(), is(2));
    }

    @Test
    public void shouldReturnLargeMessageFromQueue() throws Exception {
        final String queue = "DLQ";

        cleanQueue(queue);

        putInQueue(queue, createLargeMessage(1687L));

        try {
            final List<String> messageData = messagesOf("localhost", "3000", "0.0.0.0", queue);

            assertThat(messageData, is(1));
        } catch (final NullPointerException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    private List<String> messagesOf(final String host, final String port, final String brokerName, final String destinationName) throws Exception {
        final ObjectName on = ObjectNameBuilder.create(getDefaultJmxDomain(), brokerName, true).getJMSQueueObjectName(destinationName);

        try (final JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL(format(JMX_URL, host, port)), emptyMap())) {

            final JMSQueueControl queueControl = newProxyInstance(connector.getMBeanServerConnection(), on, JMSQueueControl.class, false);

            final CompositeData[] compositeData = queueControl.browse("");

            return stream(compositeData)
                    .map(cd -> String.valueOf(cd.get("JMSMessageID")))
                    .collect(toList());
        }
    }

    private String createLargeMessage(final long messageSize) throws IOException {
        final StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append("{\n  \"_metadata\": {\n");

        for (long index = 0L; index < messageSize - 1; index++) {
            stringBuilder.append("      \"name")
                    .append(index)
                    .append("\": \"some name\",\n");
        }

        stringBuilder.append("      \"name")
                .append(messageSize)
                .append("\": \"some name\"\n")
                .append("  }\n}");

        return stringBuilder.toString();
    }

    private void putInQueue(final String queueName, final String msgText, final String... origAddress) throws JMSException {
        TextMessage message = JMS_SESSION.createTextMessage(msgText);
        if (origAddress.length > 0) {
            message.setStringProperty("_AMQ_ORIG_ADDRESS", origAddress[0]);
        }
        producerOf(queueName).send(message);
    }

    /**
     * Returns the number of messages that were removed from the queue.
     *
     * @param queueName - the name of the queue that is to be cleaned
     * @return the number of cleaned messagesß
     */
    private int cleanQueue(final String queueName) throws JMSException {
        JMS_CONNECTION.start();
        final MessageConsumer consumer = consumerOf(queueName);

        int cleanedMessage = 0;
        while (consumer.receiveNoWait() != null) {
            cleanedMessage++;
        }
        JMS_CONNECTION.stop();
        return cleanedMessage;
    }

    private MessageConsumer consumerOf(final String queueName) throws JMSException {
        return CONSUMERS.computeIfAbsent(queueName, name -> {
            try {
                return JMS_SESSION.createConsumer(queueOf(name));
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private MessageProducer producerOf(final String queueName) throws JMSException {
        return PRODUCERS.computeIfAbsent(queueName, name -> {
            try {
                return JMS_SESSION.createProducer(queueOf(name));
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static void closeJmsConnection() throws JMSException {
        CONSUMERS.clear();
        PRODUCERS.clear();
        QUEUES.clear();
        JMS_CONNECTION.close();
    }

    private static void openJmsConnection() throws JMSException {
        JMS_CONNECTION = JMS_CF.createQueueConnection();
        JMS_SESSION = JMS_CONNECTION.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);

    }

    private static Queue queueOf(final String queueName) {
        return QUEUES.computeIfAbsent(queueName, name -> createQueue(queueName));
    }
}
