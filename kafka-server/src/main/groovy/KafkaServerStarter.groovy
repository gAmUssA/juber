import groovy.transform.CompileStatic
import groovy.util.logging.Log4j
import kafka.server.KafkaConfig
import kafka.server.KafkaServer
import kafka.utils.MockTime
import kafka.utils.TestUtils
import kafka.utils.ZKStringSerializer$
import kafka.utils.ZkUtils
import kafka.zk.EmbeddedZookeeper
import org.I0Itec.zkclient.ZkClient

import java.nio.file.Files

@CompileStatic
@Log4j("logger")
class KafkaServerStarter {

    private EmbeddedZookeeper zkServer
    private ZkUtils zkUtils
    private KafkaServer kafkaServer

    static void main(String[] args) {
        def starter = new KafkaServerStarter()

        starter.createKafkaCluster()
        //Runtime.getRuntime().addShutdownHook(new Thread({ starter.shutdownKafkaCluster() }))

    }

    // Creates an embedded zookeeper server and a kafka broker
    private void createKafkaCluster() throws IOException {
        zkServer = new EmbeddedZookeeper()
        logger.info "zoo started"
        String zkConnect = "127.0.0.1:" + zkServer.port()
        ZkClient zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer$.MODULE$)
        zkUtils = ZkUtils.apply(zkClient, false)

        KafkaConfig config = new KafkaConfig(props(
                "zookeeper.connect", zkConnect,
                "broker.id", "0",
                "log.dirs", Files.createTempDirectory("kafka-").toAbsolutePath().toString(),
                "listeners", "PLAINTEXT://localhost:9092"))
        def mock = new MockTime()
        kafkaServer = TestUtils.createServer(config, mock)
    }

    private void shutdownKafkaCluster() {
        kafkaServer.shutdown()
        zkUtils.close()
        zkServer.shutdown()
    }

    private static Properties props(String... kvs) {
        final Properties props = new Properties()
        for (int i = 0; i < kvs.length;) {
            props.setProperty(kvs[i++], kvs[i++])
        }
        return props
    }
}
