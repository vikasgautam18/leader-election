package com.gautam.mantra;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;

public class WatcherAPIExample implements Watcher{

    public static final Yaml yaml = new Yaml();
    public static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getCanonicalName());
    public static Map<String, String> properties;
    private ZooKeeper zooKeeper;

    public WatcherAPIExample() {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        InputStream inputStream = loader.getResourceAsStream("leader-election.yml");
        properties = yaml.load(inputStream);
        printProperties(properties);
    }

    public static void main(String[] args) {

        WatcherAPIExample example = new WatcherAPIExample();
        try {
            example.connectToZookeeper();
            example.watchTargetZnode();
            example.run();
            example.close();
            logger.info("Disconnected from Zookeeper");

        } catch (IOException | InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }

    /**
     * method to connect to zk
     *
     * @throws IOException throws an IO Exception if ZK is not reachable
     */
    public void connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(properties.getOrDefault("zookeeperAddress", "localhost:2181"),
                Integer.parseInt(properties.getOrDefault("sessionTimeout", "3000")), this);
    }

    public void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    public void watchTargetZnode() throws KeeperException, InterruptedException {
        final String TARGET_ZNODE = properties.get("demoNamespace");
        Stat stat = zooKeeper.exists(TARGET_ZNODE, this);

        if(stat != null){
            byte[] data = zooKeeper.getData(TARGET_ZNODE, this, stat);
            List<String> children = zooKeeper.getChildren(TARGET_ZNODE, this);
            logger.info("Data : " + new String(data) + ", children : " + children);
        } else {
            // do nothing if the node does not exist
            return;
        }
    }

    /**
     * @param watchedEvent ZK event watcher implementation
     */
    @Override
    public void process(WatchedEvent watchedEvent) {
        final String TARGET_ZNODE = properties.get("demoNamespace");
        switch (watchedEvent.getType()) {
            case None:
                if (watchedEvent.getState() == Watcher.Event.KeeperState.SyncConnected) {
                    logger.info("Connected to zookeeper successfully");
                } else {
                    synchronized (zooKeeper) {
                        logger.info("Received disconnection request... ");
                        zooKeeper.notifyAll();
                    }
                }
                break;

            case NodeDeleted:
                logger.info("The node " + TARGET_ZNODE + " has been deleted");
                break;

            case NodeCreated:
                logger.info(TARGET_ZNODE + " was created");
                break;

            case NodeDataChanged:
                logger.info(TARGET_ZNODE + " data changed");
                break;

            case NodeChildrenChanged:
                logger.info(TARGET_ZNODE + " children changed");
                break;
        }

        try {
            watchTargetZnode();
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * A small utility method to print properties
     */
    public static void printProperties(Map<String, String> properties) {
        properties.forEach((k, v) -> System.out.println(k + " -> " + v));
    }
}
