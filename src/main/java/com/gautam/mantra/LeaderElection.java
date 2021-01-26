package com.gautam.mantra;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.List;
import java.util.Map;


public class LeaderElection implements Watcher {

    public static final Yaml yaml = new Yaml();
    public static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getCanonicalName());
    public static Map<String, String> properties;
    private ZooKeeper zooKeeper;
    private String currentZnode;

    public LeaderElection() {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        InputStream inputStream = loader.getResourceAsStream("leader-election.yml");
        properties = yaml.load(inputStream);
        printProperties(properties);
    }

    public static void main(String[] args) {

        LeaderElection leaderElection = new LeaderElection();
        try {

            leaderElection.connectToZookeeper();
            leaderElection.nominateForElection();
            leaderElection.electALeader();
            leaderElection.run();
            leaderElection.close();
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

    /**
     * @param watchedEvent ZK event watcher implementation
     */
    @Override
    public void process(WatchedEvent watchedEvent) {
        switch (watchedEvent.getType()) {
            case None:
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    logger.info("Connected to zookeeper successfully");
                } else {
                    synchronized (zooKeeper) {
                        logger.info("Received disconnection request... ");
                        zooKeeper.notifyAll();
                    }
                }
                break;
        }
    }

    /**
     * A small utility method to print properties
     */
    public static void printProperties(Map<String, String> properties) {
        properties.forEach((k, v) -> System.out.println(k + " -> " + v));
    }

    /**
     * This method is used to nominate for becoming a leader
     *
     * @throws KeeperException      a possible exception
     * @throws InterruptedException a possible exception
     */
    public void nominateForElection() throws KeeperException, InterruptedException {
        String prefix = properties.get("electionNamespace") + "/c_";
        String znodeFullPath = zooKeeper.create(prefix, new byte[]{},
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        logger.info("Znode named " + znodeFullPath + " nominated for leader election");
        this.currentZnode = znodeFullPath.replace("/election/", "");
    }

    /**
     * This method contains the logic to elect a leader
     *
     * @throws KeeperException      a possible exception
     * @throws InterruptedException a possible exception
     */
    public void electALeader() throws KeeperException, InterruptedException {
        List<String> children = zooKeeper.getChildren(properties.get("electionNamespace"), false);

        Collections.sort(children);
        String smallestChild = children.get(0);

        if (smallestChild.equals(currentZnode)) {
            System.out.println("I am the leader");
            return;
        }

        System.out.println("I am not the leader, " + smallestChild + " is the leader");
    }
}
