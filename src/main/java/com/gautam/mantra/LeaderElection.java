package com.gautam.mantra;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.Map;


public class LeaderElection implements Watcher {

    public static final Yaml yaml = new Yaml();
    public static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getCanonicalName());
    public static Map<String, String> properties;
    private ZooKeeper zooKeeper;

    public LeaderElection(){
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        InputStream inputStream = loader.getResourceAsStream("leader-election.yml");
        properties = yaml.load(inputStream);
        printProperties(properties);
    }

    public static void main(String[] args) {

        LeaderElection leaderElection = new LeaderElection();
        try {
            leaderElection.connectToZookeeper();
            leaderElection.run();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

    }

    /**
     * method to connect to zk
     * @throws IOException
     */
    public void connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(properties.getOrDefault("zookeeperAddress", "localhost:2181"),
                Integer.parseInt(properties.getOrDefault("sessionTimeout", "3000")), this);
    }

    public void run() throws InterruptedException {
        synchronized (zooKeeper){
            zooKeeper.wait();
        }

    }
    /**
     *
     * @param watchedEvent ZK event watcher implementation
     */
    @Override
    public void process(WatchedEvent watchedEvent) {
        switch (watchedEvent.getType()){
            case None:
                if(watchedEvent.getState() == Event.KeeperState.SyncConnected){
                    logger.info("Connected to zookeeper successfully");
                }
        }
    }

    /**
     *
     *
     */
    public static void printProperties(Map<String, String> properties){
        properties.forEach((k, v) -> System.out.println(k + " -> " + v));
    }


}
