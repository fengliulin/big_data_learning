import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public class ZookeeperAPITest {

    /**
     * 创建永久节点
     *
     * @throws Exception
     */
    @Test
    public void createZnode() throws Exception {
        // 1、定制一个重试策略
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 1);

        // 2、获取一个客户端对象
        /*
         * param1: 要连接的Zookeeper服务器列表
         * param2: 会话超时时间
         * param3: 连接超时时间
         * param4: 重试策略
         */
        String connectionStr = "192.168.1.100:2181, 192.168.1.110:2181, 192.168.1.120:2181";
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionStr, 8000, 8000, retryPolicy);

        // 3、开启客户端
        client.start();

        // 4、创建节点
        client.create().creatingParentContainersIfNeeded()
                       .withMode(CreateMode.PERSISTENT)
                       .forPath("/hello24", "world".getBytes(StandardCharsets.UTF_8));

        // 5、关闭客户端
        client.close();
    }

    /**
     * 创建临时节点
     *
     * @throws Exception
     */
    @Test
    public void createZnodeTemp() throws Exception {
        // 1、定制一个重试策略
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 1);

        // 2、获取一个客户端对象
        /*
         * param1: 要连接的Zookeeper服务器列表
         * param2: 会话超时时间
         * param3: 连接超时时间
         * param4: 重试策略
         */
        String connectionStr = "192.168.1.100:2181, 192.168.1.110:2181, 192.168.1.120:2181";
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionStr, 8000, 8000, retryPolicy);

        // 3、开启客户端
        client.start();

        // 4、创建节点
        client.create().creatingParentContainersIfNeeded()
                .withMode(CreateMode.EPHEMERAL)
                .forPath("/hello3", "world".getBytes(StandardCharsets.UTF_8));

        // 5、关闭客户端
        client.close();
    }

    /**
     * 设置节点数据
     *
     * @throws Exception
     */
    @Test
    public void setZnodeData() throws Exception {
        // 1、定制一个重试策略
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 1);

        // 2、获取一个客户端对象
        /*
         * param1: 要连接的Zookeeper服务器列表
         * param2: 会话超时时间
         * param3: 连接超时时间
         * param4: 重试策略
         */
        String connectionStr = "192.168.1.100:2181, 192.168.1.110:2181, 192.168.1.120:2181";
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionStr, 8000, 8000, retryPolicy);

        // 3、开启客户端
        client.start();

        // 4、修改节点数据
        client.setData().forPath("/hello", "zookeeper".getBytes(StandardCharsets.UTF_8));

        // 5、关闭客户端
        client.close();
    }

    /**
     * 获取节点数据
     *
     * @throws Exception
     */
    @Test
    public void getZondeData() throws Exception {
        // 1、定制一个重试策略
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 1);

        // 2、获取一个客户端对象
        /*
         * param1: 要连接的Zookeeper服务器列表
         * param2: 会话超时时间
         * param3: 连接超时时间
         * param4: 重试策略
         */
        String connectionStr = "192.168.1.100:2181, 192.168.1.110:2181, 192.168.1.120:2181";
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionStr, 8000, 8000, retryPolicy);

        // 3、开启客户端
        client.start();

        // 4、获取节点数据
        byte[] bytes = client.getData().forPath("/hello");
        System.out.println(new String(bytes));

        // 5、关闭客户端
        client.close();
    }

    /**
     * 节点的watch机制
     *
     * @throws Exception
     */
    @Test
    public void watchZnode() throws Exception {
        // 1、定制一个重试策略
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 1);

        // 2、获取一个客户端对象
        /*
         * param1: 要连接的Zookeeper服务器列表
         * param2: 会话超时时间
         * param3: 连接超时时间
         * param4: 重试策略
         */
        String connectionStr = "192.168.1.100:2181, 192.168.1.110:2181, 192.168.1.120:2181";
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionStr, 8000, 8000, retryPolicy);

        // 3、开启客户端
        client.start();

        // 4、创建一个TreeCache对象，制定要监控的节点路径
        TreeCache treeCache = new TreeCache(client, "/hello3");

        // 5、自定义一个监听器
        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
                ChildData data = event.getData();
                if (data != null) {
                    switch (event.getType()) {
                        case NODE_ADDED:
                            System.out.println("监控到有新增节点！");
                            break;
                        case NODE_REMOVED:
                            System.out.println("监控到有节点被移除");
                        case NODE_UPDATED:
                            System.out.println("监控到节点被更新！");
                            break;
                        default:
                            break;
                    }
                }
            }
        });

        // 6、开始监听
        treeCache.start();

        Thread.sleep(1000000000);
    }
}
