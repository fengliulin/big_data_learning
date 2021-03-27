import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

public class HdfsAPI {

    /**
     * url访问方式
     *
     * @throws IOException
     */
    @Test
    public void urlHdfs() throws IOException {
        // 1、注册url
        // 我也不知道为什么要添加这个，如果不添加会提示【未知的协议hdfs】
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());

        // 2、获取hdfs文件的输入流
        InputStream inputStream = new URL("hdfs://192.168.1.100:8020/dir1/调整鼠标灵敏度-命令行.txt").openStream();

        // 3、获取本地文件的输出流
        FileOutputStream outputStream = new FileOutputStream(new File("/Users/fengliulin/Documents/pd序列号.txt"));

        // 4、实现文件的拷贝
        IOUtils.copy(inputStream, outputStream);

        // 5、关闭流
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(outputStream);
    }

    /**
     * hdfs文件的遍历
     */
    @Test
    public void listFiles() throws URISyntaxException, IOException {
        // 1、获取FileSystem实例
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://192.168.1.100:8020"), new Configuration());

        // 2、调用方法listFiles 获取 / 目录下的所有文件信息
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path("/"), true);

        // 3、遍历迭代器
        while (iterator.hasNext()) {
            LocatedFileStatus next = iterator.next();
            System.out.println(next.getPath() + "----" + next.getPath().getName());

            // 文件的block信息
            BlockLocation[] blockLocations = next.getBlockLocations();
            System.out.println("block数:" + blockLocations.length);
        }
    }

    /**
     * hdfs创建文件夹
     */
    @Test
    public void mkdirsTest() throws URISyntaxException, IOException {
        // 1、获取FileSystem实例子
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://192.168.1.100:8020"), new Configuration());

        // 2、创建文件夹
        boolean bl = fileSystem.mkdirs(new Path("/aaa/bbb/ccc"));
        System.out.println(bl);

        // 创建文件(如果父目录不存在也会直接创建)
        fileSystem.create(new Path("/aaa/bbb/ccc/a.txt"));

        // 3、关闭FileSystem
        fileSystem.close();
    }

    /**
     * 小文件的合并, 把文件里面的内容，合并到一个文件
     */
    @Test
    public void mergeFile() throws URISyntaxException, IOException, InterruptedException {
        // 1、获取FileSystem（分布式文件系统）
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://192.168.1.100:8020"), new Configuration());

        // 2、获取hdfs大文件的输出流
        FSDataOutputStream outputStream = fileSystem.create(new Path("/big_txt.txt"));

        // 3、获取一个本地文件系统
        LocalFileSystem localFileSystem = FileSystem.getLocal(new Configuration());

        // 4、获取本地文件夹下所有文件的详情
        FileStatus[] fileStatuses = localFileSystem.listStatus(new Path("/Users/fengliulin/Documents/未命名文件夹"));

        // 5、遍历每个文件，获取每个文件的输入流
        for (FileStatus fileStatus : fileStatuses) {
            FSDataInputStream inputStream = localFileSystem.open(fileStatus.getPath());

            // 6、将小文件的数据复制到大文件
            IOUtils.copy(inputStream, outputStream);
            // 5、关闭流
            IOUtils.closeQuietly(inputStream);
        }

        // 7、关闭流
        // 5、关闭流
        IOUtils.closeQuietly(outputStream);
        localFileSystem.close();
        fileSystem.close();
    }

    /**
     * 文件的上传
     */
    @Test
    public void uploadFile() throws URISyntaxException, IOException {
        // 1、获取FileSystem 实例
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://192.168.1.100:8020"), new Configuration());

        // 调用方法，实现上传
        fileSystem.copyFromLocalFile(new Path("/Users/fengliulin/Documents/pd序列号.txt"), new Path("/"));

        // 关闭
        fileSystem.close();
    }

    //region 文件的下载 2种方式

    /**
     * 实现文件的下载 方式1
     */
    @Test
    public void downloadFile1() throws URISyntaxException, IOException {
        // 1、获取FileSystem
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://192.168.1.100:8020"), new Configuration());

        // 2、获取hdfs的输入流
        FSDataInputStream inputStream = fileSystem.open(new Path("/aaa/bbb/ccc/a.txt"));

        // 3、获取本地路径的输出流
        FileOutputStream outputStream = new FileOutputStream("/Users/fengliulin/Documents/b.txt");

        // 4、文件的拷贝
        IOUtils.copy(inputStream, outputStream);

        // 5、关闭流
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(outputStream);
    }

    /**
     * 实现文件的下载 方式2
     */
    @Test
    public void downloadFile2() throws URISyntaxException, IOException {
        // 1、获取FileSystem
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://192.168.1.100:8020"), new Configuration());

        // 2、调用方法，实现文件的下载
        fileSystem.copyToLocalFile(new Path("/aaa/bbb/ccc/a.txt"), new Path("/Users/fengliulin/Documents/b1.txt"));

        // 3、关闭FileSystem
        fileSystem.close();
    }
    //endregion

    //region 获取FileSystem的4种方式

    /**
     * 获取FileSystem 方式1
     */
    @Test
    public void getFileSystem1() throws IOException {
        // 1、创建Configuration对象
        Configuration configuration = new Configuration();

        // 2、设置文件系统的类型
        configuration.set("fs.defaultFS", "hdfs://192.168.1.100:8020");

        // 3、获取制定的文件系统
        FileSystem fileSystem = FileSystem.get(configuration);

        // 4、输出
        System.out.println(fileSystem); // result: DFS[DFSClient[clientName=DFSClient_NONMAPREDUCE_-1096323710_1, ugi=fengliulin (auth:SIMPLE)]]
    }

    /**
     * 获取FileSystem 方式2
     */
    @Test
    public void getFileSystem2() throws IOException, URISyntaxException {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.1.100:8020"), new Configuration());
        System.out.println(fileSystem);
    }

    /**
     * 获取FileSystem 方式3
     */
    @Test
    public void getFileSystem3() throws IOException, URISyntaxException {
        Configuration configuration = new Configuration();

        // 指定文件系统类型
        configuration.set("fs.defaultFS", "hdfs://192.168.1.100:8020");

        // 获取指定的文件系统
        FileSystem fileSystem = FileSystem.newInstance(configuration);

        System.out.println(fileSystem);
    }

    /**
     * 获取FileSystem 方式4
     */
    @Test
    public void getFileSystem4() throws IOException, URISyntaxException {
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://192.168.1.100:8020"), new Configuration());
        System.out.println(fileSystem);
    }
    //endregion
}
