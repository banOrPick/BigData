package com.suntang.hadoop;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class HadooptestApplicationTests {

    // 这里为了直观显示参数 使用了硬编码，实际开发中可以通过外部传参
    private static final String HDFS_URL = "hdfs://192.168.1.102:8020";
    private static final String HADOOP_USER_NAME = "root";
    @Test
    void wordCount() throws Exception{
    }

}
