package io.qimia.uhrwerk.config.builders;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConnectionBuilderTest {
    private final Logger logger = LoggerFactory.getLogger(TransformBuilderTest.class);

    @Test
    void builderTest() {
        var builder = new ConnectionBuilder();

        var connection1 = builder
                .name("s3")
                .s3()
                .path("s3Path")
                .secretKey("secretKey")
                .secretId("secretID")
                .done()
                .build();

        var builder2 = new ConnectionBuilder();

        var connection2 = builder2
                .name("file")
                .file()
                .path("filePath")
                .done()
                .build();

        var builder3 = new ConnectionBuilder();

        var connection3 = builder3
                .name("jdbc")
                .jdbc()
                .jdbcUrl("myURL")
                .jdbcDriver("myDriver")
                .user("user")
                .pass("pass")
                .done()
                .build();

        logger.info(connection1.toString());
        logger.info(connection2.toString());
        logger.info(connection3.toString());

        assertEquals("s3", connection1.getName());
        assertEquals("s3Path", connection1.getPath());

        assertEquals("file", connection2.getName());
        assertEquals("filePath", connection2.getPath());

        assertEquals("jdbc", connection3.getName());
        assertEquals("pass", connection3.getJdbcPass());

    }

    @Test
    void nestedBuilderTest() {
        var builder = new ConnectionBuilder();
        var s3Builder = new S3Builder().path("s3Path")
                .secretKey("secretKey")
                .secretId("secretID");
        var s3 = new S3Builder().path("s3Path2")
                .secretKey("secretKey")
                .secretId("secretID")
                .build();

        var con1 = builder
                .name("s3_1")
                .s3(s3Builder)
                .build();

        var con2 = builder
                .name("s3_2")
                .s3(s3)
                .build();

        logger.info(con1.toString());
        logger.info(con2.toString());

        assertEquals("s3_1", con1.getName());
        assertEquals("s3Path", con1.getPath());

        assertEquals("s3_2", con2.getName());
        assertEquals("s3Path2", con2.getPath());

    }


}