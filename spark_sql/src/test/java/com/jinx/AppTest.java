package com.jinx;

import static org.junit.Assert.assertTrue;

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.sql.CallableStatement;
import java.sql.Types;
import java.util.ArrayList;
import org.elasticsearch.spark.sql.*;

/**
 * Unit test for simple App.
 */
public class AppTest {
    /**
     * Rigorous Test :-)
     */

    @Test
    public void test() {

        //JavaEsSparkSQL.saveToEs(SparkSession.builder().getOrCreate().sql("1"),"2");
    }
}