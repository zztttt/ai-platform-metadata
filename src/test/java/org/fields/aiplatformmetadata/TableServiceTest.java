package org.fields.aiplatformmetadata;

import org.fields.aiplatformmetadata.metadata.Utils;
import org.fields.aiplatformmetadata.metadata.service.MetadataService;
import org.fields.aiplatformmetadata.metadata.service.TableService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.*;

@RunWith(SpringRunner.class)
@SpringBootTest
public class TableServiceTest {
    @Autowired
    MetadataService metadataService;
    @Autowired
    TableService tableService;

    private final String oldTableName = "AShareEODPrices";
    private final String newTableName = "newTable";
    private final String functionName = "wsd";
    private final String oldUpdateTime = "20190602";
    private final String newUpdateTime = "20190603";
    private final String rootUpdateUser = "root";
    private final String updateUser = "zzt";


    @Before
    public void init(){
        Assert.assertTrue(metadataService.insertTableMetadata(oldTableName, functionName, oldUpdateTime, rootUpdateUser));
        Set<Map<String, String>> set = new HashSet<Map<String, String>>(){{
            add(new HashMap<String, String>(){{
                put("tableName", oldTableName);
                put("windColumn", "windcode");
                put("dbColumn", "s_info_windcode");
                put("userColumn", "wind代码");
                put("type", "varchar(10)");
            }});
            add(new HashMap<String, String>(){{
                put("tableName", oldTableName);
                put("windColumn", "lastradeday_s");
                put("dbColumn", "trade_dt");
                put("userColumn", "交易日期");
                put("type", "varchar(10)");
            }});
            add(new HashMap<String, String>(){{
                put("tableName", oldTableName);
                put("windColumn", "pre_close");
                put("dbColumn", "s_dq_preclose");
                put("userColumn", "昨收盘价(元)");
                put("type", "varchar(10)");
            }});
            add(new HashMap<String, String>(){{
                put("tableName", oldTableName);
                put("windColumn", "open");
                put("dbColumn", "s_dq_open");
                put("userColumn", "开盘价(元)");
                put("type", "varchar(10)");
            }});
            add(new HashMap<String, String>(){{
                put("tableName", oldTableName);
                put("windColumn", "high");
                put("dbColumn", "s_dq_high");
                put("userColumn", "最高价(元)");
                put("type", "varchar(10)");
            }});
            add(new HashMap<String, String>(){{
                put("tableName", oldTableName);
                put("windColumn", "low");
                put("dbColumn", "s_dq_low");
                put("userColumn", "最低价(元)");
                put("type", "varchar(10)");
            }});
            add(new HashMap<String, String>(){{
                put("tableName", oldTableName);
                put("windColumn", "close");
                put("dbColumn", "s_dq_close");
                put("userColumn", "收盘价(元)");
                put("type", "varchar(10)");
            }});
            add(new HashMap<String, String>(){{
                put("tableName", oldTableName);
                put("windColumn", "pct_chg");
                put("dbColumn", "s_dq_pctchange");
                put("userColumn", "涨跌幅(%)");
                put("type", "varchar(10)");
            }});
            add(new HashMap<String, String>(){{
                put("tableName", oldTableName);
                put("windColumn", "adjfactor");
                put("dbColumn", "s_dq_adjfactor");
                put("userColumn", "复权因子");
                put("type", "varchar(10)");
            }});
            add(new HashMap<String, String>(){{
                put("tableName", oldTableName);
                put("windColumn", "vwap");
                put("dbColumn", "s_dq_avgprice");
                put("userColumn", "均价(VWAP)");
                put("type", "varchar(10)");
            }});
            add(new HashMap<String, String>(){{
                put("tableName", oldTableName);
                put("windColumn", "trade_status");
                put("dbColumn", "s_dq_tradestatus");
                put("userColumn", "交易状态");
                put("type", "varchar(10)");
            }});
        }};
        Assert.assertTrue(metadataService.insertTableMetadataDetail(set));
    }
    @Test
    public void createTable1Test() throws Exception{
        Set<String> userColumns = new HashSet<String>(){{
            add("wind代码");
            add("交易日期");
            add("交易状态");
        }};
        Assert.assertTrue(tableService.createTable(oldTableName, newTableName, functionName, newUpdateTime, updateUser, userColumns));
    }

    @After
    public void release(){
        Assert.assertTrue(tableService.deleteTable(newTableName));
        Assert.assertTrue(metadataService.deleteTableMetadata(oldTableName));
        Assert.assertTrue(metadataService.deleteTableMetadataDetail(oldTableName));
        Assert.assertTrue(metadataService.deleteTableMetadata(newTableName));
        Assert.assertTrue(metadataService.deleteTableMetadataDetail(newTableName));
    }
}
