package org.fields.aiplatformmetadata;

import org.fields.aiplatformmetadata.metadata.FailUtils;
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

    // transaction 最小批量 最后一起 commit
    private final String oldTableName = "wind_AShareEODPrices_test";
    private final String newTableName1 = "newTable1";
    private final String functionName = "wsd";
    private final String oldUpdateTime = "20190602";
    private final String newUpdateTime = "20190603";
    private final String startStr = "20180101";
    private final String endStr = "20180202";
    private final String rootUpdateUser = "root";
    private final String updateUser = "zzt";

    @Before
    public void init(){
        List<Map<String, String>> list = new ArrayList<Map<String, String>>(){{
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
        List<String> columns = new ArrayList<String>(){{
            add(list.get(0).get("dbColumn"));
            add(list.get(1).get("dbColumn"));
            add(list.get(2).get("dbColumn"));
            add(list.get(3).get("dbColumn"));
            add(list.get(4).get("dbColumn"));
            add(list.get(5).get("dbColumn"));
            add(list.get(6).get("dbColumn"));
            add(list.get(7).get("dbColumn"));
            add(list.get(8).get("dbColumn"));
            add(list.get(9).get("dbColumn"));
            add(list.get(10).get("dbColumn"));
        }};
        List<String> types = new ArrayList<String>(){{
            add(list.get(0).get("type"));
            add(list.get(1).get("type"));
            add(list.get(2).get("type"));
            add(list.get(3).get("type"));
            add(list.get(4).get("type"));
            add(list.get(5).get("type"));
            add(list.get(6).get("type"));
            add(list.get(7).get("type"));
            add(list.get(8).get("type"));
            add(list.get(9).get("type"));
            add(list.get(10).get("type"));
        }};
        Assert.assertTrue(tableService.createTableBase(oldTableName, columns, types));
        Assert.assertTrue(FailUtils.insertIntoTable(oldTableName, columns));
        Assert.assertTrue(FailUtils.insertIntoTable(oldTableName, columns));
        Assert.assertTrue(FailUtils.insertIntoTable(oldTableName, columns));

        Assert.assertTrue(metadataService.insertTableMetadata(oldTableName, functionName, oldUpdateTime, rootUpdateUser));
        Assert.assertTrue(metadataService.insertTableMetadataDetail(list));

    }
    @Test
    public void createTable1Test() throws Exception{
//        List<String> windColumns = new ArrayList<String>(){{
//            add("windcode");
//            add("lastradeday_s");
//            add("trade_status");
//            add("new_wind_column");
//        }};
//        List<String> dbColumns = new ArrayList<String>(){{
//            add("s_info_windcode_1");
//            add("trade_dt_1");
//            add("s_dq_tradestatus_1");
//            add(null);
//        }};
//        List<String> userColumns = new ArrayList<String>(){{
//            add("wind代码");
//            add("交易日期");
//            add("交易状态");
//            add("new_user_column");
//        }};
//        List<String> types = new ArrayList<String>(){{
//            add("varchar(10)");
//            add("varchar(11)");
//            add("varchar(12)");
//            add("varchar(13)");
//        }};
//        Assert.assertTrue(tableService.createTable(oldTableName, newTableName1, functionName, newUpdateTime, updateUser, startStr, endStr, windColumns, dbColumns, userColumns, types));
    }

    @After
    public void release(){
//        Assert.assertTrue(tableService.deleteTable(oldTableName));
//        Assert.assertTrue(metadataService.deleteTableMetadata(oldTableName));
//        Assert.assertTrue(metadataService.deleteTableMetadataDetail(oldTableName));

//        Assert.assertTrue(tableService.deleteTable(newTableName1));
//        Assert.assertTrue(metadataService.deleteTableMetadata(newTableName1));
//        Assert.assertTrue(metadataService.deleteTableMetadataDetail(newTableName1));
    }
}
