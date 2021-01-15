package org.fields.aiplatformmetadata;

import org.fields.aiplatformmetadata.metadata.service.MetadataService;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(SpringRunner.class)
@SpringBootTest
public class MetadataServiceTest {
    @Autowired
    MetadataService metadataService;

    @Test
    public void step1_insertTableMetadataTest(){
        Assert.assertTrue(metadataService.insertTableMetadata(
                "AShareEODPrices", "wsd",
                "20190601", "zzt"));
    }
    @Test
    public void step2_insertTableMetadataDetailTest(){
        Set<Map<String, String>> set = new HashSet<>();

        Map<String, String> map = new HashMap<String, String>(){{
            put("tableName", "AShareEODPrices");
            put("windColumn", "windcode");
            put("dbColumn", "s_info_windcode");
            put("userColumn", "wind代码");
        }};
        set.add(map);

        map = new HashMap<String, String>(){{
            put("tableName", "AShareEODPrices");
            put("windColumn", "lastradeday_s");
            put("dbColumn", "trade_dt");
            put("userColumn", "交易日期");
        }};
        set.add(map);

        map = new HashMap<String, String>(){{
            put("tableName", "AShareEODPrices");
            put("windColumn", "pre_close");
            put("dbColumn", "s_dq_preclose");
            put("userColumn", "昨收盘价(元)");
        }};
        set.add(map);

        map = new HashMap<String, String>(){{
            put("tableName", "AShareEODPrices");
            put("windColumn", "open");
            put("dbColumn", "s_dq_open");
            put("userColumn", "开盘价(元)");
        }};
        set.add(map);

        map = new HashMap<String, String>(){{
            put("tableName", "AShareEODPrices");
            put("windColumn", "high");
            put("dbColumn", "s_dq_high");
            put("userColumn", "最高价(元)");
        }};
        set.add(map);

        map = new HashMap<String, String>(){{
            put("tableName", "AShareEODPrices");
            put("windColumn", "low");
            put("dbColumn", "s_dq_low");
            put("userColumn", "最低价(元)");
        }};
        set.add(map);

        map = new HashMap<String, String>(){{
            put("tableName", "AShareEODPrices");
            put("windColumn", "close");
            put("dbColumn", "s_dq_close");
            put("userColumn", "收盘价(元)");
        }};
        set.add(map);

        map = new HashMap<String, String>(){{
            put("tableName", "AShareEODPrices");
            put("windColumn", "pct_chg");
            put("dbColumn", "s_dq_pctchange");
            put("userColumn", "涨跌幅(%)");
        }};
        set.add(map);

        map = new HashMap<String, String>(){{
            put("tableName", "AShareEODPrices");
            put("windColumn", "adjfactor");
            put("dbColumn", "s_dq_adjfactor");
            put("userColumn", "复权因子");
        }};
        set.add(map);

        map = new HashMap<String, String>(){{
            put("tableName", "AShareEODPrices");
            put("windColumn", "vwap");
            put("dbColumn", "s_dq_avgprice");
            put("userColumn", "均价(VWAP)");
        }};
        set.add(map);

        map = new HashMap<String, String>(){{
            put("tableName", "AShareEODPrices");
            put("windColumn", "trade_status");
            put("dbColumn", "s_dq_tradestatus");
            put("userColumn", "交易状态");
        }};
        set.add(map);
        metadataService.insertTableMetadataDetail(set);
    }

    @Test
    public void step3_deleteTableMetadataTest(){
        Assert.assertTrue(metadataService.deleteTableMetadata("AShareEODPrices"));
    }
    @Test
    public void step4_deleteTableMetadataDetailTest(){
        Assert.assertTrue(metadataService.deleteTableMetadataDetail("AShareEODPrices"));
    }
}
