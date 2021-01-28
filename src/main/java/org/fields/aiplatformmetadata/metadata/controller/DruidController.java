package org.fields.aiplatformmetadata.metadata.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping
public class DruidController {
    @Autowired
    JdbcTemplate jdbcTemplate;

    @GetMapping("/userList")
    public List<Map<String, Object>> userList(){
        String sql = "select * from wind.task";
        List<Map<String, Object>> maps = jdbcTemplate.queryForList(sql);
        return maps;
    }

    @GetMapping("/druid_create")
    public String create(){
        String sql = "select * from wind_AShareEODPrices_test where s_info_windcode='000001.SZ' and trade_dt='20190604'";
        List<Map<String, Object>> result = jdbcTemplate.queryForList(sql);
        return "success";
    }
}
