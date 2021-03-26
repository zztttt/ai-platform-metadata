use wind;
DROP TABLE IF EXISTS metadata;
DROP TABLE IF EXISTS metadatadetail;
DROP TABLE IF EXISTS task;

DROP TABLE IF EXISTS wind_ashareeodprices_test;
DROP TABLE IF EXISTS wind_ccommodity_test;
DROP TABLE IF EXISTS wind_asharedescription_test;
DROP TABLE IF EXISTS wind_GlobalMacrography_test;

CREATE TABLE metadata
(
    id BIGINT(64) NOT NULL AUTO_INCREMENT,
    tablename VARCHAR(50) NULL,
    func VARCHAR(5) NULL,
    updatetime VARCHAR(8) NULL,
    updateuser VARCHAR(20) NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

CREATE TABLE metadatadetail
(
    id BIGINT(64) NOT NULL AUTO_INCREMENT,
    tablename VARCHAR(100) NULL,
    windcolumn VARCHAR(40) NULL,
    dbcolumn VARCHAR(40) NULL,
    usercolumn VARCHAR(40) NULL,
    type VARCHAR(20) NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

CREATE TABLE task
(
    id BIGINT(64) NOT NULL AUTO_INCREMENT,
    username VARCHAR(20) NULL,
    tablename VARCHAR(40) NULL,
    description VARCHAR(100) NULL,
    parameter longtext NULL,
    status VARCHAR(10) NULL,
    PRIMARY KEY(id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

CREATE TABLE `wind_ashareeodprices_test` (
                                             `id` bigint(64) NOT NULL AUTO_INCREMENT,
                                             `s_info_windcode` varchar(40) DEFAULT NULL,
                                             `trade_dt` varchar(8) DEFAULT NULL,
                                             `s_dq_preclose` float(20,4) DEFAULT NULL,
  `s_dq_open` float(20,4) DEFAULT NULL,
  `s_dq_high` float(20,4) DEFAULT NULL,
  `s_dq_low` float(20,4) DEFAULT NULL,
  `s_dq_close` float(20,4) DEFAULT NULL,
  `s_sq_pctchange` float(20,4) DEFAULT NULL,
  `s_dq_adjfactor` float(20,6) DEFAULT NULL,
  `s_dq_avgprice` float(20,4) DEFAULT NULL,
  `s_dq_tradestatus` varchar(10) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

CREATE TABLE `wind_ccommodity_test` (
                                        `id` bigint(64) NOT NULL AUTO_INCREMENT,
                                        `s_info_windcode` varchar(40) DEFAULT NULL,
                                        `trade_dt` varchar(8) DEFAULT NULL,
                                        `s_dq_presettle` float(20,4) DEFAULT NULL,
  `s_dq_open` float(20,4) DEFAULT NULL,
  `s_dq_high` float(20,4) DEFAULT NULL,
  `s_dq_low` float(20,4) DEFAULT NULL,
  `s_dq_close` float(20,4) DEFAULT NULL,
  `s_dq_settle` float(20,4) DEFAULT NULL,
  `s_dq_volume` float(20,4) DEFAULT NULL,
  `s_dq_oi` float(20,4) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

CREATE TABLE `wind_asharedescription_test` (
                                               `id` bigint(64) NOT NULL AUTO_INCREMENT,
                                               `s_info_windcode` varchar(40) DEFAULT NULL,
                                               `s_info_code` varchar(40) DEFAULT NULL,
                                               `s_info_compname` varchar(100) DEFAULT NULL,
                                               `s_info_listdate` varchar(8) DEFAULT NULL,
                                               PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

CREATE TABLE `wind_GlobalMacrography_test` (
                                               `id` bigint(64) NOT NULL AUTO_INCREMENT,
                                               `s_info_windcode` varchar(40) DEFAULT NULL,
                                               `trade_dt` varchar(8) DEFAULT NULL,
                                               `data` float(20,4)  DEFAULT NULL,
  `description` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

INSERT INTO `metadata` (`id`,`tablename`,`func`,`updatetime`,`updateuser`) VALUES (1, 'wind_AShareEODPrices_test','wsd', '20190603', 'root');
INSERT INTO `metadata` (`id`,`tablename`,`func`,`updatetime`,`updateuser`) VALUES (2, 'wind_CCommodity_test','wsd', '20190603', 'root');
INSERT INTO `metadata` (`id`,`tablename`,`func`,`updatetime`,`updateuser`) VALUES (3, 'wind_AShareDescription_test','wsd', '20190603', 'root');
INSERT INTO `metadata` (`id`,`tablename`,`func`,`updatetime`,`updateuser`) VALUES (4, 'wind_GlobalMacrography_test','edb', '20190603', 'root');

INSERT INTO `metadatadetail` (`tablename`,`windcolumn`,`dbcolumn`,`usercolumn`,`type`) VALUES ('wind_AShareEODPrices_test','windcode','s_info_windcode','wind代码','varchar(40)');
INSERT INTO `metadatadetail` (`tablename`,`windcolumn`,`dbcolumn`,`usercolumn`,`type`) VALUES ('wind_AShareEODPrices_test','lastradeday_s','trade_dt','交易日期','varchar(8)');
INSERT INTO `metadatadetail` (`tablename`,`windcolumn`,`dbcolumn`,`usercolumn`,`type`) VALUES ('wind_AShareEODPrices_test','pre_close','s_dq_preclose','昨收盘价(元)','float(20,4)');
INSERT INTO `metadatadetail` (`tablename`,`windcolumn`,`dbcolumn`,`usercolumn`,`type`) VALUES ('wind_AShareEODPrices_test','open','s_dq_open','开盘价(元)','float(20,4)');
INSERT INTO `metadatadetail` (`tablename`,`windcolumn`,`dbcolumn`,`usercolumn`,`type`) VALUES ('wind_AShareEODPrices_test','high','s_dq_high','最高价(元)','float(20,4)');
INSERT INTO `metadatadetail` (`tablename`,`windcolumn`,`dbcolumn`,`usercolumn`,`type`) VALUES ('wind_AShareEODPrices_test','low','s_dq_low','最低价(元)','float(20,4)');
INSERT INTO `metadatadetail` (`tablename`,`windcolumn`,`dbcolumn`,`usercolumn`,`type`) VALUES ('wind_AShareEODPrices_test','close','s_dq_close','收盘价(元)','float(20,4)');
INSERT INTO `metadatadetail` (`tablename`,`windcolumn`,`dbcolumn`,`usercolumn`,`type`) VALUES ('wind_AShareEODPrices_test','pct_chg','s_sq_pctchange','涨跌幅(%)','float(20,4)');
INSERT INTO `metadatadetail` (`tablename`,`windcolumn`,`dbcolumn`,`usercolumn`,`type`) VALUES ('wind_AShareEODPrices_test','adjfactor','s_dq_adjfactor','复权因子','float(20,6)');
INSERT INTO `metadatadetail` (`tablename`,`windcolumn`,`dbcolumn`,`usercolumn`,`type`) VALUES ('wind_AShareEODPrices_test','vwap','s_dq_avgprice','均价(VWAP)','float(20,4)');
INSERT INTO `metadatadetail` (`tablename`,`windcolumn`,`dbcolumn`,`usercolumn`,`type`) VALUES ('wind_AShareEODPrices_test','trade_status','s_dq_tradestatus','交易状态','varchar(10)');

INSERT INTO `metadatadetail` (`tablename`,`windcolumn`,`dbcolumn`,`usercolumn`,`type`) VALUES ('wind_CCommodity_test','windcode','s_info_windcode','wind代码','varchar(40)');
INSERT INTO `metadatadetail` (`tablename`,`windcolumn`,`dbcolumn`,`usercolumn`,`type`) VALUES ('wind_CCommodity_test','lastradeday_s','trade_dt','交易日期','varchar(8)');
INSERT INTO `metadatadetail` (`tablename`,`windcolumn`,`dbcolumn`,`usercolumn`,`type`) VALUES ('wind_CCommodity_test','pre_close','s_dq_presettle','前结算价(元)','float(20,4)');
INSERT INTO `metadatadetail` (`tablename`,`windcolumn`,`dbcolumn`,`usercolumn`,`type`) VALUES ('wind_CCommodity_test','open','s_dq_open','开盘价(元)','float(20,4)');
INSERT INTO `metadatadetail` (`tablename`,`windcolumn`,`dbcolumn`,`usercolumn`,`type`) VALUES ('wind_CCommodity_test','high','s_dq_high','最高价(元)','float(20,4)');
INSERT INTO `metadatadetail` (`tablename`,`windcolumn`,`dbcolumn`,`usercolumn`,`type`) VALUES ('wind_CCommodity_test','low','s_dq_low','最低价(元)','float(20,4)');
INSERT INTO `metadatadetail` (`tablename`,`windcolumn`,`dbcolumn`,`usercolumn`,`type`) VALUES ('wind_CCommodity_test','close','s_dq_close','收盘价(元)','float(20,4)');
INSERT INTO `metadatadetail` (`tablename`,`windcolumn`,`dbcolumn`,`usercolumn`,`type`) VALUES ('wind_CCommodity_test','settle','s_dq_settle','结算价(元)','float(20,4)');
INSERT INTO `metadatadetail` (`tablename`,`windcolumn`,`dbcolumn`,`usercolumn`,`type`) VALUES ('wind_CCommodity_test','volume','s_dq_volume','成交量(手)','float(20,4)');
INSERT INTO `metadatadetail` (`tablename`,`windcolumn`,`dbcolumn`,`usercolumn`,`type`) VALUES ('wind_CCommodity_test','oi','s_dq_oi','持仓量','float(20,4)');

INSERT INTO `metadatadetail` (`id`,`tablename`,`windcolumn`,`dbcolumn`,`usercolumn`,`type`) VALUES (22,'wind_AShareDescription_test','windcode','s_info_windcode','Wind代码','varchar(40)');
INSERT INTO `metadatadetail` (`id`,`tablename`,`windcolumn`,`dbcolumn`,`usercolumn`,`type`) VALUES (23,'wind_AShareDescription_test','trade_code','s_info_code','证券代码','varchar(40)');
INSERT INTO `metadatadetail` (`id`,`tablename`,`windcolumn`,`dbcolumn`,`usercolumn`,`type`) VALUES (24,'wind_AShareDescription_test ','comp_name','s_info_compname','公司中文名称','varchar(100)');
INSERT INTO `metadatadetail` (`id`,`tablename`,`windcolumn`,`dbcolumn`,`usercolumn`,`type`) VALUES (25,'wind_AShareDescription_test','ipo_date','s_info_listdate','上市日期','varchar(8)');

INSERT INTO `metadatadetail` (`tablename`,`windcolumn`,`dbcolumn`,`usercolumn`,`type`) VALUES ('wind_GlobalMacrography_test','windcode','s_info_windcode','wind代码','varchar(40)');
INSERT INTO `metadatadetail` (`tablename`,`windcolumn`,`dbcolumn`,`usercolumn`,`type`) VALUES ('wind_GlobalMacrography_test','lastradeday_s','trade_dt','交易日期','varchar(8)');
INSERT INTO `metadatadetail` (`tablename`,`windcolumn`,`dbcolumn`,`usercolumn`,`type`) VALUES ('wind_GlobalMacrography_test','null','data','数据','float(20,4)');
INSERT INTO `metadatadetail` (`tablename`,`windcolumn`,`dbcolumn`,`usercolumn`,`type`) VALUES ('wind_GlobalMacrography_test','null','description','数据描述','varchar(100)');


INSERT INTO `wind_ashareeodprices_test` (`s_info_windcode`,`trade_dt`,`s_dq_preclose`,`s_dq_open`,`s_dq_high`,`s_dq_low`,`s_dq_close`,`s_sq_pctchange`,`s_dq_adjfactor`,`s_dq_avgprice`,`s_dq_tradestatus`) VALUES ('000001.SZ','20190603','12.1800',NULL,NULL,'11.8200','11.9000','-2.3000','108.030000','12.0100','交易');
INSERT INTO `wind_ashareeodprices_test` (`s_info_windcode`,`trade_dt`,`s_dq_preclose`,`s_dq_open`,`s_dq_high`,`s_dq_low`,`s_dq_close`,`s_sq_pctchange`,`s_dq_adjfactor`,`s_dq_avgprice`,`s_dq_tradestatus`) VALUES ('000001.SZ','20190606','11.9700',NULL,'12.0700','11.8900','11.9200','-0.4200','108.030000','11.9800','交易');
INSERT INTO `wind_ashareeodprices_test` (`s_info_windcode`,`trade_dt`,`s_dq_preclose`,`s_dq_open`,`s_dq_high`,`s_dq_low`,`s_dq_close`,`s_sq_pctchange`,`s_dq_adjfactor`,`s_dq_avgprice`,`s_dq_tradestatus`) VALUES ('000001.SZ','20190610','11.9200',NULL,'12.4700','11.9800','12.3400','3.5200','108.030000','12.2700','交易');
INSERT INTO `wind_ashareeodprices_test` (`s_info_windcode`,`trade_dt`,`s_dq_preclose`,`s_dq_open`,`s_dq_high`,`s_dq_low`,`s_dq_close`,`s_sq_pctchange`,`s_dq_adjfactor`,`s_dq_avgprice`,`s_dq_tradestatus`) VALUES ('000002.SZ','20190603','26.7000','26.8100','27.0200','26.2800','26.4400','-0.9700','142.670000','26.5400','交易');
INSERT INTO `wind_ashareeodprices_test` (`s_info_windcode`,`trade_dt`,`s_dq_preclose`,`s_dq_open`,`s_dq_high`,`s_dq_low`,`s_dq_close`,`s_sq_pctchange`,`s_dq_adjfactor`,`s_dq_avgprice`,`s_dq_tradestatus`) VALUES ('000002.SZ','20190604','26.4400','26.4700','26.5400','26.2500','26.3000','-0.5300','142.670000','26.3800','交易');
INSERT INTO `wind_ashareeodprices_test` (`s_info_windcode`,`trade_dt`,`s_dq_preclose`,`s_dq_open`,`s_dq_high`,`s_dq_low`,`s_dq_close`,`s_sq_pctchange`,`s_dq_adjfactor`,`s_dq_avgprice`,`s_dq_tradestatus`) VALUES ('000002.SZ','20190605','26.3000','26.6400','27.2800','26.6300','27.0300','2.7800','142.670000','26.9700','交易');
INSERT INTO `wind_ashareeodprices_test` (`s_info_windcode`,`trade_dt`,`s_dq_preclose`,`s_dq_open`,`s_dq_high`,`s_dq_low`,`s_dq_close`,`s_sq_pctchange`,`s_dq_adjfactor`,`s_dq_avgprice`,`s_dq_tradestatus`) VALUES ('000002.SZ','20190606','27.0300','27.0100','27.2900','26.9200','27.1200','0.3300','142.670000','27.1300','交易');
INSERT INTO `wind_ashareeodprices_test` (`s_info_windcode`,`trade_dt`,`s_dq_preclose`,`s_dq_open`,`s_dq_high`,`s_dq_low`,`s_dq_close`,`s_sq_pctchange`,`s_dq_adjfactor`,`s_dq_avgprice`,`s_dq_tradestatus`) VALUES ('000002.SZ','20190610','27.1200','27.2900','28.0500','27.1700','27.8100','2.5400','142.670000','27.8000','交易');
INSERT INTO `wind_ashareeodprices_test` (`s_info_windcode`,`trade_dt`,`s_dq_preclose`,`s_dq_open`,`s_dq_high`,`s_dq_low`,`s_dq_close`,`s_sq_pctchange`,`s_dq_adjfactor`,`s_dq_avgprice`,`s_dq_tradestatus`) VALUES ('000004.SZ','20190603','21.5000','21.4100','22.1600','21.0000','22.1400','2.9800','4.060000','21.5100','交易');
INSERT INTO `wind_ashareeodprices_test` (`s_info_windcode`,`trade_dt`,`s_dq_preclose`,`s_dq_open`,`s_dq_high`,`s_dq_low`,`s_dq_close`,`s_sq_pctchange`,`s_dq_adjfactor`,`s_dq_avgprice`,`s_dq_tradestatus`) VALUES ('000004.SZ','20190604','22.1400','22.1400','22.1500','21.1300','21.2900','-3.8400','4.060000','21.4100','交易');
INSERT INTO `wind_ashareeodprices_test` (`s_info_windcode`,`trade_dt`,`s_dq_preclose`,`s_dq_open`,`s_dq_high`,`s_dq_low`,`s_dq_close`,`s_sq_pctchange`,`s_dq_adjfactor`,`s_dq_avgprice`,`s_dq_tradestatus`) VALUES ('000004.SZ','20190605','21.2900','21.4500','21.5400','21.1300','21.2000','-0.4200','4.060000','21.2700','交易');
INSERT INTO `wind_ashareeodprices_test` (`s_info_windcode`,`trade_dt`,`s_dq_preclose`,`s_dq_open`,`s_dq_high`,`s_dq_low`,`s_dq_close`,`s_sq_pctchange`,`s_dq_adjfactor`,`s_dq_avgprice`,`s_dq_tradestatus`) VALUES ('000004.SZ','20190606','21.2000','21.2000','21.3000','20.6100','20.7100','-2.3100','4.060000','20.8100','交易');
INSERT INTO `wind_ashareeodprices_test` (`s_info_windcode`,`trade_dt`,`s_dq_preclose`,`s_dq_open`,`s_dq_high`,`s_dq_low`,`s_dq_close`,`s_sq_pctchange`,`s_dq_adjfactor`,`s_dq_avgprice`,`s_dq_tradestatus`) VALUES ('000004.SZ','20190610','20.7100','20.0400','21.4000','20.0400','21.2000','2.3700','4.060000','21.0300','交易');
INSERT INTO `wind_ashareeodprices_test` (`s_info_windcode`,`trade_dt`,`s_dq_preclose`,`s_dq_open`,`s_dq_high`,`s_dq_low`,`s_dq_close`,`s_sq_pctchange`,`s_dq_adjfactor`,`s_dq_avgprice`,`s_dq_tradestatus`) VALUES ('000005.SZ','20190603','3.0300','3.0000','3.0400','2.9800','2.9900','-1.3200','9.270000','3.0000','交易');
INSERT INTO `wind_ashareeodprices_test` (`s_info_windcode`,`trade_dt`,`s_dq_preclose`,`s_dq_open`,`s_dq_high`,`s_dq_low`,`s_dq_close`,`s_sq_pctchange`,`s_dq_adjfactor`,`s_dq_avgprice`,`s_dq_tradestatus`) VALUES ('000005.SZ','20190604','2.9900','2.9900','3.0000','2.8300','2.8600','-4.3500','9.270000','2.9000','交易');
INSERT INTO `wind_ashareeodprices_test` (`s_info_windcode`,`trade_dt`,`s_dq_preclose`,`s_dq_open`,`s_dq_high`,`s_dq_low`,`s_dq_close`,`s_sq_pctchange`,`s_dq_adjfactor`,`s_dq_avgprice`,`s_dq_tradestatus`) VALUES ('000005.SZ','20190605','2.8600','2.8600','2.9100','2.8400','2.8500','-0.3500','9.270000','2.8800','交易');
INSERT INTO `wind_ashareeodprices_test` (`s_info_windcode`,`trade_dt`,`s_dq_preclose`,`s_dq_open`,`s_dq_high`,`s_dq_low`,`s_dq_close`,`s_sq_pctchange`,`s_dq_adjfactor`,`s_dq_avgprice`,`s_dq_tradestatus`) VALUES ('000005.SZ','20190606','2.8500','2.8600','2.8700','2.6800','2.7300','-4.2100','9.270000','2.7400','交易');
INSERT INTO `wind_ashareeodprices_test` (`s_info_windcode`,`trade_dt`,`s_dq_preclose`,`s_dq_open`,`s_dq_high`,`s_dq_low`,`s_dq_close`,`s_sq_pctchange`,`s_dq_adjfactor`,`s_dq_avgprice`,`s_dq_tradestatus`) VALUES ('000005.SZ','20190610','2.7300','2.7100','2.7500','2.7000','2.7300','0.0000','9.270000','2.7300','交易');
#INSERT INTO `wind_ashareeodprices_test` (`id`,`s_info_windcode`,`trade_dt`,`s_dq_preclose`,`s_dq_open`,`s_dq_high`,`s_dq_low`,`s_dq_close`,`s_sq_pctchange`,`s_dq_adjfactor`,`s_dq_avgprice`,`s_dq_tradestatus`) VALUES (39,'000001.SZ','20190604',NULL,'11.89','11.94','11.6',NULL,NULL,NULL,NULL,NULL);
#INSERT INTO `wind_ashareeodprices_test` (`id`,`s_info_windcode`,`trade_dt`,`s_dq_preclose`,`s_dq_open`,`s_dq_high`,`s_dq_low`,`s_dq_close`,`s_sq_pctchange`,`s_dq_adjfactor`,`s_dq_avgprice`,`s_dq_tradestatus`) VALUES (40,'000001.SZ','20190605',NULL,'11.97','12.14','11.92',NULL,NULL,NULL,NULL,NULL);

