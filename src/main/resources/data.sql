use wind;
DROP TABLE IF EXISTS metadata;
DROP TABLE IF EXISTS metadatadetail;
DROP TABLE IF EXISTS task;

CREATE TABLE metadata
(
	id BIGINT(64) NOT NULL,
    tablename VARCHAR(30) NULL,
    func VARCHAR(5) NULL,
    updatetime VARCHAR(8) NULL,
    updateuser VARCHAR(20) NULL,
    PRIMARY KEY (id)
);

CREATE TABLE metadatadetail
(
	id BIGINT(64) NOT NULL,
    tablename VARCHAR(40) NULL,
    windcolumn VARCHAR(40) NULL,
    dbcolumn VARCHAR(40) NULL,
    usercolumn VARCHAR(40) NULL,
    PRIMARY KEY (id)
);

CREATE TABLE task
(
	id BIGINT(64) NOT NULL,
    username VARCHAR(20) NULL,
    tablename VARCHAR(40) NULL,
    description VARCHAR(100) NULL,
    PRIMARY KEY(id)
);

INSERT INTO metadata (id, tablename, func, updatetime, updateuser) VALUES
(1, "table1", "wsd", "20190601", "zzt1"),
(2, "table2", "wss", "20190602", "zzt2"),
(3, "table3", "wsd", "20190603", "zzt3");

INSERT INTO metadatadetail (id, tablename, windcolumn, dbcolumn, usercolumn) VALUES
(1, "table1", "windcolumn1", "dbcolumn1", "usercolumn1"),
(2, "table2", "windcolumn2", "dbcolumn2", "usercolumn2"),
(3, "table3", "windcolumn3", "dbcolumn3", "usercolumn3");

INSERT INTO task (id, username, tablename, description) VALUES
(1, "user1", "table1", "des1"),
(2, "user2", "table2", "des2"),
(3, "user3", "table3", "des3")
