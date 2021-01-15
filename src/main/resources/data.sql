use wind;
DROP TABLE IF EXISTS metadata;
DROP TABLE IF EXISTS metadatadetail;

CREATE TABLE metadata
(
	id BIGINT(64) NOT NULL,
    tablename VARCHAR(30) NULL,
    func VARCHAR(5) NULL,
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

INSERT INTO metadata (id, tablename, func) VALUES
(1, "table1", "wsd"),
(2, "table2", "wss"),
(3, "table3", "wsd");

INSERT INTO metadatadetail (id, tablename, windcolumn, dbcolumn, usercolumn) VALUES
(1, "table1", "windcolumn1", "dbcolumn1", "usercolumn1"),
(2, "table2", "windcolumn2", "dbcolumn2", "usercolumn2"),
(3, "table3", "windcolumn3", "dbcolumn3", "usercolumn3");

INSERT INTO description (id, username, tablename, description) VALUES
(1, "user1", "table1", "des1"),
(2, "user2", "table2", "des2"),
(3, "user3", "table3", "des3")
