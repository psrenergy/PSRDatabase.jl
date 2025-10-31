PRAGMA user_version = 1;

CREATE TABLE Configuration (
    id INTEGER PRIMARY KEY AUTOINCREMENT
);

CREATE TABLE MyTable (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    label TEXT NOT NULL UNIQUE
);

CREATE TABLE MyTable_time_series_parameters (
    id INTEGER, 
    date_time TEXT NOT NULL,
    my_parameter INTEGER,
    FOREIGN KEY(id) REFERENCES MyTable(id) ON DELETE CASCADE ON UPDATE CASCADE,
    PRIMARY KEY (id, date_time)
);