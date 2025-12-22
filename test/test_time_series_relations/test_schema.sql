PRAGMA user_version = 1;

CREATE TABLE Configuration (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    value1 REAL NOT NULL DEFAULT 100
) STRICT;

CREATE TABLE Resource (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    label TEXT UNIQUE NOT NULL
) STRICT;

CREATE TABLE Plant (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    label TEXT UNIQUE NOT NULL
) STRICT;

CREATE TABLE Resource_time_series_generation (
    id INTEGER,
    date_time TEXT NOT NULL,
    power REAL,
    plant_id INTEGER,
    FOREIGN KEY(id) REFERENCES Resource(id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY(plant_id) REFERENCES Plant(id) ON DELETE SET NULL ON UPDATE CASCADE,
    PRIMARY KEY (id, date_time)
) STRICT;

CREATE TABLE Resource_time_series_dispatch (
    id INTEGER,
    date_time TEXT NOT NULL,
    block INTEGER NOT NULL,
    scenario INTEGER NOT NULL,
    energy REAL,
    plant_dispatch_id INTEGER,
    FOREIGN KEY(id) REFERENCES Resource(id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY(plant_dispatch_id) REFERENCES Plant(id) ON DELETE SET NULL ON UPDATE CASCADE,
    PRIMARY KEY (id, date_time, block, scenario)
) STRICT;
