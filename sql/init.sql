CREATE TABLE anime(
    id SERIAL   PRIMARY KEY,
    title       VARCHAR(255),
    startDate   DATE,
    endDate     DATE,
    episodes    INTEGER,
    rating      DECIMAL
);
