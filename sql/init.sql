CREATE TABLE anime(
    id SERIAL   PRIMARY KEY,
    title       VARCHAR(255),
    type        VARCHAR(255),
    status      VARCHAR(255),
    startDate   DATE,
    endDate     DATE,
    episodes    INTEGER,
    rating      DECIMAL
);
