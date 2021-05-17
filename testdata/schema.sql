CREATE TABLE users
(
    id         varchar(128) NOT NULL,
    email      varchar(128) NOT NULL,
    created_at datetime     NOT NULL,
    updated_at datetime     NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY users_email_uindex (email)
) ENGINE = InnoDB
DEFAULT CHARSET = utf8mb4;

CREATE TABLE categories
(
    id         varchar(128) NOT NULL,
    name       varchar(128) NOT NULL,
    created_at datetime     NOT NULL,
    updated_at datetime     NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY categories_name_uindex (name)
) ENGINE = InnoDB
DEFAULT CHARSET = utf8mb4;

INSERT INTO categories
VALUES ('C-TEST1', 'Alpha', '2021-01-01 00:00:00', '2021-01-01 00:00:00'),
       ('C-TEST2', 'Beta', '2021-01-01 00:00:00', '2021-01-01 00:00:00'),
       ('C-TEST3', 'Delta', '2021-01-01 00:00:00', '2021-01-01 00:00:00'),
       ('C-TEST4', 'Epsilon', '2021-01-01 00:00:00', '2021-01-01 00:00:00'),
       ('C-TEST5', 'Gamme', '2021-01-01 00:00:00', '2021-01-01 00:00:00');
