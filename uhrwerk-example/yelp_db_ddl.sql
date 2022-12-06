create table yelp_db.attribute
(
    id          int auto_increment
        primary key,
    business_id varchar(22)  not null,
    name        varchar(255) null,
    value       mediumtext   null
);

create table yelp_db.business
(
    id           varchar(22)  not null
        primary key,
    name         varchar(255) null,
    neighborhood varchar(255) null,
    address      varchar(255) null,
    city         varchar(255) null,
    state        varchar(255) null,
    postal_code  varchar(255) null,
    latitude     float        null,
    longitude    float        null,
    stars        float        null,
    review_count int          null,
    is_open      tinyint(1)   null
);

create table yelp_db.category
(
    id          int auto_increment
        primary key,
    business_id varchar(22)  not null,
    category    varchar(255) null
);

create table yelp_db.checkin
(
    id          int auto_increment
        primary key,
    business_id varchar(22)  not null,
    date        varchar(255) null,
    count       int          null
);

create table yelp_db.elite_years
(
    id      int auto_increment
        primary key,
    user_id varchar(22) not null,
    year    char(4)     not null
);

create table yelp_db.friend
(
    id        int auto_increment
        primary key,
    user_id   varchar(22) not null,
    friend_id varchar(22) not null
);

create table yelp_db.hours
(
    id          int auto_increment
        primary key,
    business_id varchar(22)  not null,
    hours       varchar(255) null
);

create table yelp_db.photo
(
    id          varchar(22)  not null
        primary key,
    business_id varchar(22)  not null,
    caption     varchar(255) null,
    label       varchar(255) null
);

create table yelp_db.review
(
    id          varchar(22) not null
        primary key,
    business_id varchar(22) not null,
    user_id     varchar(22) not null,
    stars       int         null,
    date        datetime    null,
    text        mediumtext  null,
    useful      int         null,
    funny       int         null,
    cool        int         null
);

create table yelp_db.tip
(
    id          int auto_increment
        primary key,
    user_id     varchar(22) not null,
    business_id varchar(22) not null,
    text        mediumtext  null,
    date        datetime    null,
    likes       int         null
);

create table yelp_db.user
(
    id                 varchar(22)  not null
        primary key,
    name               varchar(255) null,
    review_count       int          null,
    yelping_since      datetime     null,
    useful             int          null,
    funny              int          null,
    cool               int          null,
    fans               int          null,
    average_stars      float        null,
    compliment_hot     int          null,
    compliment_more    int          null,
    compliment_profile int          null,
    compliment_cute    int          null,
    compliment_list    int          null,
    compliment_note    int          null,
    compliment_plain   int          null,
    compliment_cool    int          null,
    compliment_funny   int          null,
    compliment_writer  int          null,
    compliment_photos  int          null
);

