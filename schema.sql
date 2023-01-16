drop table if exists people;
drop table if exists cities;
drop table if exists counties;


create table `counties` (
  `id` int not null auto_increment,
  `name` varchar(80) default null,
  `country` varchar(80) default null,
  primary key (`id`)
);

create table `cities` (
  `id` int not null auto_increment,
  `name` varchar(80) default null,
  `county_id` int default null,
  primary key (`id`),
  foreign key (`county_id`) REFERENCES counties(`id`)
);

create table `people` (
  `id` int not null auto_increment,
  `given_name` varchar(80) default null,
  `family_name` varchar(80) default null,
  `date_of_birth` date default null,
  `place_of_birth_id` int default null,
  primary key (`id`),
  foreign key (`place_of_birth_id`) REFERENCES cities(`id`)
);

