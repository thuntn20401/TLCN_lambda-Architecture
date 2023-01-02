CREATE KEYSPACE IF NOT EXISTS stackoverflow WITH replication = {
      'class': 'SimpleStrategy', 'replication_factor': '1'
  };
USE stackoverflow;
CREATE TABLE userprofile ( id TEXT,  displayname TEXT, upvotes TEXT , downvotes TEXT , reputation TEXT ,PRIMARY KEY (id));
CREATE TABLE  toptags ( userid TEXT , tag TEXT , PRIMARY KEY (userid,tag));
CREATE TABLE usertoquestion ( userid 	TEXT , questions TEXT, PRIMARY KEY (userid,questions));


CREATE TABLE trendingtags  (updatedon TIMESTAMP, count INT , tagname TEXT , PRIMARY KEY (updatedon,count)) WITH CLUSTERING ORDER BY (count DESC) ;
CREATE TABLE realtimequestions  (tags TEXT, title TEXT , PRIMARY KEY (tags,title) );
CREATE TABLE realtimereputations (  id TEXT , reputation INT ,  PRIMARY KEY(id)) ;




