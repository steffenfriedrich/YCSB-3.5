USE YCSB
GO
CREATE SCHEMA bookStore;
GO
CREATE TABLE bookStore.Users (
	[Id] INT IDENTITY(1,1) NOT NULL,
	[Name] varchar(50) NOT NULL,
	[BirthDate] DATE NOT NULL
	CONSTRAINT pk_UserId PRIMARY KEY (Id)
)
GO
CREATE TABLE bookStore.Authors (
	[Id] int IDENTITY(1,1) NOT NULL,
	[Name] varchar(50) NOT NULL,
	[BirthDate] DATE NOT NULL,
	[Gender] varchar(50),
    [Resume] varchar (200),
	CONSTRAINT pk_AuthorId PRIMARY KEY (Id)
)
GO
CREATE TABLE bookStore.Books (
	[Id] int IDENTITY(1,1) NOT NULL,
	[Title] varchar(500) NOT NULL,
	[Language] varchar(500),
	[Resume] varchar(500),
	[Genres] varchar (500),
	[Authors] varchar (500) ,
	CONSTRAINT pk_BookId PRIMARY KEY (Id)
)
GO
CREATE TABLE bookStore.Recommendations (
	[InsertOrder] int IDENTITY(1,1) NOT NULL,
	[UserId] int  NOT NULL,
	[BookId] int  NOT NULL,
	[CreateDate] DATE NOT NUll,
    [Text] varchar (200),
	[Stars] int,
	[Likes] int,
	CONSTRAINT pk_RecommendationID PRIMARY KEY (UserId,BookId),
	CONSTRAINT fk_Recommendation_Book FOREIGN KEY (BookId)     
		REFERENCES bookStore.Books (Id)  
			ON UPDATE CASCADE  ,
	CONSTRAINT fk_Recommendation_User FOREIGN KEY (UserId)     
		REFERENCES bookStore.Users (Id)           
			ON UPDATE CASCADE  
)
GO


select * from bookStore.Recommendations ORDER BY bookStore.Recommendations.InsertOrder asc
select * from bookStore.Users
select * from bookStore.Books
select * from bookStore.Authors 




DROP TABLE bookStore.Recommendations;
DROP TABLE bookStore.Users;
DROP TABLE  bookStore.Books;
DROP TABLE bookStore.Authors ;




CREATE TABLE bookStore.Genres (
	Name varchar(7) CHECK(Name='comedy'OR Name='drama' OR Name='horror' OR Name='action' OR Name='novel'OR Name='romance') NOT NULL,
	BookID int NOT NULL,
	CONSTRAINT pk_Genre PRIMARY KEY (Name,BookID)
	)
GO
CREATE TABLE bookStore.MapAuthorsBooks(
	AuthorID int NOT NULL, 
	BookID int NOT NULL
)
GO
INSERT INTO bookStore.Users (Id,Name,BirthDate)
	VALUES (9539999,'Trol','1999-12-12')
GO
INSERT INTO bookStore.Authors(Id,Name,BirthDate,Gender,[Resume])
	VALUES (76955756,'Hans','1999-12-12','male','hallo ich bin der beste Author')
GO
INSERT INTO bookStore.Books
	VALUES ('Buchtitel','Italian','horror action','hallo ich bin der beste Author und das ist die beste buchbeschreibung zum Buch')
INSERT INTO bookStore.Genres
	VALUES ('drama',2)
GO
INSERT INTO bookStore.Recommendations (UserId,BookId,CreateDate,[Text],Stars,Likes)
	VALUES (3,1,'20120618 10:34:09 AM','hihihi ich mag es',5,22)
GO










delete  from bookStore.Authors 

-- funktioniert obwphl unterschtrichen 
SELECT TOP(2)* FROM bookStore.Recommendations
	where BookId=1
	order by bookStore.Recommendations.InsertOrder desc

-- mach ein schleifne string builder 
SELECT * FROM bookStore.Books WHERE CONTAINS(Genres, 'action AND drama ');



-- wichtig  full text search  einschalten 
use YCSB
 create fulltext catalog FullTextCatalog as default
 --nicht vergessen CREATE index search auf tabele  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!--

 select *
 from sys.fulltext_catalogs

 create fulltext index on bookStore.Books(Genres)
 key index pk_BookId

 drop Table bookStore.Users

CREATE LOGIN Arkadi   
    WITH PASSWORD = 'Arkadi';  
USE YCSB;  
GO  
CREATE USER Arkadi FOR LOGIN Arkadi;  
GO   
--nicht vergessen CREATE index search auf tabele  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!--

SET IDENTITY_INSERT bookStore.Users OFF