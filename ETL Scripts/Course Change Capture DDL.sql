

use pacifica;

drop table if exists control_change_capture ; 

CREATE TABLE `control_change_capture` (
  `ID` int(11) NOT NULL AUTO_INCREMENT,
  `Table_Name` varchar(25) DEFAULT NULL,
  `Last_Extract_Datetime` datetime DEFAULT NULL ,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB AUTO_INCREMENT=10 DEFAULT CHARSET=utf8mb4;
