#MYSQLDUMP
mysqldump --host=127.0.0.1 --port=3306 --skip-extended-insert --compact -u root db_name > dump.sql

# Replace Auto_Increment
cat test.sql | sed -e 's/ AUTO_INCREMENT,/,/g' > no_auto_inc.sql

# Import into sqlite3
./mysql2sqlite no_auto_inc.sql | sqlite3 vmp.db

# Name
A protective deity of Egypt's southern border with Nubia, she came to personify the former annual flooding of the Nile and to serve as a war, hunting, and fertility goddess.

# SQL Mode setting
Inject the following
```
SET sql_mode = '';
SET GLOBAL sql_mode = '';
```


TODO

docker run -p 3306:3306 --name temp-mysql -e MYSQL_ALLOW_EMPTY_PASSWORD=yes mysql:latest --defaul
t-authentication-plugin=mysql_native_password

CREATE DATABASE vpm_temp



