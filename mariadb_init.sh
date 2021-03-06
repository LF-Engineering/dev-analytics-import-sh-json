#!/bin/bash
if ( [ -z "$PASS" ] || [ -z "$USR" ] || [ -z "$SH_PASS" ] || [ -z "$SH_USR" ] || [ -z "$SH_DB" ] )
then
  echo "$0: please specify MariaDB root user via USR=..."
  echo "$0: please specify MariaDB root password via PASS=..."
  echo "$0: please specify MariaDB Sorting Hat user via SH_USR=..."
  echo "$0: please specify MariaDB Sorting Hat user via SH_PASS=..."
  echo "$0: please specify MariaDB Sorting Hat database via SH_DB=..."
  exit 1
fi

#echo "create user '$SH_USR'@localhost identified by '$SH_PASS';" | mysql -h127.0.0.1 -P13306 -p"${PASS}" -u"${USR}"
echo "create user '$SH_USR'@'%' identified by '$SH_PASS';" | mysql -h127.0.0.1 -P13306 -p"${PASS}" -u"${USR}"
echo "create database ``${SH_DB}``;" | mysql -h127.0.0.1 -P13306 -p"${PASS}" -u"${USR}"
echo "grant all privileges on ``${SH_DB}``.* to '$SH_USR'@'%';" | mysql -h127.0.0.1 -P13306 -p"${PASS}" -u"${USR}"
echo "grant all privileges on ``${SH_DB}``.* to '$SH_USR'@localhost;" | mysql -h127.0.0.1 -P13306 -p"${PASS}" -u"${USR}"
echo "flush privileges;" | mysql -h127.0.0.1 -P13306 -p"${PASS}" -u"${USR}"
if [ -z "$FULL" ]
then
  mysql -h127.0.0.1 -P13306 -p"${PASS}" -u"${USR}" "${SH_DB}" < structure.sql
else
  mysql -h127.0.0.1 -P13306 -p"${PASS}" -u"${USR}" "${SH_DB}" < sortinghat.sql
fi
echo "show databases;" | mysql -h127.0.0.1 -P13306 -p"${SH_PASS}" -u"${SH_USR}" "${SH_DB}"
