-- MariaDB dump 10.17  Distrib 10.4.6-MariaDB, for debian-linux-gnu (x86_64)
--
-- Host: dev-analytics-sortinghat-dev.ch92vofnmy4d.us-west-2.rds.amazonaws.com    Database: sortinghat
-- ------------------------------------------------------
-- Server version	10.3.8-MariaDB-log

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `countries`
--

DROP TABLE IF EXISTS `countries`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `countries` (
  `code` varchar(2) COLLATE utf8mb4_unicode_520_ci NOT NULL,
  `name` varchar(191) COLLATE utf8mb4_unicode_520_ci NOT NULL,
  `alpha3` varchar(3) COLLATE utf8mb4_unicode_520_ci NOT NULL,
  PRIMARY KEY (`code`),
  UNIQUE KEY `_alpha_unique` (`alpha3`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `domains_organizations`
--

DROP TABLE IF EXISTS `domains_organizations`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `domains_organizations` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `domain` varchar(128) COLLATE utf8mb4_unicode_520_ci NOT NULL,
  `is_top_domain` tinyint(1) DEFAULT NULL,
  `organization_id` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `_domain_unique` (`domain`),
  KEY `organization_id` (`organization_id`),
  CONSTRAINT `domains_organizations_ibfk_1` FOREIGN KEY (`organization_id`) REFERENCES `organizations` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `top_domain_check` CHECK (`is_top_domain` in (0,1))
) ENGINE=InnoDB AUTO_INCREMENT=1817 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `enrollments`
--

DROP TABLE IF EXISTS `enrollments`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `enrollments` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `start` datetime NOT NULL,
  `end` datetime NOT NULL,
  `uuid` varchar(128) COLLATE utf8mb4_unicode_520_ci NOT NULL,
  `organization_id` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `_period_unique` (`uuid`,`organization_id`,`start`,`end`),
  KEY `organization_id` (`organization_id`),
  CONSTRAINT `enrollments_ibfk_1` FOREIGN KEY (`uuid`) REFERENCES `uidentities` (`uuid`) ON DELETE CASCADE,
  CONSTRAINT `enrollments_ibfk_2` FOREIGN KEY (`organization_id`) REFERENCES `organizations` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=5347 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `identities`
--

DROP TABLE IF EXISTS `identities`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `identities` (
  `id` varchar(128) COLLATE utf8mb4_unicode_520_ci NOT NULL,
  `name` varchar(128) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `email` varchar(128) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `username` varchar(128) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `source` varchar(32) COLLATE utf8mb4_unicode_520_ci NOT NULL,
  `uuid` varchar(128) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `last_modified` datetime(6) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `_identity_unique` (`name`,`email`,`username`,`source`),
  KEY `uuid` (`uuid`),
  CONSTRAINT `identities_ibfk_1` FOREIGN KEY (`uuid`) REFERENCES `uidentities` (`uuid`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `matching_blacklist`
--

DROP TABLE IF EXISTS `matching_blacklist`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `matching_blacklist` (
  `excluded` varchar(128) COLLATE utf8mb4_unicode_520_ci NOT NULL,
  PRIMARY KEY (`excluded`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `organizations`
--

DROP TABLE IF EXISTS `organizations`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `organizations` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(191) COLLATE utf8mb4_unicode_520_ci NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `_name_unique` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=21725 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `profiles`
--

DROP TABLE IF EXISTS `profiles`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `profiles` (
  `uuid` varchar(128) COLLATE utf8mb4_unicode_520_ci NOT NULL,
  `name` varchar(128) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `email` varchar(128) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `gender` varchar(32) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `gender_acc` int(11) DEFAULT NULL,
  `is_bot` tinyint(1) DEFAULT NULL,
  `country_code` varchar(2) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  PRIMARY KEY (`uuid`),
  KEY `country_code` (`country_code`),
  CONSTRAINT `profiles_ibfk_1` FOREIGN KEY (`uuid`) REFERENCES `uidentities` (`uuid`) ON DELETE CASCADE,
  CONSTRAINT `profiles_ibfk_2` FOREIGN KEY (`country_code`) REFERENCES `countries` (`code`) ON DELETE CASCADE,
  CONSTRAINT `is_bot_check` CHECK (`is_bot` in (0,1))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `uidentities`
--

DROP TABLE IF EXISTS `uidentities`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `uidentities` (
  `uuid` varchar(128) COLLATE utf8mb4_unicode_520_ci NOT NULL,
  `last_modified` datetime(6) DEFAULT NULL,
  PRIMARY KEY (`uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2019-07-18  6:06:46
