/*M!999999\- enable the sandbox mode */ 
-- MariaDB dump 10.19-12.2.2-MariaDB, for Linux (x86_64)
--
-- Host: localhost    Database: nasa_analytics
-- ------------------------------------------------------
-- Server version	12.2.2-MariaDB

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*M!100616 SET @OLD_NOTE_VERBOSITY=@@NOTE_VERBOSITY, NOTE_VERBOSITY=0 */;

--
-- Table structure for table `execution_metadata`
--

DROP TABLE IF EXISTS `execution_metadata`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `execution_metadata` (
  `run_id` varchar(50) NOT NULL,
  `pipeline_name` varchar(20) DEFAULT NULL,
  `batch_id` int(11) DEFAULT NULL,
  `batch_size` int(11) DEFAULT NULL,
  `avg_batch_size` float DEFAULT NULL,
  `runtime_ms` bigint(20) DEFAULT NULL,
  `execution_time` timestamp NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`run_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `execution_metadata`
--

SET @OLD_AUTOCOMMIT=@@AUTOCOMMIT, @@AUTOCOMMIT=0;
LOCK TABLES `execution_metadata` WRITE;
/*!40000 ALTER TABLE `execution_metadata` DISABLE KEYS */;
/*!40000 ALTER TABLE `execution_metadata` ENABLE KEYS */;
UNLOCK TABLES;
COMMIT;
SET AUTOCOMMIT=@OLD_AUTOCOMMIT;

--
-- Table structure for table `q1_daily_traffic`
--

DROP TABLE IF EXISTS `q1_daily_traffic`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `q1_daily_traffic` (
  `run_id` varchar(50) DEFAULT NULL,
  `log_date` varchar(15) DEFAULT NULL,
  `status_code` int(11) DEFAULT NULL,
  `request_count` bigint(20) DEFAULT NULL,
  `total_bytes` bigint(20) DEFAULT NULL,
  KEY `run_id` (`run_id`),
  CONSTRAINT `1` FOREIGN KEY (`run_id`) REFERENCES `execution_metadata` (`run_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `q1_daily_traffic`
--

SET @OLD_AUTOCOMMIT=@@AUTOCOMMIT, @@AUTOCOMMIT=0;
LOCK TABLES `q1_daily_traffic` WRITE;
/*!40000 ALTER TABLE `q1_daily_traffic` DISABLE KEYS */;
/*!40000 ALTER TABLE `q1_daily_traffic` ENABLE KEYS */;
UNLOCK TABLES;
COMMIT;
SET AUTOCOMMIT=@OLD_AUTOCOMMIT;

--
-- Table structure for table `q2_top_resources`
--

DROP TABLE IF EXISTS `q2_top_resources`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `q2_top_resources` (
  `run_id` varchar(50) DEFAULT NULL,
  `resource_path` varchar(500) DEFAULT NULL,
  `request_count` bigint(20) DEFAULT NULL,
  `total_bytes` bigint(20) DEFAULT NULL,
  `distinct_host_count` bigint(20) DEFAULT NULL,
  KEY `run_id` (`run_id`),
  CONSTRAINT `1` FOREIGN KEY (`run_id`) REFERENCES `execution_metadata` (`run_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `q2_top_resources`
--

SET @OLD_AUTOCOMMIT=@@AUTOCOMMIT, @@AUTOCOMMIT=0;
LOCK TABLES `q2_top_resources` WRITE;
/*!40000 ALTER TABLE `q2_top_resources` DISABLE KEYS */;
/*!40000 ALTER TABLE `q2_top_resources` ENABLE KEYS */;
UNLOCK TABLES;
COMMIT;
SET AUTOCOMMIT=@OLD_AUTOCOMMIT;

--
-- Table structure for table `q3_hourly_errors`
--

DROP TABLE IF EXISTS `q3_hourly_errors`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `q3_hourly_errors` (
  `run_id` varchar(50) DEFAULT NULL,
  `log_date` varchar(20) DEFAULT NULL,
  `log_hour` varchar(5) DEFAULT NULL,
  `error_request_count` bigint(20) DEFAULT NULL,
  `total_request_count` bigint(20) DEFAULT NULL,
  `error_rate` float DEFAULT NULL,
  `distinct_error_hosts` bigint(20) DEFAULT NULL,
  KEY `run_id` (`run_id`),
  CONSTRAINT `1` FOREIGN KEY (`run_id`) REFERENCES `execution_metadata` (`run_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `q3_hourly_errors`
--

SET @OLD_AUTOCOMMIT=@@AUTOCOMMIT, @@AUTOCOMMIT=0;
LOCK TABLES `q3_hourly_errors` WRITE;
/*!40000 ALTER TABLE `q3_hourly_errors` DISABLE KEYS */;
/*!40000 ALTER TABLE `q3_hourly_errors` ENABLE KEYS */;
UNLOCK TABLES;
COMMIT;
SET AUTOCOMMIT=@OLD_AUTOCOMMIT;

--
-- Table structure for table `query_metrics`
--

DROP TABLE IF EXISTS `query_metrics`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `query_metrics` (
  `run_id` varchar(50) DEFAULT NULL,
  `query_name` varchar(100) DEFAULT NULL,
  `runtime_ms` bigint(20) DEFAULT NULL,
  KEY `run_id` (`run_id`),
  CONSTRAINT `1` FOREIGN KEY (`run_id`) REFERENCES `execution_metadata` (`run_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `query_metrics`
--

SET @OLD_AUTOCOMMIT=@@AUTOCOMMIT, @@AUTOCOMMIT=0;
LOCK TABLES `query_metrics` WRITE;
/*!40000 ALTER TABLE `query_metrics` DISABLE KEYS */;
/*!40000 ALTER TABLE `query_metrics` ENABLE KEYS */;
UNLOCK TABLES;
COMMIT;
SET AUTOCOMMIT=@OLD_AUTOCOMMIT;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*M!100616 SET NOTE_VERBOSITY=@OLD_NOTE_VERBOSITY */;

-- Dump completed on 2026-04-24  2:43:18
