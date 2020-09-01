package com.emarsys.rdb.connector.snowflake

import slick.jdbc.JdbcProfile

trait SnowflakeProfile extends JdbcProfile { }

object SnowflakeProfile extends SnowflakeProfile