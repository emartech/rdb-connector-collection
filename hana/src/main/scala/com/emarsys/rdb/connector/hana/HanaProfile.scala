package com.emarsys.rdb.connector.hana

import slick.jdbc.JdbcProfile

trait HanaProfile extends JdbcProfile { }

object HanaProfile extends HanaProfile
