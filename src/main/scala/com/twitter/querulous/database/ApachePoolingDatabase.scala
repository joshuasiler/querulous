package com.twitter.querulous.database
import com.twitter.querulous.database.Database

import java.sql.{SQLException, Connection}
import org.apache.commons.dbcp.{PoolableConnectionFactory, DriverManagerConnectionFactory, PoolingDataSource}
import org.apache.commons.pool.impl.{GenericObjectPool, StackKeyedObjectPoolFactory}
import com.twitter.xrayspecs.Duration

class ApachePoolingDatabaseFactory(
  minOpenConnections: Int,
  maxOpenConnections: Int,
  checkConnectionHealthWhenIdleFor: Duration,
  maxWaitForConnectionReservation: Duration,
  checkConnectionHealthOnReservation: Boolean,
  evictConnectionIfIdleFor: Duration) extends DatabaseFactory {

  def apply(dbhosts: List[String], dbname: String, username: String, password: String, urlOptions: Map[String, String]) = {
    val pool = new ApachePoolingDatabase(
      dbhosts,
      dbname,
      username,
      password,
      urlOptions,
      minOpenConnections,
      maxOpenConnections,
      checkConnectionHealthWhenIdleFor,
      maxWaitForConnectionReservation,
      checkConnectionHealthOnReservation,
      evictConnectionIfIdleFor)
    pool
  }
}

class ApachePoolingDatabase(
  dbhosts: List[String],
  dbname: String,
  username: String,
  password: String,
  urlOptions: Map[String, String],
  minOpenConnections: Int,
  maxOpenConnections: Int,
  checkConnectionHealthWhenIdleFor: Duration,
  maxWaitForConnectionReservation: Duration,
  checkConnectionHealthOnReservation: Boolean,
  evictConnectionIfIdleFor: Duration) extends Database {

  Class.forName("com.mysql.jdbc.Driver")

  private val config = new GenericObjectPool.Config
  config.maxActive = maxOpenConnections
  config.maxIdle = maxOpenConnections
  config.minIdle = minOpenConnections
  config.maxWait = maxWaitForConnectionReservation.inMillis

  config.timeBetweenEvictionRunsMillis = checkConnectionHealthWhenIdleFor.inMillis
  config.testWhileIdle = false
  config.testOnBorrow = checkConnectionHealthOnReservation
  config.minEvictableIdleTimeMillis = evictConnectionIfIdleFor.inMillis

  //config.lifo = false

  private val connectionPool = new GenericObjectPool(null, config)
  private val connectionFactory = new DriverManagerConnectionFactory(url(dbhosts, dbname, urlOptions), username, password)
  private val poolableConnectionFactory = new PoolableConnectionFactory(
    connectionFactory,
    connectionPool,
    null,
    "/* ping */ SELECT 1",
    false,
    true)
  private val poolingDataSource = new PoolingDataSource(connectionPool)
  poolingDataSource.setAccessToUnderlyingConnectionAllowed(true)

  def close(connection: Connection) {
    try {
      connection.close()
    } catch {
      case _: SQLException =>
    }
  }

  def open() = poolingDataSource.getConnection()

  override def toString = dbhosts.first + "_" + dbname
}
