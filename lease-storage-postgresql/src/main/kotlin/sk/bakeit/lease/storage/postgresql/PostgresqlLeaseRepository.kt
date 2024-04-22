package sk.bakeit.lease.storage.postgresql

import sk.bakeit.lease.api.Lease
import sk.bakeit.lease.api.LeaseAcquiringFailed
import sk.bakeit.lease.api.LeaseNotFound
import sk.bakeit.lease.api.LeaseRepository
import sk.bakeit.lease.api.StaleLease
import sk.bakeit.lease.storage.postgresql.jdbc.TransactionManager
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.Instant
import javax.sql.DataSource

/**
 *
 * Private API. It should not be used directly by the client code.
 *
 * This implementation of the `LeaseRepository` uses Postgresql database to manage leases.
 *
 * @param dataSource the datasource providing connection to the postgresql database.
 */
internal class PostgresqlLeaseRepository(
    dataSource: DataSource,
) : LeaseRepository {

    companion object {
        private const val TABLE_LEASE = "lease" // TODO make table name configurable
        private const val COLUMN_NAME = "name"
        private const val COLUMN_VERSION = "version"
        private const val COLUMN_ACQUIRED_AT = "acquired_at"
        private const val COLUMN_RENEWED_AT = "renewed_at"
        private const val COLUMN_TIMEOUT = "timeout"
        private const val COLUMN_HOLDER_NAME = "holder_name"
        private const val COLUMN_EXPIRY_DATETIME = "expiry_datetime"
        private const val FULL_COLUMN_LIST = "$COLUMN_NAME, $COLUMN_VERSION, $COLUMN_ACQUIRED_AT," +
                " $COLUMN_RENEWED_AT, $COLUMN_TIMEOUT, $COLUMN_HOLDER_NAME, $COLUMN_EXPIRY_DATETIME"
    }

    private val transactionManager = TransactionManager { dataSource.connection }

    override fun findLease(leaseName: String): Lease? {
        return transactionManager.doInTransaction { connection ->
            val statement = connection.prepareStatement(
                """SELECT $FULL_COLUMN_LIST
               FROM $TABLE_LEASE WHERE $COLUMN_NAME = ?""".trimIndent()
            )

            statement.use {
                it.setString(1, leaseName)
                toAtMostOneLease(it.executeQuery())
            }
        }
    }

    override fun acquireLease(leaseName: String, owner: String, timeout: Long): Lease {
        return transactionManager.doInTransaction { connection ->
            val statement = connection.prepareStatement(
                """INSERT INTO $TABLE_LEASE($FULL_COLUMN_LIST)
                   VALUES(?, 1, ?, ?, ?, ?, ?)
                   ON CONFLICT ($COLUMN_NAME) DO UPDATE
                       SET $COLUMN_VERSION = $TABLE_LEASE.$COLUMN_VERSION + 1,
                           $COLUMN_RENEWED_AT = EXCLUDED.$COLUMN_RENEWED_AT,
                           $COLUMN_TIMEOUT = EXCLUDED.$COLUMN_TIMEOUT,
                           $COLUMN_HOLDER_NAME = EXCLUDED.$COLUMN_HOLDER_NAME,
                           $COLUMN_EXPIRY_DATETIME = EXCLUDED.$COLUMN_EXPIRY_DATETIME
                       WHERE $TABLE_LEASE.$COLUMN_EXPIRY_DATETIME < ?
                   RETURNING $FULL_COLUMN_LIST
                """.trimIndent()
            )

            val instant = Instant.now()
            var index = 0

            statement.use {
                it.setString(++index, leaseName)
                it.setTimestamp(++index, Timestamp.from(instant))
                it.setTimestamp(++index, Timestamp.from(instant))
                it.setLong(++index, timeout)
                it.setString(++index, owner)
                it.setTimestamp(++index, Timestamp.from(instant.plusMillis(timeout)))
                it.setTimestamp(++index, Timestamp.from(instant))

                statement.execute()
                toAtMostOneLease(statement.resultSet)
                    ?: throw LeaseAcquiringFailed("A valid lease '$leaseName' already exists.")
            }
        }
    }

    internal fun renewLease(lease: Lease): Lease {
        if (lease !is PostgresqlLease) {
            throw IllegalArgumentException(
                "Unsupported Lease implementation - ${lease.javaClass}." +
                        " ${PostgresqlLease::class.qualifiedName} expected."
            )
        }

        return transactionManager.doInTransaction { connection ->
            val statement = connection.prepareStatement(
                """
                UPDATE $TABLE_LEASE SET 
                    $COLUMN_VERSION = $COLUMN_VERSION + 1,
                    $COLUMN_RENEWED_AT = ?,
                    $COLUMN_EXPIRY_DATETIME = (?::TIMESTAMP WITH TIME ZONE + make_interval(secs => $COLUMN_TIMEOUT / 1000) )
                WHERE $COLUMN_NAME = ? AND $COLUMN_VERSION = ?
                RETURNING $FULL_COLUMN_LIST
            """.trimIndent()
            )

            statement.use {
                var index = 0
                val instant = Instant.now()
                it.setTimestamp(++index, Timestamp.from(instant))
                it.setTimestamp(++index, Timestamp.from(instant))
                it.setString(++index, lease.name)
                it.setLong(++index, lease.version)

                it.execute()
                toAtMostOneLease(it.resultSet) ?: run {
                    detectStaleData(lease.name, lease.version, connection)
                    throw LeaseNotFound(lease.name)
                }
            }
        }
    }

    override fun removeLease(lease: Lease) {
        transactionManager.doInTransaction { connection ->
            val statement = connection.prepareStatement("DELETE FROM lease WHERE name = ?")
            statement.use {
                it.setString(1, lease.name)
                val affectedRows: Int = it.executeUpdate()

                if (affectedRows == 0) {
                    throw LeaseNotFound(lease.name)
                } else if (affectedRows > 1) {
                    throw IllegalStateException("Multiple leases with the name '${lease.name}' deleted.")
                }
            }
        }
    }

    private fun detectStaleData(leaseName: String, expectedVersion: Long, connection: Connection) {
        val statement =  connection.prepareStatement(
                "SELECT $COLUMN_VERSION FROM $TABLE_LEASE WHERE $COLUMN_NAME = ?"
            )
        statement.use {
            statement.setString(1, leaseName)
            val resultSet = it.executeQuery()

            if (resultSet.next()) {
                val actualVersion = resultSet.getLong(1)

                if (actualVersion != expectedVersion) {
                    throw StaleLease(
                        "Stale data detected for the lease '$leaseName'." +
                            " Stale version $expectedVersion presented" +
                            " but actual version was $actualVersion.")
                }
            }
        }
    }

    private fun toAtMostOneLease(resultSet: ResultSet): PostgresqlLease? {
        val isEmpty = !resultSet.next()

        return if (isEmpty) {
            null
        } else {
            val name = resultSet.getString(COLUMN_NAME)
            val version = resultSet.getLong(COLUMN_VERSION)
            val acquiredAt = resultSet.getTimestamp(COLUMN_ACQUIRED_AT).toInstant()
            val renewedAt = resultSet.getTimestamp(COLUMN_RENEWED_AT).toInstant()
            val timeout = resultSet.getLong(COLUMN_TIMEOUT)
            val holderName = resultSet.getString(COLUMN_HOLDER_NAME)

            if (resultSet.next()) {
                throw IllegalStateException("More than one lease found for the name '$name'")
            }

            PostgresqlLease(this, name, version, acquiredAt, renewedAt, timeout, holderName)
        }
    }
}