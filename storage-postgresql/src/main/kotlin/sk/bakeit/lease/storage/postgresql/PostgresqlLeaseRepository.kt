package sk.bakeit.lease.storage.postgresql

import org.postgresql.util.PSQLException
import org.postgresql.util.PSQLState
import sk.bakeit.lease.api.Lease
import sk.bakeit.lease.api.LeaseAcquiringFailed
import sk.bakeit.lease.api.LeaseAlreadyExists
import sk.bakeit.lease.api.LeaseNotFound
import sk.bakeit.lease.api.LeaseRepository
import sk.bakeit.lease.storage.postgresql.tx.Transaction
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.Instant
import javax.sql.DataSource

/**
 * This implementation of the `LeaseRepository` uses Postgresql database to manage leases.
 *
 * @param dataSource the datasource providing connection to the postgresql database.
 */
class PostgresqlLeaseRepository(
    private val dataSource: DataSource,
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

    override fun findLease(leaseName: String): Lease? {
        return doInTransaction { connection ->
            val statement = connection.prepareStatement(
                """SELECT $FULL_COLUMN_LIST
               FROM $TABLE_LEASE WHERE $COLUMN_NAME = ?""".trimIndent()
            )

            statement.use {
                it.setString(1, leaseName)
                it.executeQuery().toAtMostOneLease()
            }
        }
    }

    override fun createLease(leaseName: String, owner: String, timeout: Long): Lease {
        return doInTransaction { connection ->
            val statement = connection.prepareStatement(
                """INSERT INTO $TABLE_LEASE($FULL_COLUMN_LIST)
                   VALUES(?, 1, ?, ?, ?, ?, ?)
                   RETURNING $FULL_COLUMN_LIST
                """.trimIndent()
            )

            val instant = Instant.now()
            statement.use {
                it.setString(1, leaseName)
                it.setTimestamp(2, Timestamp.from(instant))
                it.setTimestamp(3, Timestamp.from(instant))
                it.setLong(4, timeout)
                it.setString(5, owner)
                it.setTimestamp(6, Timestamp.from(instant.plusMillis(timeout)))

                try {
                    statement.execute()
                    statement.resultSet.toAtMostOneLease()
                        ?: throw IllegalStateException("INSERT statement didn't return the inserted value.")
                } catch (error: PSQLException) {
                    if (error.sqlState == PSQLState.UNIQUE_VIOLATION.state) {
                        throw LeaseAlreadyExists(leaseName)
                    } else {
                        throw error
                    }
                }
            }
        }
    }

    override fun acquireLease(leaseName: String, owner: String, timeout: Long): Lease {
        return doInTransaction { connection ->
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
                statement.resultSet.toAtMostOneLease()
                    ?: throw LeaseAcquiringFailed("A valid lease '$leaseName already exists.'")
            }
        }
    }

    override fun renewLease(lease: Lease): Lease {
        if (lease !is PersistentLease) {
            throw IllegalArgumentException(
                "Unsupported Lease implementation - ${lease.javaClass}." +
                        " ${PersistentLease::class.qualifiedName} expected."
            )
        }

        return doInTransaction { connection ->
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
                it.resultSet.toAtMostOneLease() ?: throw LeaseNotFound(lease.name)
            }
        }
    }

    override fun removeLease(lease: Lease) {
        doInTransaction { connection ->
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

    private fun <T> doInTransaction(block: (Connection) -> T): T {

        val transaction = Transaction.getCurrentTransaction { dataSource.connection }

        return try {
            transaction.beginTransaction()
            block.invoke(transaction.connection)
        } catch (ex: Exception) {
            transaction.setRollbackOnly()
            throw ex
        } finally {
            transaction.endTransaction()
        }
    }

    private fun ResultSet.toAtMostOneLease(): PersistentLease? {
        val isEmpty = !next()

        return if (isEmpty) {
            null
        } else {
            val name = getString(COLUMN_NAME)
            val version = getLong(COLUMN_VERSION)
            val acquiredAt = getTimestamp(COLUMN_ACQUIRED_AT).toInstant()
            val renewedAt = getTimestamp(COLUMN_RENEWED_AT).toInstant()
            val timeout = getLong(COLUMN_TIMEOUT)
            val holderName = getString(COLUMN_HOLDER_NAME)

            if (next()) {
                throw IllegalStateException("More than one lease found for the name '$name'")
            }

            PersistentLease(name, version, acquiredAt, renewedAt, timeout, holderName)
        }
    }
}