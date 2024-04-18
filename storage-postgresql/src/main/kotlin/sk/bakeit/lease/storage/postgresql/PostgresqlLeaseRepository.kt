package sk.bakeit.lease.storage.postgresql

import org.postgresql.util.PSQLException
import org.postgresql.util.PSQLState
import sk.bakeit.lease.api.Lease
import sk.bakeit.lease.api.LeaseAlreadyExists
import sk.bakeit.lease.api.LeaseNotFound
import sk.bakeit.lease.api.LeaseRepository
import java.sql.Connection
import java.sql.ResultSet
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
        private const val FULL_COLUMN_LIST = "$COLUMN_NAME, $COLUMN_VERSION, $COLUMN_ACQUIRED_AT," +
                " $COLUMN_RENEWED_AT, $COLUMN_TIMEOUT, $COLUMN_HOLDER_NAME"
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
                   VALUES(?, 1, now(), now(), ?, ?)
                   RETURNING $FULL_COLUMN_LIST
                """.trimIndent()
            )

            statement.use {
                it.setString(1, leaseName)
                it.setLong(2, timeout)
                it.setString(3, owner)

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
                    $COLUMN_RENEWED_AT = now()
                WHERE $COLUMN_NAME = ? AND $COLUMN_VERSION = ?
                RETURNING $FULL_COLUMN_LIST
            """.trimIndent()
            )

            statement.use {
                it.setString(1, lease.name)
                it.setLong(2, lease.version)

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
        val connection = dataSource.connection
        val autoCommitWasEnabled: Boolean = connection.autoCommit

        if (autoCommitWasEnabled) {
            connection.autoCommit = false
        }

        val originalTransactionIsolation = connection.transactionIsolation
        if (originalTransactionIsolation != Connection.TRANSACTION_READ_COMMITTED) {
            connection.transactionIsolation = Connection.TRANSACTION_READ_COMMITTED
        }

        return try {
            val result = block.invoke(connection)
            connection.commit()
            result
        } catch (ex: Exception) {
            connection.rollback()
            throw ex
        } finally {
            if (autoCommitWasEnabled) {
                connection.autoCommit = true
            }

            if (originalTransactionIsolation != Connection.TRANSACTION_READ_COMMITTED) {
                connection.transactionIsolation = originalTransactionIsolation
            }

            connection.close()
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