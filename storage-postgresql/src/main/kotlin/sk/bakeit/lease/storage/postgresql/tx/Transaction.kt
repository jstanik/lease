package sk.bakeit.lease.storage.postgresql.tx

import java.sql.Connection

/**
 * A simple reentrant transaction implementation. Closes the connection in the end of the transaction.
 */
internal class Transaction(
    val connection: Connection,
) {

    companion object {
        private val currentTransaction = ThreadLocal<Transaction>()

        @JvmStatic
        fun getCurrentTransaction(connectionProvider: () -> Connection): Transaction {
            val tx: Transaction? = currentTransaction.get()

            return tx ?: Transaction(connectionProvider()).also { newTx ->
                currentTransaction.set(newTx)
            }
        }
    }

    private var rollbackOnly: Boolean = false
    private var nested = 0
    private var autoCommitWasEnabled: Boolean = false
    private var originalTransactionIsolation: Int = Connection.TRANSACTION_NONE

    fun beginTransaction() {
        if (++nested == 1) {
            autoCommitWasEnabled = connection.autoCommit

            if (autoCommitWasEnabled) {
                connection.autoCommit = false
            }

            originalTransactionIsolation = connection.transactionIsolation

            if (originalTransactionIsolation != Connection.TRANSACTION_READ_COMMITTED) {
                connection.transactionIsolation = Connection.TRANSACTION_READ_COMMITTED
            }
        }
    }

    fun setRollbackOnly() {
        rollbackOnly = true
    }

    /**
     * Tries to commit the transaction if the flag `setRollbackOnly` hasn't been set.
     */
    fun endTransaction() {
        if (--nested == 0) {

            currentTransaction.remove()

            if (autoCommitWasEnabled) {
                connection.autoCommit = true
            }

            if (originalTransactionIsolation != Connection.TRANSACTION_READ_COMMITTED) {
                connection.transactionIsolation = originalTransactionIsolation
            }

            connection.use { connection ->
                if (rollbackOnly) {
                    connection.rollback()
                } else {
                    connection.commit()
                }
            }

        } else if (nested < 0) {
            throw IllegalStateException("Transaction already ended.")
        }
    }
}
