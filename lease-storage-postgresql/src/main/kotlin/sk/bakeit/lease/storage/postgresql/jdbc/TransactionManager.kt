package sk.bakeit.lease.storage.postgresql.jdbc

import java.sql.Connection

/**
 * A simple transaction manager that allows to execute block of code within a transaction
 * context.
 */
internal class TransactionManager(
    private val connectionProvider: () -> Connection,
) {

    private val currentContext = ThreadLocal<TransactionContext>()

    /**
     * Executes the block of the code in the transaction context. This method is reentrant completing
     * the transaction only on the exit of the first entered block.
     */
    fun <T> doInTransaction(block: (Connection) -> T): T {

        val transactionContext = getCurrentContext()

        var caughtException: Exception? = null
        return try {
            transactionContext.enter()
            block.invoke(transactionContext.connection)
        } catch (ex: Exception) {
            transactionContext.setRollbackOnly()
            caughtException = ex
            throw ex
        } finally {
            val afterCompletion: () -> Unit = {
                clearContext()
                transactionContext.connection.close()
            }
            when (caughtException) {
                null -> transactionContext.exit(afterCompletion)
                else -> try {
                    transactionContext.exit(afterCompletion)
                } catch (exitException: Exception) {
                    caughtException.addSuppressed(exitException)
                }
            }
        }
    }

    private fun getCurrentContext(): TransactionContext {
        val context: TransactionContext? = currentContext.get()

        return context ?: TransactionContext(connectionProvider.invoke()).also { newContext ->
            currentContext.set(newContext)
        }
    }

    private fun clearContext() {
        currentContext.remove()
    }

    private class TransactionContext(
        val connection: Connection,
    ) {

        private var rollbackOnly: Boolean = false
        private var nested = 0
        private var autoCommitWasEnabled: Boolean = false
        private var originalTransactionIsolation: Int = Connection.TRANSACTION_NONE

        fun enter() {
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

        fun exit(afterCompletion: () -> Unit) {
            if (nested == 1) {
                nested = 0

                if (autoCommitWasEnabled) {
                    connection.autoCommit = true
                }

                if (originalTransactionIsolation != Connection.TRANSACTION_READ_COMMITTED) {
                    connection.transactionIsolation = originalTransactionIsolation
                }

                var completionError: Exception? = null
                try {
                    if (rollbackOnly) {
                        connection.rollback()
                    } else {
                        connection.commit()
                    }
                } catch (error: Exception) {
                    completionError = error
                    throw error
                } finally {
                    when (completionError) {
                        null -> afterCompletion.invoke()
                        else ->
                            try {
                                afterCompletion.invoke()
                            } catch (afterCompletionException: Exception) {
                                completionError.addSuppressed(afterCompletionException)
                            }
                    }
                }

            } else if (nested == 0) {
                throw IllegalStateException("No transaction context.")
            } else {
                --nested
            }
        }
    }
}
