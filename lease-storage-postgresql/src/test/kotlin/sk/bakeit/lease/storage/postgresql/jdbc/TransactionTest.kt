package sk.bakeit.lease.storage.postgresql.jdbc

import com.google.common.truth.Truth.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.sql.Connection

@ExtendWith(MockitoExtension::class)
class TransactionTest {

    // These tests incur a memory leak due to not cleaning up a ThreadLocal variable
    // inside Transaction class. Introducing cleanup code would decrease the readability
    // of the code and hide the test purpose.

    @Mock
    lateinit var connection: Connection

    @Test
    fun `getCurrentTransaction called twice from the same thread returns same instance`() {
        val actual1 = Transaction.getCurrentTransaction { connection }
        val actual2 = Transaction.getCurrentTransaction { connection }

        assertThat(actual2).isSameInstanceAs(actual1)
    }

    @Test
    fun beginTransaction() {
        val cut = Transaction.getCurrentTransaction { connection }

        whenever(connection.autoCommit).thenReturn(true)
        whenever(connection.transactionIsolation).thenReturn(Connection.TRANSACTION_NONE)

        cut.beginTransaction()
        cut.beginTransaction() // reentrant - should have no effect

        verify(connection, times(1)).autoCommit = false
        verify(connection, times(1)).transactionIsolation = Connection.TRANSACTION_READ_COMMITTED
    }

    @Test
    fun endTransaction() {
        val cut = Transaction.getCurrentTransaction { connection }
        whenever(connection.autoCommit).thenReturn(true)
        whenever(connection.transactionIsolation).thenReturn(Connection.TRANSACTION_NONE)
        cut.beginTransaction()
        verify(connection, times(1)).autoCommit = false
        verify(connection, times(1)).transactionIsolation = Connection.TRANSACTION_READ_COMMITTED
        
        cut.endTransaction()

        verify(connection, times(1)).autoCommit = true
        verify(connection, times(1)).transactionIsolation = Connection.TRANSACTION_NONE
        val otherTransaction = Transaction.getCurrentTransaction { connection }
        assertThat(otherTransaction).isNotSameInstanceAs(cut)
    }


}