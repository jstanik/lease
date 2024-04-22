package sk.bakeit.lease.storage.postgresql.jdbc

import com.google.common.truth.Truth.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoInteractions
import org.mockito.kotlin.whenever
import java.io.IOException
import java.sql.Connection
import java.sql.SQLException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

@ExtendWith(MockitoExtension::class)
class TransactionManagerTest {

    @Mock
    lateinit var connection1: Connection

    @Mock
    lateinit var connection2: Connection


    private lateinit var cut: TransactionManager

    @BeforeEach
    fun beforeEach() {
        val list = mutableListOf(connection1, connection2)
        cut = TransactionManager { list.removeFirst() }
    }

    @Test
    fun doInTransaction() {

        whenever(connection1.autoCommit).thenReturn(true)
        whenever(connection1.transactionIsolation).thenReturn(Connection.TRANSACTION_NONE)

        cut.doInTransaction {
            cut.doInTransaction {
                println("In transaction")
            }
        }

        verify(connection1, times(1)).autoCommit = false
        verify(connection1, times(1)).transactionIsolation = Connection.TRANSACTION_READ_COMMITTED

        verify(connection1, times(1)).commit()
        verify(connection1, times(1)).close()
        verifyNoInteractions(connection2)
    }

    @Test
    fun `doInTransaction - exception causes rollback`() {

        whenever(connection1.autoCommit).thenReturn(false)
        whenever(connection1.transactionIsolation).thenReturn(Connection.TRANSACTION_READ_COMMITTED)

        val expectedException = RuntimeException("Error")

        val actual = assertThrows<RuntimeException> {
            cut.doInTransaction {
                cut.doInTransaction {
                    throw expectedException
                }
            }
        }

        assertThat(actual).isEqualTo(expectedException)

        verify(connection1, times(1)).rollback()
        verify(connection1, times(1)).close()
    }

    @Test
    fun `doInTransaction - close exception is added as suppressed to the business exception`() {

        whenever(connection1.autoCommit).thenReturn(false)
        whenever(connection1.transactionIsolation).thenReturn(Connection.TRANSACTION_READ_COMMITTED)

        val closeException = IOException("close failed")
        whenever(connection1.close()).thenThrow(closeException)

        val businessException = RuntimeException("Error")

        val actual = assertThrows<RuntimeException> {
            cut.doInTransaction {
                cut.doInTransaction {
                    throw businessException
                }
            }
        }

        assertThat(actual).isEqualTo(businessException)
        assertThat(actual.suppressed).asList().contains(closeException)

        verify(connection1, times(1)).rollback()
        verify(connection1, times(1)).close()
    }

    @Test
    fun `doInTransaction - close exception is suppressed by commit exception`() {

        whenever(connection1.autoCommit).thenReturn(false)
        whenever(connection1.transactionIsolation).thenReturn(Connection.TRANSACTION_READ_COMMITTED)

        val commitException = SQLException("commit failed")
        whenever(connection1.commit()).thenThrow(commitException)

        val closeException = IOException("close failed")
        whenever(connection1.close()).thenThrow(closeException)

        val actual = assertThrows<SQLException> {
            cut.doInTransaction {
            }
        }

        assertThat(actual as Throwable).isEqualTo(commitException)
        assertThat(actual.suppressed).asList().contains(closeException)

        verify(connection1, times(1)).commit()
        verify(connection1, times(1)).close()
    }

    @Test
    fun `doInTransaction - subsequent calls executed in two different transactions`() {

        whenever(connection1.autoCommit).thenReturn(false)
        whenever(connection1.transactionIsolation).thenReturn(Connection.TRANSACTION_READ_COMMITTED)
        whenever(connection2.autoCommit).thenReturn(false)
        whenever(connection2.transactionIsolation).thenReturn(Connection.TRANSACTION_READ_COMMITTED)

        cut.doInTransaction {
            println(it)
        }
        cut.doInTransaction {
            println(it)
        }


        verify(connection1, times(1)).close()
        verify(connection2, times(1)).close()
    }

    @Test
    fun `doInTransaction - start transaction per thread`() {

        whenever(connection1.autoCommit).thenReturn(false)
        whenever(connection1.transactionIsolation).thenReturn(Connection.TRANSACTION_READ_COMMITTED)

        whenever(connection2.autoCommit).thenReturn(false)
        whenever(connection2.transactionIsolation).thenReturn(Connection.TRANSACTION_READ_COMMITTED)

        val checkPointTx1 = CountDownLatch(1)
        val checkPointTx2 = CountDownLatch(1)
        val allTasksFinished = CountDownLatch(2)

        Thread {
            cut.doInTransaction {
                checkPointTx1.countDown()
                checkPointTx2.await()
            }
            allTasksFinished.countDown()
        }.start()
        Thread {
            checkPointTx1.await()
            cut.doInTransaction {
                checkPointTx2.countDown()
            }
            allTasksFinished.countDown()
        }.start()

        allTasksFinished.await(5, TimeUnit.SECONDS)

        verify(connection1, times(1)).close()
        verify(connection2, times(1)).close()
    }
}