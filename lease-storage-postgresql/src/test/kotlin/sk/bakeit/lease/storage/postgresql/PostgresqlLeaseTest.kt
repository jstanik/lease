package sk.bakeit.lease.storage.postgresql

import com.google.common.collect.BoundType.CLOSED
import com.google.common.collect.Range.range
import com.google.common.truth.Truth.assertThat
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import java.time.Instant
import java.time.temporal.ChronoUnit.MILLIS

class PostgresqlLeaseTest {
    companion object {
        private val dataSource = HikariDataSource(HikariConfig().apply {
            jdbcUrl =
                "jdbc:tc:postgresql:16.2-alpine:///unittests?TC_INITSCRIPT=file:src/main/sql/init_database.sql"
            isAutoCommit = false
        })

        @JvmStatic
        @AfterAll
        fun afterAll() {
            dataSource.close()
        }
    }

    private val repository = PostgresqlLeaseRepository(dataSource)

    @Test
    fun renew() {
        val name = "renewLease-name1"
        val holderName = "renewLease-holder"

        val original = repository.acquireLease(name, holderName) as PostgresqlLease

        val before = Instant.now().truncatedTo(MILLIS)

        val actual = original.renew() as PostgresqlLease
        val after = Instant.now().truncatedTo(MILLIS)

        assertThat(actual.name).isEqualTo(original.name)
        assertThat(actual.version).isEqualTo(2)
        assertThat(actual.holderName).isEqualTo(original.holderName)
        assertThat(actual.timeout).isEqualTo(original.timeout)

        assertThat(actual.acquiredAt.truncatedTo(MILLIS)).isLessThan(before)
        assertThat(actual.renewedAt.truncatedTo(MILLIS)).isIn(range(before, CLOSED, after, CLOSED))
    }

    @Test
    fun release() {
        val name = "release-name1"
        val cut = repository.acquireLease(name, "irrelevant")

        cut.release()

        assertThat(repository.findLease(name)).isNull()
    }
}