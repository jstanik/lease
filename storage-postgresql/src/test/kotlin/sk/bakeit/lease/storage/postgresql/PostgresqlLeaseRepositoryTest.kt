package sk.bakeit.lease.storage.postgresql

import com.google.common.collect.BoundType.CLOSED
import com.google.common.collect.Range.range
import com.google.common.truth.Truth.assertThat
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import sk.bakeit.lease.api.LeaseAcquiringFailed
import sk.bakeit.lease.api.LeaseAlreadyExists
import sk.bakeit.lease.api.LeaseNotFound
import java.time.Instant
import java.time.temporal.ChronoUnit.MILLIS

class PostgresqlLeaseRepositoryTest {

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

    val cut = PostgresqlLeaseRepository(dataSource)

    @Test
    fun createLease() {
        val name = "createLease-name1"
        val holderName = "createLease-holder"
        val timeout = 30000L
        val before = Instant.now().truncatedTo(MILLIS).minusMillis(10)

        val actual = cut.createLease(name, holderName, timeout) as PersistentLease

        val after = Instant.now().truncatedTo(MILLIS).plusMillis(10)

        assertThat(actual.name).isEqualTo(name)
        assertThat(actual.version).isEqualTo(1)
        assertThat(actual.holderName).isEqualTo(holderName)
        assertThat(actual.timeout).isEqualTo(timeout)
        assertThat(actual.acquiredAt.truncatedTo(MILLIS)).isIn(range(before, CLOSED, after, CLOSED))
        assertThat(actual.renewedAt.truncatedTo(MILLIS)).isIn(range(before, CLOSED, after, CLOSED))
    }

    @Test
    fun `createLease called twice with the same lease`() {
        val name = "createLease-duplicate"

        // create first lease
        cut.createLease(name, "irrelevant 1") as PersistentLease

        // try to repeat the creation with the same lease name
        assertThrows<LeaseAlreadyExists> {
            cut.createLease(name, "irrelevant 2")
        }
    }

    @Test
    fun `acquireLease with unique name`() {
        val name = "acquireLease-unique"
        val holderName = "createLease-holder"
        val timeout = 30000L
        val before = Instant.now().truncatedTo(MILLIS)

        val actual = cut.acquireLease(name, holderName, timeout) as PersistentLease

        val after = Instant.now().truncatedTo(MILLIS)

        assertThat(actual.name).isEqualTo(name)
        assertThat(actual.version).isEqualTo(1)
        assertThat(actual.holderName).isEqualTo(holderName)
        assertThat(actual.timeout).isEqualTo(timeout)
        assertThat(actual.acquiredAt.truncatedTo(MILLIS)).isIn(range(before, CLOSED, after, CLOSED))
        assertThat(actual.renewedAt.truncatedTo(MILLIS)).isIn(range(before, CLOSED, after, CLOSED))
    }

    @Test
    fun `acquireLease fails when valid lease exists`() {
        val name = "acquireLease-failes-when-valid-exists"
        val holderName = "createLease-holder"
        val timeout = 30000L
        cut.acquireLease(name, holderName, timeout)

        assertThrows<LeaseAcquiringFailed> {
            cut.acquireLease(name, "irrelevant")
        }
    }

    @Test
    fun `acquireLease steal existing lease when it is expired`() {
        val name = "acquireLease-steal"
        val holderName = "createLease-holder"
        val timeout = 1L

        val expired = cut.acquireLease(name, holderName, timeout) as PersistentLease
        Thread.sleep(2)

        val before = Instant.now().truncatedTo(MILLIS)
        val actual = cut.acquireLease(name, "thief", 100L) as PersistentLease
        val after = Instant.now().truncatedTo(MILLIS)

        assertThat(actual.version).isEqualTo(expired.version + 1)
        assertThat(actual.name).isEqualTo(expired.name)
        assertThat(actual.acquiredAt).isEqualTo(expired.acquiredAt)
        assertThat(actual.renewedAt).isNotEqualTo(expired.renewedAt)
        assertThat(actual.renewedAt.truncatedTo(MILLIS)).isIn(range(before, CLOSED, after, CLOSED))
        assertThat(actual.timeout).isEqualTo(100L)
        assertThat(actual.holderName).isEqualTo("thief")
    }

    @Test
    fun renewLease() {
        val name = "renewLease-name1"
        val holderName = "renewLease-holder"

        val createdLease = cut.createLease(name, holderName) as PersistentLease

        val before = Instant.now().truncatedTo(MILLIS)

        val actual = cut.renewLease(createdLease) as PersistentLease
        val after = Instant.now().truncatedTo(MILLIS)

        assertThat(actual.name).isEqualTo(createdLease.name)
        assertThat(actual.version).isEqualTo(2)
        assertThat(actual.holderName).isEqualTo(createdLease.holderName)
        assertThat(actual.timeout).isEqualTo(createdLease.timeout)

        assertThat(actual.acquiredAt.truncatedTo(MILLIS)).isLessThan(before)
        assertThat(actual.renewedAt.truncatedTo(MILLIS)).isIn(range(before, CLOSED, after, CLOSED))
    }

    @Test
    fun `renewLease that does not exist`() {
        val lease = PersistentLease(
            "renewLease-non-existing",
            1,
            Instant.now(),
            Instant.now(),
            30000,
            "renewLease-non-existing-holder"
        )

        assertThrows<LeaseNotFound> { cut.renewLease(lease) }
    }

    @Test
    fun removeLease() {
        val name = "removeLease-name1"
        val existingLease = cut.createLease(name, "irrelevant")

        cut.removeLease(existingLease)

        assertThat(cut.findLease(name)).isNull()
    }

    @Test
    fun `removeLease fails when no such lease exists`() {
        val name = "removeLease-non-existing"

        val lease = PersistentLease(name, 1, Instant.now(), Instant.now(), 3000, "irrelevant")

        assertThrows<LeaseNotFound> { cut.removeLease(lease) }
    }

    @Test
    fun `findLease - not found`() {
        val actual = cut.findLease("findLease-not-found")

        assertThat(actual).isNull()
    }

    @Test
    fun findLease() {
        val name = "findLease-name1"
        val holderName = "findLease-holder"
        val timeout = 30000L

        val created = cut.createLease(name, holderName, timeout) as PersistentLease

        val actual = cut.findLease(name) as PersistentLease

        assertThat(actual.name).isEqualTo(created.name)
        assertThat(actual.version).isEqualTo(created.version)
        assertThat(actual.acquiredAt).isEqualTo(created.acquiredAt)
        assertThat(actual.renewedAt).isEqualTo(created.renewedAt)
        assertThat(actual.timeout).isEqualTo(created.timeout)
        assertThat(actual.holderName).isEqualTo(created.holderName)
    }
}