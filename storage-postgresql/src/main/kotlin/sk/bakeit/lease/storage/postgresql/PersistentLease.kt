package sk.bakeit.lease.storage.postgresql

import sk.bakeit.lease.api.Lease
import java.time.Instant

/**
 * Private API. It should not be used directly by the client code.
 */
internal data class PersistentLease(
    override val name: String,
    val version: Long,
    override val acquiredAt: Instant,
    override val renewedAt: Instant,
    override val timeout: Long,
    override val holderName: String
): Lease