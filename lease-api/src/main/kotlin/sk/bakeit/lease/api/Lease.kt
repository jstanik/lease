package sk.bakeit.lease.api

import java.time.Instant

/**
 * A lease for coordination purposes.
 */
interface Lease {
    /**
     * The name of the lease.
     */
    val name: String

    /**
     * The timestamp when the lease was acquired for the first time.
     */
    val acquiredAt: Instant

    /**
     * The timestamp when the lease was last renewed by the current holder.
     */
    val renewedAt: Instant

    /**
     * The lease timeout in milliseconds from the last renewal. After the timeout the lease can
     * be acquired by other holder.
     */
    val timeout: Long

    /**
     * The name of the current leaseholder.
     */
    val holderName: String

    /**
     * Renews this lease.
     *
     * @param timeout the timeout of the renewed lease
     *
     * @return a new instance representing the renewed lease
     */
    fun renew(timeout: Long = this.timeout): Lease

    /**
     * Releases the lease. Basically this means the lease will be deleted.
     */
    fun release()

}