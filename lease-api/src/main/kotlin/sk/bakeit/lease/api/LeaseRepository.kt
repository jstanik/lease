package sk.bakeit.lease.api

/**
 * A repository for the `Lease` entity.
 */
interface LeaseRepository {

    /**
     * Acquires a lease if possible. A lease can be acquired if there is no such a lease in the
     * repository or if the existing lease is expired.
     *
     * @throws LeaseAcquiringFailed when the lease acquiring failed
     */
    fun acquireLease(
        leaseName: String,
        owner: String,
        timeout: Long = 60_000,
    ): Lease

    /**
     * Finds a lease with the given name.
     *
     * @return the lease having the required name or `null` if no such lease can be found
     */
    fun findLease(leaseName: String): Lease?

    /**
     * Removes this lease.
     *
     * @throws LeaseNotFound if no such lease exists in the repository.
     */
    fun removeLease(lease: Lease)
}