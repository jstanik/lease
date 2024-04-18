package sk.bakeit.lease.api

/**
 * A repository for the `Lease` entity.
 */
interface LeaseRepository {

    /**
     * Finds a lease with the given name.
     *
     * @return the lease having the required name or `null` if no such lease can be found
     */
    fun findLease(leaseName: String): Lease?

    /**
     * Creates the lease with a specific name and an owner.
     *
     * @param leaseName the requested name of the lease
     * @param owner the owner name
     * @param timeout the lease timeout in milliseconds
     */
    fun createLease(
        leaseName: String,
        owner: String,
        timeout: Long = 60_000,
    ): Lease

    /**
     * Renews the lease.
     *
     * @param lease the lease to renew
     * @throws LeaseNotFound
     */
    fun renewLease(lease: Lease): Lease

    /**
     * Removes this lease.
     */
    fun removeLease(lease: Lease)
}