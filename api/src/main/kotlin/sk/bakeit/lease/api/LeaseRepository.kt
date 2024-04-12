package sk.bakeit.lease.api

/**
 * A repository for the `Lease` entity.
 */
interface LeaseRepository {

    /**
     * Finds a lease with the given name.
     *
     * @return the lease having the required name or `null` no such lease was found
     */
    fun findLease(leaseName: String): Lease?

    /**
     * Creates the lease with a specific name and an owner.
     *
     * @param leaseName the requested name of the lease
     * @param owner the owner name
     */
    fun createLease(leaseName: String, owner: String): Lease
}