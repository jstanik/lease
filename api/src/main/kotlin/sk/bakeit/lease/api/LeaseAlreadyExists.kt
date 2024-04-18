package sk.bakeit.lease.api

/**
 * This exception indicates that a lease with the requested name already exists.
 */
class LeaseAlreadyExists(
    val leaseName: String
) : LeaseException("Lease with the name '${leaseName} already exists.")