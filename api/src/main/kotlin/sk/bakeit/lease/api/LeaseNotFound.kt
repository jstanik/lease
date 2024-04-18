package sk.bakeit.lease.api

/**
 * This exception indicates that a lease with a given name was not found in the repository.
 */
class LeaseNotFound(leaseName: String) : LeaseException("Lease $leaseName not found.")