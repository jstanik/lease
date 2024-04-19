package sk.bakeit.lease.api

/**
 * Indicates that acquiring a lease failed. This is usually because the lease is already acquired
 * by a different holder and the lease is not expired.
 */
class LeaseAcquiringFailed(message: String): LeaseException(message)