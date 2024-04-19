package sk.bakeit.lease.api

/**
 * This exception indicates the client's lease data are stale and needs to be refreshed.
 */
class StaleLease(message: String): LeaseException(message)