package sk.bakeit.lease.api

/**
 * Exception indicating that a lease with the requested name already exists.
 */
class LeaseAlreadyExists(
    val lease: Lease
) : RuntimeException(
    """
    Lease with the name '${lease.name} already exists:
      name: ${lease.name}
      holder: ${lease.holderName}
      acquiredAt: ${lease.acquiredAt}
      renewedAt: ${lease.renewedAt}
      timeout: ${lease.timeout}
    """.trimIndent()
) {
}