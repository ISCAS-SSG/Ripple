package ripple.server.core;

/**
 * Type of atomic operations
 *
 * @author Zhen Tang
 */
public enum OperationType {
    /**
     * Add a new entry to a specific item
     */
    ADD_ENTRY,
    /**
     * Remove an entry from a specific item
     */
    REMOVE_ENTRY,
}