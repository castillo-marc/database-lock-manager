import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.locks.Lock;

/**
 * The Lock Manager handles lock and unlock requests from transactions. The
 * Lock Manager will maintain a hash table that is keyed on the resource
 * being locked. The Lock Manager will also keep a FIFO queue of requests
 * for locks that cannot be immediately granted.
 */
public class LockManager {

    public enum LockType {
        S,
        X,
        IS,
        IX
    }

    private HashMap<Resource, ResourceLock> resourceToLock;

    public LockManager() {
        this.resourceToLock = new HashMap<Resource, ResourceLock>();

    }

    /**
     * The acquire method will grant the lock if it is compatible. If the lock
     * is not compatible, then the request will be placed on the requesters
     * queue.
     * @param transaction that is requesting the lock
     * @param resource that the transaction wants
     * @param lockType of requested lock
     */
    public void acquire(Transaction transaction, Resource resource, LockType lockType)
            throws IllegalArgumentException {

        ResourceLock resourceLock = null;
        if (!resourceToLock.keySet().contains(resource)) {
            resourceLock = new ResourceLock();
        } else {
            resourceLock = resourceToLock.get(resource);
        }

        Request request = new Request(transaction, lockType);
        Request lockBeforeUpgrade = null;

        //all the illegal argument exceptions
        if (transaction.getStatus() == Transaction.Status.Waiting) {
            throw new IllegalArgumentException("Transaction is blocked");
        }

        if (!resourceLock.lockOwners.isEmpty()) {
            for (Request owner : resourceLock.lockOwners) {
                if (owner.transaction.equals(transaction)) {
                    if (owner.lockType.equals(lockType)) {
                        throw new IllegalArgumentException("Transaction already holds this type of lock");
                    }
                    if (owner.lockType == LockType.X && lockType == LockType.S) {
                        throw new IllegalArgumentException("Transaction trying to downgrade X -> S");
                    }
                    if (owner.lockType == LockType.IX && lockType == LockType.IS) {
                        throw new IllegalArgumentException("Transaction trying to downgrade IX -> IS");
                    }
                    if (owner.lockType == LockType.S && lockType == LockType.X) {
                        //going to upgrade this later
                        lockBeforeUpgrade = owner;
                    }
                }
            }
        }

        if (resource.getResourceType() == Resource.ResourceType.PAGE) {
            if (lockType == LockType.IS || lockType == LockType.IX) {
                throw new IllegalArgumentException("Transaction requesting intent lock on page");
            }
            Page page = (Page) resource;
            Table parent = page.getTable();
            ResourceLock locksOnParent = resourceToLock.get(parent);
            if (locksOnParent == null) {
                throw new IllegalArgumentException("Transaction doesn't hold appropriate parent lock");
            }
            Boolean holdsParentLock = false;
            for (Request ownerOfParent : locksOnParent.lockOwners) {
                if (ownerOfParent.transaction.equals(transaction)) {
                    holdsParentLock = true;
                    if (lockType == LockType.S) {
                        if (!(ownerOfParent.lockType == LockType.IS || ownerOfParent.lockType == LockType.IX)) {
                            throw new IllegalArgumentException("Transaction doesn't hold appropriate parent lock");
                        }
                    }
                    if (lockType == LockType.X) {
                        if (ownerOfParent.lockType != LockType.IX) {
                            throw new IllegalArgumentException("Transaction doesn't hold appropriate parent lock");
                        }
                    }
                }
            }
            if (holdsParentLock == false) {
                throw new IllegalArgumentException("Transaction doesn't hold appropriate parent lock");
            }
        }

        if (compatible(resource, transaction, lockType)) {
            resourceLock.lockOwners.add(request);
            if (lockBeforeUpgrade != null) {
                resourceLock.lockOwners.remove(lockBeforeUpgrade);
            }
            resourceToLock.put(resource, resourceLock);

        } else {
            if (lockBeforeUpgrade != null) {
                resourceLock.requestersQueue.add(0, request);
            } else {
                resourceLock.requestersQueue.add(request);
            }
            resourceToLock.put(resource, resourceLock);
            transaction.sleep();
        }

        return;
    }

    /**
     * Checks whether the a transaction is compatible to get the desired lock on the given resource
     * @param resource the resource we are looking it
     * @param transaction the transaction requesting a lock
     * @param lockType the type of lock the transaction is request
     * @return true if the transaction can get the lock, false if it has to wait
     */
    private boolean compatible(Resource resource, Transaction transaction, LockType lockType) {

        if (!resourceToLock.keySet().contains(resource)) {
            return true;
        }

        ResourceLock resourceLock = resourceToLock.get(resource);
        Request request = new Request(transaction, lockType);

        for (Request owner : resourceLock.lockOwners) {
            if (!checkMatrixCompatibility(owner, request)) {
                return false;
            }
        }

        return true;
    }

    //my own helper
    private boolean checkMatrixCompatibility(Request owner, Request requester) {
        LockType ownedType = owner.lockType;
        LockType requestedType = requester.lockType;
        if (ownedType == LockType.S) {
            if (requestedType == LockType.IX) {
                return false;
            }
            if (requestedType == LockType.X) {
                //upgrade case
                if (owner.transaction.equals(requester.transaction)) {
                    return true;
                } else {
                    return false;
                }
            }
        } else if (ownedType == LockType.X) {
            return false;
        } else if (ownedType == LockType.IS) {
            if (requestedType == LockType.X) {
                return false;
            }
        } else {
            if (requestedType == LockType.S || requestedType == LockType.X) {
                return false;
            }
        }
        return true;
    }

    /**
     * Will release the lock and grant all mutually compatible transactions at
     * the head of the FIFO queue. See spec for more details.
     * @param transaction releasing lock
     * @param resource of Resource being released
     */
    public void release(Transaction transaction, Resource resource) throws IllegalArgumentException{
        if (transaction.getStatus() == Transaction.Status.Waiting) {
            throw new IllegalArgumentException("Transaction is blocked");
        }

        ResourceLock resourceLock = null;
        if (!resourceToLock.keySet().contains(resource)) {
            throw new IllegalArgumentException("Resource has no locks on it");
        } else {
            resourceLock = resourceToLock.get(resource);
        }

        Request toBeReleased = null;
        for (Request owner : resourceLock.lockOwners) {
            if (owner.transaction == transaction) {
                toBeReleased = owner;
                break;
            }
        }

        if (toBeReleased == null) {
            throw new IllegalArgumentException("Transaction does not hold a lock on resource");
        }

        if (!(toBeReleased.lockType == LockType.S || toBeReleased.lockType == LockType.X ||
                toBeReleased.lockType == LockType.IS || toBeReleased.lockType == LockType.IX)) {
            throw new IllegalArgumentException("Transaction does not hold appropriate lock type");
        }

        if (resource.getResourceType() == Resource.ResourceType.TABLE) {
            Table table = (Table) resource;
            for (Page child : table.getPages()) {
                ResourceLock locksOnChild = resourceToLock.get(child);
                for (Request ownerOfChild : locksOnChild.lockOwners) {
                    if (ownerOfChild.transaction.equals(transaction)) {
                        throw new IllegalArgumentException("Transaction has not released bottom up");
                    }
                }
            }
        }

        resourceLock.lockOwners.remove(toBeReleased);
        transaction.wake();
        promote(resource);
        return;
    }

    /**
     * This method will grant mutually compatible lock requests for the resource
     * from the FIFO queue.
     * @param resource of locked Resource
     */
     private void promote(Resource resource) {
         ResourceLock resourceLock = resourceToLock.get(resource);
         int numToPop = 0;
         for (Request requester : resourceLock.requestersQueue) {
             Transaction transaction = requester.transaction;
             LockType lockType = requester.lockType;
             if (compatible(resource, transaction, lockType)) {
                 transaction.wake();
                 acquire(transaction, resource, lockType);
                 numToPop++;
             } else {
                 break;
             }
         }
         for (int i = 0; i < numToPop; i++) {
             resourceLock.requestersQueue.removeFirst();
         }
         return;
     }

    /**
     * Will return true if the specified transaction holds a lock of type
     * lockType on the resource.
     * @param transaction potentially holding lock
     * @param resource on which we are checking if the transaction has a lock
     * @param lockType of lock
     * @return true if the transaction holds lock
     */
    public boolean holds(Transaction transaction, Resource resource, LockType lockType) {
        ResourceLock resourceLock = resourceToLock.get(resource);
        Request request = new Request(transaction, lockType);

        for (Request owner : resourceLock.lockOwners) {
            if (owner.equals(request)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Contains all information about the lock for a specific resource. This
     * information includes lock owner(s), and lock requester(s).
     */
    private class ResourceLock {
        private ArrayList<Request> lockOwners;
        private LinkedList<Request> requestersQueue;

        public ResourceLock() {
            this.lockOwners = new ArrayList<Request>();
            this.requestersQueue = new LinkedList<Request>();
        }

    }

    /**
     * Used to create request objects containing the transaction and lock type.
     * These objects will be added to the requester queue for a specific resource
     * lock.
     */
    private class Request {
        private Transaction transaction;
        private LockType lockType;

        public Request(Transaction transaction, LockType lockType) {
            this.transaction = transaction;
            this.lockType = lockType;
        }

        @Override
        public String toString() {
            return String.format(
                    "Request(transaction=%s, lockType=%s)",
                    transaction, lockType);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null) {
                return false;
            } else if (o instanceof Request) {
                Request otherRequest  = (Request) o;
                return otherRequest.transaction.equals(this.transaction) && otherRequest.lockType.equals(this.lockType);
            } else {
                return false;
            }
        }
    }
}
