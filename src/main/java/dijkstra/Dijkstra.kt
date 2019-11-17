package dijkstra

import com.sun.org.apache.xpath.internal.operations.Bool
import java.util.*
import java.util.concurrent.Phaser
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import kotlin.Comparator
import kotlin.collections.ArrayList
import kotlin.concurrent.thread

private val NODE_DISTANCE_COMPARATOR = Comparator<Node> { o1, o2 -> Integer.compare(o1!!.distance, o2!!.distance) }

class MultiPriorityQueue<T>(val n: Int, comparator: Comparator<T>) {
    private val size: Int = n;
    private val lockList = List(n) { ReentrantLock() }
    private val locks: List<Lock> = ArrayList(lockList)

    private val queuesList = List(n) { PriorityQueue<T>(comparator) }
    private val queues: List<PriorityQueue<T>> = ArrayList(queuesList)
    private val r = Random()
    var processElements = AtomicInteger(0)

    fun poll(): T? {
        var cur: T? = null
        val num: Int = r.nextInt(size)
        val q = queues[num]
        val lock: Lock = locks[num]
        if (lock.tryLock()) {
            try {
                cur = q.poll()
            } finally {
                lock.unlock();
            }
        }
        return cur
        /*while (true) {
            val num: Int = r.nextInt(size)
            val q = queues[num]
            val lock: Lock = locks[num]
            if (lock.tryLock()) {
                try {
                    cur = q.poll()
                } finally {
                    lock.unlock();
                }
            } else {
                continue
            }
            if (cur == null) {
                if (processElements.get() > 0) continue
            }
            else {
                //processElements.decrementAndGet();
                break;
            }
        }
        return cur;*/
    }


    fun add(elem: T): Boolean {
        var isAdded: Boolean = false
        while (true) {
            val num: Int = r.nextInt(size)
            val q = queues[num]
            val lock: Lock = locks[num]
            if (lock.tryLock()) {
                try {
                    isAdded = q.add(elem)
                } finally {
                    lock.unlock();
                }
            }
            if (isAdded) {
                processElements.incrementAndGet()
                break
            }
        }
        return isAdded
    }
}


// Returns `Integer.MAX_VALUE` if a path has not been found.
fun shortestPathParallel(start: Node) {
    val workers = Runtime.getRuntime().availableProcessors()

    // The distance to the start node is `0`
    start.distance = 0
    // Create a priority (by distance) queue and add the start node into it

    val q = MultiPriorityQueue(workers, NODE_DISTANCE_COMPARATOR) // TODO replace me with a multi-queue based PQ!
    q.add(start)

    // Run worker threads and wait until the total work is done
    val onFinish = Phaser(workers + 1) // `arrive()` should be invoked at the end by each worker
    repeat(workers) {
        thread {
            while (true) {
                // TODO Write the required algorithm here,
                // TODO break from this loop when there is no more node to process.
                // TODO Be careful, "empty queue" != "all nodes are processed".
                val cur: Node = q.poll()
                        ?: if (q.processElements.get() > 0) {
                            continue
                        } else {
                            break;
                        }
                for (e in cur.outgoingEdges) {
                    while (e.to.distance > cur.distance + e.weight) {
                        val distance: Int = e.to.distance
                        val updDistance: Int = cur.distance + e.weight
                        if (distance > updDistance) {
                            val updated: Boolean = e.to.casDistance(distance, updDistance)
                            if (updated) {
                                q.add(e.to)
                                break
                            }
                        }
                    }
                }
                q.processElements.decrementAndGet()

//                val cur: Node? = synchronized(q) { q.poll() }
//                if (cur == null) {
//                    if (workIsDone) break else continue
//                }
//                for (e in cur.outgoingEdges) {
//                    if (e.to.distance > cur.distance + e.weight) {
//                        e.to.distance = cur.distance + e.weight
//                        q.addOrDecreaseKey(e.to)
//                    }
//                }
            }
            onFinish.arrive()
        }
    }
    onFinish.arriveAndAwaitAdvance()
}