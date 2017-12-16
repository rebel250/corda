package net.corda.node.services.events

import com.nhaarman.mockito_kotlin.*
import net.corda.core.contracts.*
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.FlowLogicRef
import net.corda.core.flows.FlowLogicRefFactory
import net.corda.core.utilities.days
import net.corda.testing.internal.rigorousMock
import net.corda.core.internal.FlowStateMachine
import net.corda.core.internal.concurrent.openFuture
import net.corda.core.internal.uncheckedCast
import net.corda.core.node.StateLoader
import net.corda.node.services.api.FlowStarter
import net.corda.nodeapi.internal.persistence.CordaPersistence
import net.corda.nodeapi.internal.persistence.DatabaseTransaction
import net.corda.testing.node.TestClock
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestWatcher
import org.junit.runner.Description
import org.slf4j.Logger
import java.time.Clock
import java.time.Instant
import java.util.concurrent.Executor

class NodeSchedulerServiceTest {
    private val mark = Instant.now()
    private val testClock = TestClock(rigorousMock<Clock>().also {
        doReturn(mark).whenever(it).instant()
    })
    private val database = rigorousMock<CordaPersistence>().also {
        doAnswer {
            val block: DatabaseTransaction.() -> Any? = uncheckedCast(it.arguments[0])
            rigorousMock<DatabaseTransaction>().block()
        }.whenever(it).transaction(any())
    }
    private val flowStarter = rigorousMock<FlowStarter>().also {
        doReturn(openFuture<FlowStateMachine<Any?>>()).whenever(it).startFlow(any<FlowLogic<*>>(), any())
    }
    private val stateLoader = rigorousMock<StateLoader>().also {
        doAnswer {
            val stateRef = it.arguments[0] as StateRef
            rigorousMock<TransactionState<ContractState>>().also {
                doReturn(rigorousMock<SchedulableState>().also {
                    doReturn(activities[stateRef]).whenever(it).nextScheduledActivity(same(stateRef)!!, any())
                }).whenever(it).data
            }
        }.whenever(it).loadState(any())
    }
    private val serverThread = rigorousMock<Executor>().also {
        doAnswer { (it.arguments[0] as Runnable).run() }.whenever(it).execute(any()) // XXX: Enough?
    }
    private val flowLogicRefFactory = rigorousMock<FlowLogicRefFactory>().also {
        doAnswer { flows[it.arguments[0]] }.whenever(it).toFlowLogic(any())
    }
    private val log = mock<Logger>().also {
        doReturn(true).whenever(it).isTraceEnabled
    }
    private val scheduler = NodeSchedulerService(testClock, database, flowStarter, stateLoader, serverThread = serverThread, flowLogicRefFactory = flowLogicRefFactory, log = log, scheduledStates = mutableMapOf()).apply {
        start()
    }
    private val activities = mutableMapOf<StateRef, ScheduledActivity>()
    private val flows = mutableMapOf<FlowLogicRef, FlowLogic<*>>()

    private class Scheduled(time: Instant) {
        val stateRef = rigorousMock<StateRef>()
        val flowLogic = rigorousMock<FlowLogic<*>>()
        val ssr = ScheduledStateRef(stateRef, time)
    }

    private fun schedule(time: Instant) = Scheduled(time).apply {
        val logicRef = rigorousMock<FlowLogicRef>()
        activities[stateRef] = ScheduledActivity(logicRef, time)
        flows[logicRef] = flowLogic
        scheduler.scheduleStateActivity(ssr)
    }

    private fun assertPending(scheduled: Scheduled) {
        verify(log, timeout(5000)).trace("Scheduling as next ${scheduled.ssr}")
    }

    private fun assertStarted(scheduled: Scheduled) {
        verify(flowStarter, timeout(5000)).startFlow(same(scheduled.flowLogic)!!, any())
    }

    @Rule
    @JvmField
    val tearDown = object : TestWatcher() {
        override fun succeeded(description: Description?) {
            scheduler.dispose()
            verifyNoMoreInteractions(flowStarter)
        }
    }

    @Test
    fun `test activity due now`() {
        assertStarted(schedule(mark))
    }

    @Test
    fun `test activity due in the past`() {
        assertStarted(schedule(mark - 1.days))
    }

    @Test
    fun `test activity due in the future`() {
        val scheduled = schedule(mark + 1.days)
        assertPending(scheduled)
        testClock.advanceBy(1.days)
        assertStarted(scheduled)
    }

    @Test
    fun `test activity due in the future and schedule another earlier`() {
        val scheduled2 = schedule(mark + 2.days)
        assertPending(scheduled2)
        val scheduled1 = schedule(mark + 1.days)
        assertPending(scheduled1)
        testClock.advanceBy(1.days)
        assertStarted(scheduled1)
        assertPending(scheduled2)
        testClock.advanceBy(1.days)
        assertStarted(scheduled2)
    }

    @Test
    fun `test activity due in the future and schedule another later`() {
        val scheduled1 = schedule(mark + 1.days)
        val scheduled2 = schedule(mark + 2.days)
        assertPending(scheduled1)
        testClock.advanceBy(1.days)
        assertStarted(scheduled1)
        assertPending(scheduled2)
        testClock.advanceBy(1.days)
        assertStarted(scheduled2)
    }

    @Test
    fun `test activity due in the future and schedule another for same time`() {
        val scheduledA = schedule(mark + 1.days)
        val scheduledB = schedule(mark + 1.days)
        assertPending(scheduledA)
        testClock.advanceBy(1.days)
        assertStarted(scheduledA)
        assertStarted(scheduledB)
    }

    @Test
    fun `test activity due in the future and schedule another for same time then unschedule original`() {
        val scheduledA = schedule(mark + 1.days)
        val scheduledB = schedule(mark + 1.days)
        scheduler.unscheduleStateActivity(scheduledA.stateRef)
        assertPending(scheduledB)
        testClock.advanceBy(1.days)
        assertStarted(scheduledB)
    }

    @Test
    fun `test activity due in the future then unschedule`() {
        scheduler.unscheduleStateActivity(schedule(mark + 1.days).stateRef)
        testClock.advanceBy(1.days)
    }
}
