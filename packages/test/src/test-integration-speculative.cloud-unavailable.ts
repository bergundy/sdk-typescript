/**
 * Tests for speculative blocks (optimistic start of Activities and timers).
 *
 * IMPORTANT: Except for the test that starts its own dev server, tests in this file require a server
 * whose `default` namespace has the `optimistic_start` capability (dynamic config
 * `system.enableOptimisticStart`). Run with e.g. `TEMPORAL_SERVICE_ADDRESS=localhost:27233`.
 */
import { setTimeout as setTimeoutPromise } from 'timers/promises';
import type { ExecutionContext } from 'ava';
import * as activity from '@temporalio/activity';
import type { WorkflowHandle } from '@temporalio/client';
import { ApplicationFailure } from '@temporalio/common';
import { tsToMs } from '@temporalio/common/lib/time';
import type { temporal } from '@temporalio/proto';
import * as wf from '@temporalio/workflow';
import { ActivityCancellationType } from '@temporalio/workflow';
import type { Context } from './helpers-integration';
import { createLocalTestEnvironment, helpers, makeTestFunction } from './helpers-integration';

const test = makeTestFunction({ workflowsPath: __filename });

interface Activities {
  act1(): Promise<string>;
  act2(): Promise<string>;
  act3(): Promise<string>;
  flaky(): Promise<string>;
  blockUntilReleased(): Promise<void>;
  waitForCancel(): Promise<void>;
}

const { act1, act2, act3, flaky, blockUntilReleased } = wf.proxyActivities<Activities>({
  startToCloseTimeout: '1 minute',
  retry: { initialInterval: '100ms', backoffCoefficient: 1, maximumAttempts: 3 },
});

const { act1: localAct1 } = wf.proxyLocalActivities<Activities>({ startToCloseTimeout: '1 minute' });

////////////////////////////////////////////////////////////////////////////////////////////////////
// Workflows
////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * The example from the design: two Activities resolving out of order, a timer, then an Activity
 * that is still running when the speculation window reaches its deadline.
 */
export async function speculativeExample(): Promise<string[]> {
  const observed: string[] = [];
  await wf.speculative(async () => {
    const a1 = act1().then((r) => observed.push(r));
    const a2 = act2().then((r) => observed.push(r));
    await Promise.all([a1, a2]);
    await wf.sleep('300ms');
    observed.push(await act3());
  });
  return observed;
}

/**
 * Loops on workflow time inside a speculative block. Without workflow time moving forward across
 * local timers, the loop would spin until the window's deadline.
 */
export async function speculativeTimeLoop(): Promise<{ start: number; end: number; iterations: number }> {
  const start = Date.now();
  let iterations = 0;
  await wf.speculative(async () => {
    while (Date.now() < start + 500) {
      await wf.sleep(100);
      iterations++;
    }
  });
  return { start, end: Date.now(), iterations };
}

export async function speculativeFlakyActivity(): Promise<string> {
  return await wf.speculative(() => flaky());
}

export const releaseSignal = wf.defineSignal('release');

export async function speculativeSignalDuringWindow(): Promise<string[]> {
  const log: string[] = [];
  let signaled = false;
  wf.setHandler(releaseSignal, () => {
    signaled = true;
    log.push('signal');
  });
  await wf.speculative(async () => {
    await blockUntilReleased();
    log.push(`activity completed, signaled=${signaled}`);
  });
  await wf.condition(() => signaled);
  return log;
}

export async function speculativeLocalActivity(): Promise<void> {
  await wf.speculative(async () => {
    try {
      await localAct1();
    } catch (_err) {
      // Catching the error must not prevent the Workflow Task from failing
    }
  });
}

export async function speculativeCancelActivity(cancellationType: ActivityCancellationType): Promise<string> {
  const { waitForCancel } = wf.proxyActivities<Activities>({ startToCloseTimeout: '1 minute', cancellationType });
  return await wf.speculative(async () => {
    const scope = new wf.CancellationScope();
    const promise = scope.run(() => waitForCancel());
    // Let the Activity start, inside the same window
    await wf.sleep(200);
    scope.cancel();
    try {
      await promise;
      return 'completed';
    } catch (err) {
      if (wf.isCancellation(err)) return 'cancelled';
      throw err;
    }
  });
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////

type HistoryEvent = temporal.api.history.v1.IHistoryEvent;

function isOptimisticToken(taskToken: Uint8Array): boolean {
  return Buffer.from(taskToken).toString('latin1').startsWith('optimistic_act_');
}

/**
 * Describe an event as `<Type> [<details>] [optimistic]`, with the Activity type for scheduled
 * events, the scheduled event ID for other Activity events, and `optimistic` if the event carries
 * an optimistic start or resolution.
 */
function describeEvent(e: HistoryEvent): string {
  const key = Object.keys(e).find((k) => k.endsWith('EventAttributes') && (e as any)[k] != null);
  if (key === undefined) throw new Error(`Event without attributes: ${JSON.stringify(e)}`);
  const type = key[0]!.toUpperCase() + key.slice(1, -'EventAttributes'.length);
  const attrs = (e as any)[key];
  const parts = [type];
  if (e.activityTaskScheduledEventAttributes) {
    parts.push(e.activityTaskScheduledEventAttributes.activityType!.name!);
  } else if (attrs.scheduledEventId != null && type.startsWith('ActivityTask')) {
    parts.push(`${attrs.scheduledEventId}`);
  }
  if (attrs.optimisticStart || attrs.optimisticResolution) {
    parts.push('optimistic');
  }
  return parts.join(' ');
}

function describeHistory(history: temporal.api.history.v1.IHistory): string[] {
  return (history.events ?? []).map(describeEvent);
}

async function assertOptimisticStartCapability(t: ExecutionContext<Context>): Promise<void> {
  const { client } = t.context.env;
  const { namespaceInfo } = await client.workflowService.describeNamespace({ namespace: client.options.namespace });
  t.true(
    namespaceInfo?.capabilities?.optimisticStart,
    `Namespace '${client.options.namespace}' must have the optimistic_start capability`
  );
}

async function assertWorkflowTaskFailed(
  t: ExecutionContext<Context>,
  handle: WorkflowHandle,
  expectedMessage: RegExp
): Promise<void> {
  const deadline = Date.now() + 10_000;
  for (;;) {
    const history = await handle.fetchHistory();
    const failure = history.events?.find((e) => e.workflowTaskFailedEventAttributes)?.workflowTaskFailedEventAttributes
      ?.failure;
    if (failure) {
      t.regex(failure.message ?? '', expectedMessage);
      return;
    }
    if (Date.now() > deadline) {
      t.fail('Workflow Task did not fail');
      return;
    }
    await setTimeoutPromise(200);
  }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////

for (const maxCachedWorkflows of [undefined, 0]) {
  const title = maxCachedWorkflows === 0 ? ' without a Workflow cache' : '';
  test(`Speculative block runs the design example and hands off a long Activity${title}`, async (t) => {
    await assertOptimisticStartCapability(t);
    const { createWorker, startWorkflow, runReplayHistory } = helpers(t);

    const tokens: Record<string, boolean> = {};
    let resolveAct2Done!: () => void;
    const act2Done = new Promise<void>((resolve) => (resolveAct2Done = resolve));
    const worker = await createWorker({
      maxCachedWorkflows,
      activities: {
        async act1() {
          tokens.act1 = isOptimisticToken(activity.activityInfo().taskToken);
          // Complete after act2, so the workflow observes act2 first
          await act2Done;
          await setTimeoutPromise(200);
          return 'act1';
        },
        async act2() {
          tokens.act2 = isOptimisticToken(activity.activityInfo().taskToken);
          await setTimeoutPromise(50);
          resolveAct2Done();
          return 'act2';
        },
        async act3() {
          tokens.act3 = isOptimisticToken(activity.activityInfo().taskToken);
          // Still running at 80% of the Workflow Task timeout, so it's handed off to the server
          await setTimeoutPromise(4000);
          return 'act3';
        },
      },
    });

    const handle = await startWorkflow(speculativeExample, { workflowTaskTimeout: '3s' });
    const result = await worker.runUntil(handle.result());
    t.deepEqual(result, ['act2', 'act1', 'act3']);
    t.deepEqual(tokens, { act1: true, act2: true, act3: true });

    const history = await handle.fetchHistory();
    t.deepEqual(describeHistory(history), [
      'WorkflowExecutionStarted',
      'WorkflowTaskScheduled',
      'WorkflowTaskStarted',
      'WorkflowTaskCompleted',
      'ActivityTaskScheduled act1 optimistic',
      'ActivityTaskScheduled act2 optimistic',
      'ActivityTaskStarted 6 optimistic',
      'ActivityTaskCompleted 6 optimistic',
      'ActivityTaskStarted 5 optimistic',
      'ActivityTaskCompleted 5 optimistic',
      'TimerStarted optimistic',
      'TimerFired optimistic',
      'ActivityTaskScheduled act3 optimistic',
      // act3 was handed off at the checkpoint, and completed through the normal RPC
      'ActivityTaskStarted 13',
      'ActivityTaskCompleted 13',
      'WorkflowTaskScheduled',
      'WorkflowTaskStarted',
      'WorkflowTaskCompleted',
      'WorkflowExecutionCompleted',
    ]);

    // Workflow time recorded on resolutions moves forward, and the timer took at least its duration
    const events = history.events!;
    const resolutionTimes = events
      .map(
        (e) =>
          e.activityTaskCompletedEventAttributes?.optimisticResolution?.workflowTime ??
          e.timerFiredEventAttributes?.optimisticResolution?.workflowTime
      )
      .filter((time) => time != null)
      .map((time) => tsToMs(time));
    t.is(resolutionTimes.length, 3);
    t.deepEqual(
      resolutionTimes,
      [...resolutionTimes].sort((a, b) => a - b)
    );
    const timerStarted = tsToMs(events[10]!.timerStartedEventAttributes!.optimisticStart!.startTime);
    t.true(resolutionTimes[2]! - timerStarted >= 300);
    // The Workflow Task didn't time out: the window checkpointed before its deadline
    const wftStarted = tsToMs(events[2]!.eventTime);
    const wftCompleted = tsToMs(events[3]!.eventTime);
    t.true(wftCompleted - wftStarted < 3000, `Checkpoint after ${wftCompleted - wftStarted}ms`);

    await runReplayHistory({}, history);
  });
}

test('Workflow time advances across speculative timers', async (t) => {
  await assertOptimisticStartCapability(t);
  const { createWorker, startWorkflow, runReplayHistory } = helpers(t);
  const worker = await createWorker();
  const handle = await startWorkflow(speculativeTimeLoop);
  const { start, end, iterations } = await worker.runUntil(handle.result());
  t.true(end - start >= 500);
  // Each timer advances workflow time by at least its duration
  t.true(iterations >= 1 && iterations <= 5, `${iterations} iterations`);

  const history = await handle.fetchHistory();
  const described = describeHistory(history);
  t.is(described.filter((d) => d === 'TimerStarted optimistic').length, iterations);
  t.is(described.filter((d) => d === 'TimerFired optimistic').length, iterations);
  // All in a single Workflow Task
  t.is(described.filter((d) => d === 'WorkflowTaskCompleted').length, 1);
  // Workflow time when the loop ended is the time recorded on the last timer resolution
  const lastFired = history.events!.findLast((e) => e.timerFiredEventAttributes)!;
  t.is(end, tsToMs(lastFired.timerFiredEventAttributes!.optimisticResolution!.workflowTime));

  await runReplayHistory({}, history);
});

test('Failed speculative Activity is retried by the server', async (t) => {
  await assertOptimisticStartCapability(t);
  const { createWorker, startWorkflow, runReplayHistory } = helpers(t);
  const attempts: { attempt: number; optimistic: boolean }[] = [];
  const worker = await createWorker({
    activities: {
      async flaky() {
        const info = activity.activityInfo();
        attempts.push({ attempt: info.attempt, optimistic: isOptimisticToken(info.taskToken) });
        if (info.attempt === 1) throw ApplicationFailure.retryable('first attempt fails');
        return 'ok';
      },
    },
  });
  const handle = await startWorkflow(speculativeFlakyActivity);
  t.is(await worker.runUntil(handle.result()), 'ok');
  t.deepEqual(attempts, [
    { attempt: 1, optimistic: true },
    { attempt: 2, optimistic: false },
  ]);

  const history = await handle.fetchHistory();
  const described = describeHistory(history);
  t.true(described.includes('ActivityTaskScheduled flaky optimistic'));
  const started = history.events!.find(
    (e) => e.activityTaskStartedEventAttributes
  )!.activityTaskStartedEventAttributes!;
  t.is(started.attempt, 2);
  t.falsy(started.optimisticResolution);
  t.is(started.lastFailure?.message, 'first attempt fails');

  await runReplayHistory({}, history);
});

test('Signal received during a speculation window is delivered after the checkpoint', async (t) => {
  await assertOptimisticStartCapability(t);
  const { createWorker, startWorkflow, runReplayHistory } = helpers(t);
  let resolveStarted!: () => void;
  const started = new Promise<void>((resolve) => (resolveStarted = resolve));
  let release!: () => void;
  const released = new Promise<void>((resolve) => (release = resolve));
  const worker = await createWorker({
    activities: {
      async blockUntilReleased() {
        t.true(isOptimisticToken(activity.activityInfo().taskToken));
        resolveStarted();
        await released;
      },
    },
  });
  const handle = await startWorkflow(speculativeSignalDuringWindow);
  const result = await worker.runUntil(async () => {
    await started;
    await handle.signal(releaseSignal);
    release();
    return await handle.result();
  });
  t.deepEqual(result, ['activity completed, signaled=false', 'signal']);

  const history = await handle.fetchHistory();
  const described = describeHistory(history);
  const completed = described.indexOf('ActivityTaskCompleted 5 optimistic');
  const signaled = described.indexOf('WorkflowExecutionSignaled');
  t.true(completed > 0 && signaled > completed, JSON.stringify(described));

  await runReplayHistory({}, history);
});

test('Local Activity inside a speculative block fails the Workflow Task', async (t) => {
  const { createWorker, startWorkflow } = helpers(t);
  const worker = await createWorker({
    activities: {
      async act1() {
        return 'act1';
      },
    },
  });
  const handle = await startWorkflow(speculativeLocalActivity);
  await worker.runUntil(async () => {
    await assertWorkflowTaskFailed(t, handle, /can't be scheduled inside a speculative block/);
    await handle.terminate();
  });
});

for (const cancellationType of [
  ActivityCancellationType.TRY_CANCEL,
  ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
]) {
  test(`Speculative Activity can be cancelled (${cancellationType})`, async (t) => {
    await assertOptimisticStartCapability(t);
    const { createWorker, startWorkflow, runReplayHistory } = helpers(t);
    let resolveCancelled!: () => void;
    const activityCancelled = new Promise<void>((resolve) => (resolveCancelled = resolve));
    const worker = await createWorker({
      activities: {
        async waitForCancel() {
          t.true(isOptimisticToken(activity.activityInfo().taskToken));
          try {
            await activity.Context.current().cancelled;
          } finally {
            resolveCancelled();
          }
        },
      },
    });
    const handle = await startWorkflow(speculativeCancelActivity, { args: [cancellationType] });
    const result = await worker.runUntil(async () => {
      const result = await handle.result();
      await activityCancelled;
      return result;
    });
    t.is(result, 'cancelled');

    const history = await handle.fetchHistory();
    const described = describeHistory(history);
    const expectedPrefix = [
      'WorkflowExecutionStarted',
      'WorkflowTaskScheduled',
      'WorkflowTaskStarted',
      'WorkflowTaskCompleted',
      'ActivityTaskScheduled waitForCancel optimistic',
      'TimerStarted optimistic',
      'TimerFired optimistic',
      'ActivityTaskCancelRequested 5',
    ];
    t.deepEqual(described.slice(0, expectedPrefix.length), expectedPrefix);
    if (cancellationType === ActivityCancellationType.WAIT_CANCELLATION_COMPLETED) {
      // The workflow observed the cancellation inside the window
      t.deepEqual(described.slice(expectedPrefix.length), [
        'ActivityTaskStarted 5 optimistic',
        'ActivityTaskCanceled 5 optimistic',
        'WorkflowExecutionCompleted',
      ]);
    } else {
      t.deepEqual(described.slice(expectedPrefix.length), ['WorkflowExecutionCompleted']);
    }

    await runReplayHistory({}, history);
  });
}

test('Speculative block runs normally against a server without optimistic start', async (t) => {
  const env = await createLocalTestEnvironment();
  try {
    const { namespaceInfo } = await env.client.workflowService.describeNamespace({
      namespace: env.client.options.namespace,
    });
    t.falsy(namespaceInfo?.capabilities?.optimisticStart);

    const { createWorker, startWorkflow, runReplayHistory } = helpers(t, env);
    const tokens: boolean[] = [];
    const worker = await createWorker({
      activities: {
        async act1() {
          tokens.push(isOptimisticToken(activity.activityInfo().taskToken));
          await setTimeoutPromise(100);
          return 'act1';
        },
        async act2() {
          tokens.push(isOptimisticToken(activity.activityInfo().taskToken));
          return 'act2';
        },
        async act3() {
          tokens.push(isOptimisticToken(activity.activityInfo().taskToken));
          return 'act3';
        },
      },
    });
    const handle = await startWorkflow(speculativeExample);
    const result = await worker.runUntil(handle.result());
    t.deepEqual(result, ['act2', 'act1', 'act3']);
    t.deepEqual(tokens, [false, false, false]);

    const history = await handle.fetchHistory();
    const described = describeHistory(history);
    t.false(
      described.some((d) => d.endsWith(' optimistic')),
      JSON.stringify(described)
    );
    t.true(described.filter((d) => d === 'WorkflowTaskCompleted').length >= 4);

    await runReplayHistory({}, history);
  } finally {
    await env.teardown();
  }
});
