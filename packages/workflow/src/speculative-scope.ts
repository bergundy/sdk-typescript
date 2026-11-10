import type { AsyncLocalStorage as ALS } from 'node:async_hooks';
import { IllegalStateError } from '@temporalio/common';
import { AsyncLocalStorage } from './cancellation-scope';
import { assertInWorkflowContext } from './global-attributes';

/**
 * A speculative block: a scope in which Activities and timers may start on this Worker right away,
 * before the Temporal Service knows about them. Create one with {@link speculative}.
 *
 * Like {@link CancellationScope}, the scope follows async context: Activities and timers started
 * by code running inside the block, including Promises started inside it and left running, are
 * covered while the block is {@link SpeculativeScope.active | active}. Once the block's function
 * settles, the scope is no longer active, so commands issued afterwards by work it left running are
 * sent normally.
 *
 * @experimental Speculative blocks are an experimental feature and may change without notice.
 */
export class SpeculativeScope {
  #state: 'new' | 'active' | 'exited' = 'new';

  /**
   * Whether the block is running, i.e. its function was called and hasn't settled yet.
   *
   * Activities and timers started while the current scope is active are marked as speculative.
   */
  public get active(): boolean {
    return this.#state === 'active';
  }

  /**
   * Activate the scope as current and run `fn` in it. A scope can only run once.
   *
   * The scope stops being active as soon as the Promise returned by `fn` settles. Exiting doesn't
   * wait for anything else, and code after the block doesn't wait for a round trip to the Service.
   *
   * @return the result of `fn`
   */
  run<T>(fn: () => Promise<T>): Promise<T> {
    const activator = assertInWorkflowContext('Workflow.speculative(...) may only be used from a Workflow Execution');
    if (this.#state !== 'new') {
      throw new IllegalStateError('A SpeculativeScope can only be run once');
    }
    this.#state = 'active';
    activator.activeSpeculativeBlocks++;
    const exit = () => {
      this.#state = 'exited';
      activator.activeSpeculativeBlocks--;
      activator.speculativeBlockExited = true;
    };
    return storage.run(this, this.runInContext.bind(this, fn, exit) as () => Promise<T>);
  }

  /**
   * Method that runs a function in AsyncLocalStorage context.
   *
   * Could have been written as anonymous function, made into a method for improved stack traces.
   */
  protected async runInContext<T>(fn: () => Promise<T>, exit: () => void): Promise<T> {
    try {
      return await fn();
    } finally {
      exit();
    }
  }

  /**
   * Get the innermost speculative scope of the calling code, whether or not it is still active,
   * or `undefined` when the code wasn't started inside a speculative block.
   */
  static current(): SpeculativeScope | undefined {
    return storage.getStore();
  }
}

const storage: ALS<SpeculativeScope> = new AsyncLocalStorage();

/**
 * Run `fn` in a speculative block, letting Activities and timers it starts begin right away on this
 * Worker, before the Temporal Service knows about them.
 *
 * This cuts latency for chains of short Activities and timers, much like Local Activities, while
 * history ends up with ordinary Activity and timer events. The Worker reports the work to the
 * Service at the next checkpoint. Work that's still running at that point (for example an Activity
 * still running when about 80% of the Workflow Task timeout has elapsed) is handed off to the
 * Service, and from then on it gets the usual server-side timeouts, heartbeats, cancellation and
 * visibility.
 *
 * ```ts
 * import { proxyActivities, sleep, speculative } from '@temporalio/workflow';
 * import type * as activities from './activities';
 *
 * const { charge, ship } = proxyActivities<typeof activities>({ startToCloseTimeout: '1 minute' });
 *
 * export async function order(id: string): Promise<void> {
 *   await speculative(async () => {
 *     await charge(id);
 *     await sleep('1 second');
 *     await ship(id);
 *   });
 *   // Exiting the block ends the speculation window, unless another speculative block is active.
 * }
 * ```
 *
 * Semantics:
 *
 * - Only Activities on the Workflow's own task queue (from {@link proxyActivities}) and timers
 *   ({@link sleep}, `setTimeout`, {@link condition} with a timeout, ...) may start speculatively.
 *   Other commands, such as child Workflows, are sent to the Service normally, which ends the
 *   speculation window.
 * - Whether a command actually starts speculatively is up to the SDK: it depends on free Activity
 *   slots, Service support, the time left in the Workflow Task, and so on. Workflow code behaves the
 *   same either way, and replay works with both kinds of history.
 * - The block follows async context, like a {@link CancellationScope}: Promises started inside it are
 *   covered while it's running, and stop being covered once `fn` settles.
 * - Progress inside a speculation window isn't durable until the next checkpoint. If the Worker
 *   crashes, the Workflow resumes from the last checkpoint and runs those Activities again, without
 *   increasing their attempt number, as with Local Activities.
 * - Signals, Updates, cancellation requests and results of other work that arrive during a window
 *   are delivered after the next checkpoint.
 * - Scheduling a Local Activity inside a speculative block fails the Workflow Task.
 * - Activities started speculatively can't complete asynchronously. Doing so fails the attempt, and
 *   the Activity is retried according to its retry policy.
 *
 * @param fn the code to run in the block.
 * @return the result of `fn`.
 *
 * @experimental Speculative blocks are an experimental feature and may change without notice.
 */
export function speculative<T>(fn: () => Promise<T>): Promise<T> {
  return new SpeculativeScope().run(fn);
}

/**
 * Whether Activities and timers started by the calling code should be marked as speculative.
 *
 * @internal
 */
export function inSpeculativeBlock(): boolean {
  return storage.getStore()?.active ?? false;
}
