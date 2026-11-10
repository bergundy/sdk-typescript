/**
 * Tests the commands and activation completions produced by speculative blocks
 *
 * @module
 */

import { proxyLocalActivities, sleep, speculative } from '@temporalio/workflow';
import type * as activities from '../activities';
import { httpGet } from './configured-activities';

export async function speculativeBlockCommands(): Promise<void> {
  await sleep(1);
  let leftRunning: Promise<void> | undefined;
  await speculative(async () => {
    // Started inside the block, the second sleep is issued after it exits
    leftRunning = sleep(2).then(() => sleep(4));
    await Promise.all([sleep(3), httpGet('https://temporal.io')]);
  });
  await leftRunning;
}

export async function speculativeNestedBlocks(): Promise<void> {
  await speculative(async () => {
    await speculative(() => sleep(1));
    await sleep(2);
  });
}

const { echo } = proxyLocalActivities<typeof activities>({ startToCloseTimeout: '1m' });

export async function speculativeLocalActivity(): Promise<void> {
  await speculative(async () => {
    try {
      await echo('hello');
    } catch (_err) {
      // Catching the error doesn't prevent the Workflow Task from failing
    }
  });
}
