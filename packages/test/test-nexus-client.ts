import * as nexus from 'nexus-rpc';
import { Client, NexusResponse } from '@temporalio/client';

const myService = nexus.service('my-service', {
  myOp: nexus.operation<string, string>(),
});

const c = new Client({ namespace: 'my-namespace', /* connection */ });
const s = c.nexusService(myService, { endpoint: 'my-endpoint' });
// Alternatively connect to the same namespace.
// const s = c.nexusService(myService, { taskQueue: 'my-task-queue' });

const _output: string = await s.executeOperation(myService.operations.myOp, 'hello');

// The following options can be provided:
const _output2: string = await s.executeOperation(myService.operations.myOp, 'hello', {
  // links
  // headers
  // wait - milliseconds
  // requestId
  // abortSignal
  // callbackURL
  // callbackHeader
});

const { result, links }: NexusResponse<string> = await s.executeOperation(myService.operations.myOp, 'hello', {
  returnFullResponse: true,
});

// String literals with type inference also supported.
const handle = await s.startOperation('myOp', 'input');
const _handleFromToken = s.getHandle('myOp', handle.token);
// Get the service and operation names.
const { service, operation } = handle;
const _output4 = await handle.getResult();
const response2: NexusResponse<string> = await handle.getResult({
  // headers
  // abortSignal
  // wait
  returnFullResponse: true,
});
await handle.cancel();
const { state } = await handle.getInfo();
