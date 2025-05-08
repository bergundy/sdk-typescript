import {
  CancelOperationOptions,
  GetOperationInfoOptions,
  GetOperationResultOptions,
  Link,
  OperationInfo,
  OperationInput,
  OperationKey,
  OperationOutput,
  Service,
  StartOperationOptions,
} from "nexus-rpc";

export type NexusClientOptions = {
  endpoint: string;
} | {
  taskQueue: string;
}

// TODO: This should move to the Nexus SDK.
export interface ExecuteOperationOptions extends StartOperationOptions {
  // Duration in milliseconds to wait for operation completion.
  //
  // ⚠ NOTE: unlike GetOperationResultOptions.wait, zero and negative values are considered effectively infinite.
  wait: number;
}

export interface NexusClient<T extends Service> {
  executeOperation<O extends T["operations"][keyof T["operations"]]>(
    op: O,
    input: OperationInput<O>,
    options?: Partial<ExecuteOperationOptions>,
  ): Promise<OperationOutput<O>>;

  executeOperation<O extends T["operations"][keyof T["operations"]]>(
    op: O,
    input: OperationInput<O>,
    options: Partial<ExecuteOperationOptions> & { returnFullResponse: true },
  ): Promise<NexusResponse<OperationOutput<O>>>;

  executeOperation<K extends OperationKey<T["operations"]>>(
    op: K,
    input: OperationInput<T["operations"][K]>,
    options?: Partial<ExecuteOperationOptions>,
  ): Promise<OperationOutput<T["operations"][K]>>;

  executeOperation<K extends OperationKey<T["operations"]>>(
    op: K,
    input: OperationInput<T["operations"][K]>,
    options: Partial<ExecuteOperationOptions> & { returnFullResponse: true },
  ): Promise<NexusResponse<OperationOutput<T["operations"][K]>>>;

  startOperation<O extends T["operations"][keyof T["operations"]]>(
    op: O,
    input: OperationInput<O>,
    options?: Partial<StartOperationOptions>,
  ): Promise<NexusOperationHandle<OperationOutput<O>>>;

  startOperation<O extends T["operations"][keyof T["operations"]]>(
    op: O,
    input: OperationInput<O>,
    options: Partial<StartOperationOptions> & { returnFullResponse: true },
  ): Promise<NexusResponse<NexusOperationHandle<OperationOutput<O>>>>;

  startOperation<K extends OperationKey<T["operations"]>>(
    op: K,
    input: OperationInput<T["operations"][K]>,
    options?: Partial<StartOperationOptions>,
  ): Promise<NexusOperationHandle<OperationOutput<T["operations"][K]>>>;

  startOperation<K extends OperationKey<T["operations"]>>(
    op: K,
    input: OperationInput<T["operations"][K]>,
    options: Partial<StartOperationOptions> & { returnFullResponse: true },
  ): Promise<NexusResponse<NexusOperationHandle<OperationOutput<T["operations"][K]>>>>;

  getHandle<O extends T["operations"][keyof T["operations"]]>(
    op: O,
    token: string,
  ): NexusOperationHandle<OperationOutput<O>>;

  getHandle<K extends OperationKey<T["operations"]>>(
    op: K,
    token: string,
  ): NexusOperationHandle<OperationOutput<T["operations"][K]>>;
}

export interface NexusOperationHandle<T> {
  readonly service: string;
  readonly operation: string;
  readonly token: string;

  getResult(options?: Partial<GetOperationResultOptions>): Promise<T>;
  getResult(options: Partial<GetOperationResultOptions> & { returnFullResponse: true }): Promise<NexusResponse<T>>;
  getInfo(options?: Partial<GetOperationInfoOptions>): Promise<OperationInfo>;
  cancel(options?: Partial<CancelOperationOptions>): Promise<void>;
}

export interface NexusResponse<T> {
  result: T;
  links: Link[];
}
