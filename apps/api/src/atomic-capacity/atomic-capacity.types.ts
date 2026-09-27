export type OperationState =
  | 'PENDING'
  | 'PROCESSING'
  | 'CONFIRMED'
  | 'REJECTED'
  | 'EXPIRED';

export type OperationReason =
  | 'CAPACITY_UNAVAILABLE'
  | 'INSUFFICIENT_FUNDS'
  | 'HOLD_EXPIRED';

export interface AcquireCommand {
  operationId: string;
  resourceId: string;
  actorId: string;
  walletId: string;
  amount: number;
  holdExpiresAt: Date;
}

export interface OperationResult {
  operationId: string;
  state: OperationState;
  reason: OperationReason | null;
  holdExpiresAt: Date;
}

export class IdempotencyConflictError extends Error {
  constructor(operationId: string) {
    super(`operation_id ${operationId} was already used with a different payload`);
    this.name = 'IdempotencyConflictError';
  }
}

export class OperationNotFoundError extends Error {
  constructor(operationId: string) {
    super(`operation_id ${operationId} was not found`);
    this.name = 'OperationNotFoundError';
  }
}

export class DataInvariantError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'DataInvariantError';
  }
}
