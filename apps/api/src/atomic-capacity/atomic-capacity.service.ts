import { Inject, Injectable } from '@nestjs/common';
import type { PoolClient, QueryResultRow } from 'pg';

import { DatabaseService } from '../database/database.service';
import { CLOCK } from './clock';
import type { Clock } from './clock';
import {
  DataInvariantError,
  IdempotencyConflictError,
  OperationNotFoundError,
} from './atomic-capacity.types';
import type {
  AcquireCommand,
  OperationReason,
  OperationResult,
  OperationState,
} from './atomic-capacity.types';

interface OperationRow extends QueryResultRow {
  operation_id: string;
  resource_id: string;
  wallet_id: string;
  amount: string;
  state: OperationState;
  reason: OperationReason | null;
  hold_expires_at: Date;
}

interface OperationSnapshotRow extends OperationRow {
  payload_matches: boolean;
}

interface CapacityRow extends QueryResultRow {
  capacity: number;
}

interface CapacityUsageRow extends QueryResultRow {
  used: number;
}

@Injectable()
export class AtomicCapacityService {
  constructor(
    private readonly database: DatabaseService,
    @Inject(CLOCK) private readonly clock: Clock,
  ) {}

  async acquire(command: AcquireCommand): Promise<OperationResult> {
    this.validateCommand(command);

    const payload = this.payloadFor(command);

    return this.database.transactionWithDeadlockRetry(async (client) => {
      const initial = await this.getOperationSnapshot(
        client,
        command.operationId,
        payload,
      );

      if (initial && !initial.payload_matches) {
        throw new IdempotencyConflictError(command.operationId);
      }

      if (initial && this.isTerminal(initial.state)) {
        return this.toResult(initial);
      }

      if (!initial) {
        const insertNow = this.clock.now();

        await client.query(
          `
            INSERT INTO issue3_lab.operations (
              operation_id,
              resource_id,
              actor_id,
              wallet_id,
              amount,
              payload,
              state,
              reason,
              hold_expires_at,
              created_at,
              updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6::jsonb, 'PENDING', NULL, $7, $8, $8)
            ON CONFLICT (operation_id) DO NOTHING
          `,
          [
            command.operationId,
            command.resourceId,
            command.actorId,
            command.walletId,
            String(command.amount),
            payload,
            command.holdExpiresAt,
            insertNow,
          ],
        );
      }

      const persisted = await this.getOperationSnapshot(
        client,
        command.operationId,
        payload,
      );

      if (!persisted) {
        throw new DataInvariantError('operation disappeared after idempotent insert');
      }

      if (!persisted.payload_matches) {
        throw new IdempotencyConflictError(command.operationId);
      }

      if (this.isTerminal(persisted.state)) {
        return this.toResult(persisted);
      }

      await this.lockWallet(client, persisted.wallet_id);
      const resource = await this.lockResource(client, persisted.resource_id);

      const lockedRows = await this.lockWalletOperations(
        client,
        persisted.wallet_id,
        command.operationId,
        payload,
      );

      const current = lockedRows.find(
        (operation) => operation.operation_id === command.operationId,
      );

      if (!current) {
        throw new OperationNotFoundError(command.operationId);
      }

      if (!current.payload_matches) {
        throw new IdempotencyConflictError(command.operationId);
      }

      const decisionNow = this.clock.now();

      if (this.isTerminal(current.state)) {
        return this.toResult(current);
      }

      if (current.state === 'PROCESSING') {
        if (this.isExpired(current, decisionNow)) {
          const expired = await this.expireProcessingOperation(
            client,
            current,
            decisionNow,
          );
          return this.toResult(expired);
        }

        return this.toResult(current);
      }

      if (current.state !== 'PENDING') {
        throw new DataInvariantError(
          `operation ${current.operation_id} cannot acquire from state ${current.state}`,
        );
      }

      if (this.isExpired(current, decisionNow)) {
        const expired = await this.updateOperation(
          client,
          current.operation_id,
          'EXPIRED',
          'HOLD_EXPIRED',
          decisionNow,
        );
        return this.toResult(expired);
      }

      for (const operation of lockedRows) {
        if (
          operation.operation_id !== current.operation_id &&
          operation.state === 'PROCESSING' &&
          this.isExpired(operation, decisionNow)
        ) {
          await this.expireProcessingOperation(client, operation, decisionNow);
        }
      }

      const usageResult = await client.query<CapacityUsageRow>(
        `
          SELECT COUNT(*)::int AS used
          FROM issue3_lab.operations
          WHERE resource_id = $1
            AND (
              state = 'CONFIRMED'
              OR (state = 'PROCESSING' AND hold_expires_at > $2)
            )
        `,
        [current.resource_id, decisionNow],
      );

      const used = usageResult.rows[0]?.used ?? 0;

      if (used >= resource.capacity) {
        const rejected = await this.updateOperation(
          client,
          current.operation_id,
          'REJECTED',
          'CAPACITY_UNAVAILABLE',
          decisionNow,
        );
        return this.toResult(rejected);
      }

      const reserveResult = await client.query(
        `
          UPDATE issue3_lab.synthetic_wallets
          SET
            available_amount = available_amount - $2::bigint,
            reserved_amount = reserved_amount + $2::bigint
          WHERE wallet_id = $1
            AND available_amount >= $2::bigint
        `,
        [current.wallet_id, current.amount],
      );

      if ((reserveResult.rowCount ?? 0) !== 1) {
        const rejected = await this.updateOperation(
          client,
          current.operation_id,
          'REJECTED',
          'INSUFFICIENT_FUNDS',
          decisionNow,
        );
        return this.toResult(rejected);
      }

      await client.query(
        `
          INSERT INTO issue3_lab.synthetic_ledger (
            operation_id,
            wallet_id,
            event_type,
            amount,
            created_at
          )
          VALUES ($1, $2, 'RESERVED', $3::bigint, $4)
        `,
        [current.operation_id, current.wallet_id, current.amount, decisionNow],
      );

      const processing = await this.updateOperation(
        client,
        current.operation_id,
        'PROCESSING',
        null,
        decisionNow,
      );

      return this.toResult(processing);
    });
  }

  async confirm(operationId: string): Promise<OperationResult> {
    if (!operationId.trim()) {
      throw new TypeError('operationId is required');
    }

    return this.database.transactionWithDeadlockRetry(async (client) => {
      const snapshot = await this.getOperation(client, operationId);

      if (!snapshot) {
        throw new OperationNotFoundError(operationId);
      }

      if (this.isTerminal(snapshot.state)) {
        return this.toResult(snapshot);
      }

      await this.lockWallet(client, snapshot.wallet_id);
      await this.lockResource(client, snapshot.resource_id);

      const current = await this.lockOperation(client, operationId);
      const decisionNow = this.clock.now();

      if (this.isTerminal(current.state)) {
        return this.toResult(current);
      }

      if (current.state !== 'PROCESSING') {
        throw new DataInvariantError(
          `operation ${operationId} cannot confirm from state ${current.state}`,
        );
      }

      if (this.isExpired(current, decisionNow)) {
        const expired = await this.expireProcessingOperation(
          client,
          current,
          decisionNow,
        );
        return this.toResult(expired);
      }

      const consumeResult = await client.query(
        `
          UPDATE issue3_lab.synthetic_wallets
          SET
            reserved_amount = reserved_amount - $2::bigint,
            consumed_amount = consumed_amount + $2::bigint
          WHERE wallet_id = $1
            AND reserved_amount >= $2::bigint
        `,
        [current.wallet_id, current.amount],
      );

      if ((consumeResult.rowCount ?? 0) !== 1) {
        throw new DataInvariantError(
          `wallet ${current.wallet_id} does not contain the reserved synthetic balance`,
        );
      }

      await client.query(
        `
          INSERT INTO issue3_lab.synthetic_ledger (
            operation_id,
            wallet_id,
            event_type,
            amount,
            created_at
          )
          VALUES ($1, $2, 'CONSUMED', $3::bigint, $4)
        `,
        [current.operation_id, current.wallet_id, current.amount, decisionNow],
      );

      const confirmed = await this.updateOperation(
        client,
        current.operation_id,
        'CONFIRMED',
        null,
        decisionNow,
      );

      return this.toResult(confirmed);
    });
  }

  private async getOperationSnapshot(
    client: PoolClient,
    operationId: string,
    payload: string,
  ): Promise<OperationSnapshotRow | undefined> {
    const result = await client.query<OperationSnapshotRow>(
      `
        SELECT
          operation_id,
          resource_id,
          wallet_id,
          amount,
          state,
          reason,
          hold_expires_at,
          payload = $2::jsonb AS payload_matches
        FROM issue3_lab.operations
        WHERE operation_id = $1
      `,
      [operationId, payload],
    );

    return result.rows[0];
  }

  private async getOperation(
    client: PoolClient,
    operationId: string,
  ): Promise<OperationRow | undefined> {
    const result = await client.query<OperationRow>(
      `
        SELECT
          operation_id,
          resource_id,
          wallet_id,
          amount,
          state,
          reason,
          hold_expires_at
        FROM issue3_lab.operations
        WHERE operation_id = $1
      `,
      [operationId],
    );

    return result.rows[0];
  }

  private async lockWallet(client: PoolClient, walletId: string): Promise<void> {
    const result = await client.query(
      `
        SELECT wallet_id
        FROM issue3_lab.synthetic_wallets
        WHERE wallet_id = $1
        FOR UPDATE
      `,
      [walletId],
    );

    if ((result.rowCount ?? 0) !== 1) {
      throw new DataInvariantError(`wallet ${walletId} does not exist`);
    }
  }

  private async lockResource(
    client: PoolClient,
    resourceId: string,
  ): Promise<CapacityRow> {
    const result = await client.query<CapacityRow>(
      `
        SELECT capacity
        FROM issue3_lab.capacity_resources
        WHERE resource_id = $1
        FOR UPDATE
      `,
      [resourceId],
    );

    const resource = result.rows[0];

    if (!resource) {
      throw new DataInvariantError(`resource ${resourceId} does not exist`);
    }

    return resource;
  }

  private async lockWalletOperations(
    client: PoolClient,
    walletId: string,
    operationId: string,
    payload: string,
  ): Promise<OperationSnapshotRow[]> {
    const result = await client.query<OperationSnapshotRow>(
      `
        SELECT
          operation_id,
          resource_id,
          wallet_id,
          amount,
          state,
          reason,
          hold_expires_at,
          payload = $3::jsonb AS payload_matches
        FROM issue3_lab.operations
        WHERE wallet_id = $1
          AND (operation_id = $2 OR state = 'PROCESSING')
        ORDER BY operation_id
        FOR UPDATE
      `,
      [walletId, operationId, payload],
    );

    return result.rows;
  }

  private async lockOperation(
    client: PoolClient,
    operationId: string,
  ): Promise<OperationRow> {
    const result = await client.query<OperationRow>(
      `
        SELECT
          operation_id,
          resource_id,
          wallet_id,
          amount,
          state,
          reason,
          hold_expires_at
        FROM issue3_lab.operations
        WHERE operation_id = $1
        FOR UPDATE
      `,
      [operationId],
    );

    const operation = result.rows[0];

    if (!operation) {
      throw new OperationNotFoundError(operationId);
    }

    return operation;
  }

  private async expireProcessingOperation(
    client: PoolClient,
    operation: OperationRow,
    now: Date,
  ): Promise<OperationRow> {
    if (operation.state !== 'PROCESSING') {
      throw new DataInvariantError(
        `operation ${operation.operation_id} cannot expire reserved balance from state ${operation.state}`,
      );
    }

    const releaseResult = await client.query(
      `
        UPDATE issue3_lab.synthetic_wallets
        SET
          reserved_amount = reserved_amount - $2::bigint,
          available_amount = available_amount + $2::bigint
        WHERE wallet_id = $1
          AND reserved_amount >= $2::bigint
      `,
      [operation.wallet_id, operation.amount],
    );

    if ((releaseResult.rowCount ?? 0) !== 1) {
      throw new DataInvariantError(
        `wallet ${operation.wallet_id} cannot release expired reservation`,
      );
    }

    await client.query(
      `
        INSERT INTO issue3_lab.synthetic_ledger (
          operation_id,
          wallet_id,
          event_type,
          amount,
          created_at
        )
        VALUES ($1, $2, 'RELEASED', $3::bigint, $4)
        ON CONFLICT (operation_id, event_type) DO NOTHING
      `,
      [operation.operation_id, operation.wallet_id, operation.amount, now],
    );

    return this.updateOperation(
      client,
      operation.operation_id,
      'EXPIRED',
      'HOLD_EXPIRED',
      now,
    );
  }

  private async updateOperation(
    client: PoolClient,
    operationId: string,
    state: OperationState,
    reason: OperationReason | null,
    now: Date,
  ): Promise<OperationRow> {
    const result = await client.query<OperationRow>(
      `
        UPDATE issue3_lab.operations
        SET state = $2, reason = $3, updated_at = $4
        WHERE operation_id = $1
        RETURNING
          operation_id,
          resource_id,
          wallet_id,
          amount,
          state,
          reason,
          hold_expires_at
      `,
      [operationId, state, reason, now],
    );

    const operation = result.rows[0];

    if (!operation) {
      throw new OperationNotFoundError(operationId);
    }

    return operation;
  }

  private isExpired(operation: OperationRow, now: Date): boolean {
    return operation.hold_expires_at.getTime() <= now.getTime();
  }

  private isTerminal(state: OperationState): boolean {
    return state === 'CONFIRMED' || state === 'REJECTED' || state === 'EXPIRED';
  }

  private toResult(operation: OperationRow): OperationResult {
    return {
      operationId: operation.operation_id,
      state: operation.state,
      reason: operation.reason,
      holdExpiresAt: new Date(operation.hold_expires_at),
    };
  }

  private payloadFor(command: AcquireCommand): string {
    return JSON.stringify({
      resource_id: command.resourceId,
      actor_id: command.actorId,
      wallet_id: command.walletId,
      amount: command.amount,
      hold_expires_at: command.holdExpiresAt.toISOString(),
    });
  }

  private validateCommand(command: AcquireCommand): void {
    if (!command.operationId.trim()) {
      throw new TypeError('operationId is required');
    }

    if (!command.resourceId.trim()) {
      throw new TypeError('resourceId is required');
    }

    if (!command.actorId.trim()) {
      throw new TypeError('actorId is required');
    }

    if (!command.walletId.trim()) {
      throw new TypeError('walletId is required');
    }

    if (!Number.isSafeInteger(command.amount) || command.amount <= 0) {
      throw new TypeError('amount must be a positive safe integer');
    }

    if (Number.isNaN(command.holdExpiresAt.getTime())) {
      throw new TypeError('holdExpiresAt must be a valid date');
    }
  }
}
