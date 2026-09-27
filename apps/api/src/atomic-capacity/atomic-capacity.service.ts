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

    const now = this.clock.now();
    const payload = this.payloadFor(command);

    return this.database.transaction(async (client) => {
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
          now,
        ],
      );

      const snapshotResult = await client.query<OperationSnapshotRow>(
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
        [command.operationId, payload],
      );

      const snapshot = snapshotResult.rows[0];

      if (!snapshot) {
        throw new DataInvariantError('operation disappeared after idempotent insert');
      }

      if (!snapshot.payload_matches) {
        throw new IdempotencyConflictError(command.operationId);
      }

      if (this.isTerminal(snapshot.state)) {
        return this.toResult(snapshot);
      }

      const resourceResult = await client.query<CapacityRow>(
        `
          SELECT capacity
          FROM issue3_lab.capacity_resources
          WHERE resource_id = $1
          FOR UPDATE
        `,
        [snapshot.resource_id],
      );

      const resource = resourceResult.rows[0];

      if (!resource) {
        throw new DataInvariantError(
          `resource ${snapshot.resource_id} referenced by operation does not exist`,
        );
      }

      await this.expireResourceHolds(client, snapshot.resource_id, now);

      const current = await this.lockOperation(client, command.operationId);

      if (this.isTerminal(current.state) || current.state === 'PROCESSING') {
        return this.toResult(current);
      }

      if (current.hold_expires_at.getTime() <= now.getTime()) {
        const expired = await this.updateOperation(
          client,
          current.operation_id,
          'EXPIRED',
          'HOLD_EXPIRED',
          now,
        );
        return this.toResult(expired);
      }

      const usageResult = await client.query<CapacityUsageRow>(
        `
          SELECT COUNT(*)::int AS used
          FROM issue3_lab.operations
          WHERE resource_id = $1
            AND state IN ('PROCESSING', 'CONFIRMED')
        `,
        [current.resource_id],
      );

      const used = usageResult.rows[0]?.used ?? 0;

      if (used >= resource.capacity) {
        const rejected = await this.updateOperation(
          client,
          current.operation_id,
          'REJECTED',
          'CAPACITY_UNAVAILABLE',
          now,
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
          now,
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
        [current.operation_id, current.wallet_id, current.amount, now],
      );

      const processing = await this.updateOperation(
        client,
        current.operation_id,
        'PROCESSING',
        null,
        now,
      );

      return this.toResult(processing);
    });
  }

  async confirm(operationId: string): Promise<OperationResult> {
    if (!operationId.trim()) {
      throw new TypeError('operationId is required');
    }

    const now = this.clock.now();

    return this.database.transaction(async (client) => {
      const snapshotResult = await client.query<OperationRow>(
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

      const snapshot = snapshotResult.rows[0];

      if (!snapshot) {
        throw new OperationNotFoundError(operationId);
      }

      if (this.isTerminal(snapshot.state)) {
        return this.toResult(snapshot);
      }

      await client.query(
        `
          SELECT resource_id
          FROM issue3_lab.capacity_resources
          WHERE resource_id = $1
          FOR UPDATE
        `,
        [snapshot.resource_id],
      );

      await this.expireResourceHolds(client, snapshot.resource_id, now);

      const current = await this.lockOperation(client, operationId);

      if (this.isTerminal(current.state)) {
        return this.toResult(current);
      }

      if (current.state !== 'PROCESSING') {
        throw new DataInvariantError(
          `operation ${operationId} cannot confirm from state ${current.state}`,
        );
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
        [current.operation_id, current.wallet_id, current.amount, now],
      );

      const confirmed = await this.updateOperation(
        client,
        current.operation_id,
        'CONFIRMED',
        null,
        now,
      );

      return this.toResult(confirmed);
    });
  }

  private async expireResourceHolds(
    client: PoolClient,
    resourceId: string,
    now: Date,
  ): Promise<void> {
    const expiredResult = await client.query<OperationRow>(
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
        WHERE resource_id = $1
          AND state = 'PROCESSING'
          AND hold_expires_at <= $2
        ORDER BY operation_id
        FOR UPDATE
      `,
      [resourceId, now],
    );

    for (const expired of expiredResult.rows) {
      const releaseResult = await client.query(
        `
          UPDATE issue3_lab.synthetic_wallets
          SET
            reserved_amount = reserved_amount - $2::bigint,
            available_amount = available_amount + $2::bigint
          WHERE wallet_id = $1
            AND reserved_amount >= $2::bigint
        `,
        [expired.wallet_id, expired.amount],
      );

      if ((releaseResult.rowCount ?? 0) !== 1) {
        throw new DataInvariantError(
          `wallet ${expired.wallet_id} cannot release expired reservation`,
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
        `,
        [expired.operation_id, expired.wallet_id, expired.amount, now],
      );

      await this.updateOperation(
        client,
        expired.operation_id,
        'EXPIRED',
        'HOLD_EXPIRED',
        now,
      );
    }
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
