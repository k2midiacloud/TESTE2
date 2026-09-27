import { readFile } from 'node:fs/promises';
import { resolve } from 'node:path';

import { Pool } from 'pg';
import type { PoolClient } from 'pg';
import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
} from 'vitest';

import { DatabaseService } from '../database/database.service';
import { AtomicCapacityService } from './atomic-capacity.service';
import type { Clock } from './clock';
import {
  IdempotencyConflictError,
} from './atomic-capacity.types';
import type {
  AcquireCommand,
  OperationResult,
} from './atomic-capacity.types';

class MutableClock implements Clock {
  constructor(private current: Date) {}

  now(): Date {
    return new Date(this.current);
  }

  set(value: Date): void {
    this.current = new Date(value);
  }

  advance(milliseconds: number): void {
    this.current = new Date(this.current.getTime() + milliseconds);
  }
}

const describePostgres = process.env.DATABASE_URL ? describe : describe.skip;

describePostgres('AtomicCapacityService PostgreSQL integration', () => {
  let adminPool: Pool;
  let database: DatabaseService;
  let service: AtomicCapacityService;
  let clock: MutableClock;

  beforeAll(async () => {
    const databaseUrl = process.env.DATABASE_URL;

    if (!databaseUrl) {
      throw new Error('DATABASE_URL is required for PostgreSQL integration tests');
    }

    adminPool = new Pool({
      connectionString: databaseUrl,
      application_name: 'issue3-test-admin',
    });

    const migrationSql = await readFile(
      resolve(process.cwd(), 'migrations/001_atomic_capacity.sql'),
      'utf8',
    );

    await adminPool.query(migrationSql);

    database = new DatabaseService();
    clock = new MutableClock(new Date('2026-09-27T21:00:00.000Z'));
    service = new AtomicCapacityService(database, clock);
  });

  beforeEach(async () => {
    clock.set(new Date('2026-09-27T21:00:00.000Z'));

    await adminPool.query(`
      TRUNCATE TABLE
        issue3_lab.synthetic_ledger,
        issue3_lab.operations,
        issue3_lab.synthetic_wallets,
        issue3_lab.capacity_resources
      RESTART IDENTITY CASCADE
    `);
  });

  afterAll(async () => {
    await database.onModuleDestroy();
    await adminPool.end();
  });

  it('reserves synthetic balance and confirms exactly once on the happy path', async () => {
    await seedResource('resource-1', 1);
    await seedWallet('wallet-1', 10);

    const command = makeCommand({
      operationId: 'op-happy',
      resourceId: 'resource-1',
      actorId: 'actor-1',
      walletId: 'wallet-1',
      amount: 10,
    });

    const acquired = await service.acquire(command);
    expect(acquired.state).toBe('PROCESSING');

    const confirmed = await service.confirm(command.operationId);
    expect(confirmed.state).toBe('CONFIRMED');

    const wallet = await walletState('wallet-1');
    expect(wallet).toEqual({
      available_amount: '0',
      reserved_amount: '0',
      consumed_amount: '10',
    });

    const events = await ledgerEvents(command.operationId);
    expect(events).toEqual(['RESERVED', 'CONSUMED']);

    await expect(
      adminPool.query(
        `
          UPDATE issue3_lab.synthetic_ledger
          SET amount = 999
          WHERE operation_id = $1
        `,
        [command.operationId],
      ),
    ).rejects.toThrow(/append-only/);
  });

  it('allows at most one winner when two operations contend for the last capacity', async () => {
    await seedResource('resource-last', 1);
    await seedWallet('wallet-a', 10);
    await seedWallet('wallet-b', 10);

    const blocker = await lockResource('resource-last');

    const firstPromise = service.acquire(
      makeCommand({
        operationId: 'op-a',
        resourceId: 'resource-last',
        actorId: 'actor-a',
        walletId: 'wallet-a',
        amount: 10,
      }),
    );

    const secondPromise = service.acquire(
      makeCommand({
        operationId: 'op-b',
        resourceId: 'resource-last',
        actorId: 'actor-b',
        walletId: 'wallet-b',
        amount: 10,
      }),
    );

    await waitForLockWaiters(2);
    await blocker.query('COMMIT');
    blocker.release();

    const results = await Promise.all([firstPromise, secondPromise]);
    const winners = results.filter((result) => result.state === 'PROCESSING');
    const losers = results.filter((result) => result.state === 'REJECTED');

    expect(winners).toHaveLength(1);
    expect(losers).toHaveLength(1);
    expect(losers[0]?.reason).toBe('CAPACITY_UNAVAILABLE');

    const winner = winners[0];

    if (!winner) {
      throw new Error('concurrency test did not produce a winner');
    }

    await service.confirm(winner.operationId);

    const persisted = await adminPool.query<{
      capacity: number;
      confirmed: number;
    }>(`
      SELECT
        r.capacity,
        COUNT(o.operation_id) FILTER (WHERE o.state = 'CONFIRMED')::int AS confirmed
      FROM issue3_lab.capacity_resources r
      LEFT JOIN issue3_lab.operations o
        ON o.resource_id = r.resource_id
      WHERE r.resource_id = 'resource-last'
      GROUP BY r.capacity
    `);

    expect(persisted.rows[0]?.confirmed).toBe(1);
    expect(persisted.rows[0]?.confirmed).toBeLessThanOrEqual(
      persisted.rows[0]?.capacity ?? 0,
    );

    const loserId = losers[0]?.operationId;

    if (!loserId) {
      throw new Error('concurrency test did not produce a loser');
    }

    expect(await ledgerEvents(loserId)).toEqual([]);
    expect(await ledgerEvents(winner.operationId)).toEqual([
      'RESERVED',
      'CONSUMED',
    ]);
  });

  it('replays the same operation_id without extending the hold or duplicating effects', async () => {
    await seedResource('resource-idempotent', 1);
    await seedWallet('wallet-idempotent', 10);

    const command = makeCommand({
      operationId: 'op-idempotent',
      resourceId: 'resource-idempotent',
      actorId: 'actor-idempotent',
      walletId: 'wallet-idempotent',
      amount: 10,
    });

    const first = await service.acquire(command);
    clock.advance(10_000);
    const retry = await service.acquire(command);

    expect(first.state).toBe('PROCESSING');
    expect(retry.state).toBe('PROCESSING');
    expect(retry.holdExpiresAt.toISOString()).toBe(
      command.holdExpiresAt.toISOString(),
    );

    const firstConfirmation = await service.confirm(command.operationId);
    const secondConfirmation = await service.confirm(command.operationId);

    expect(firstConfirmation.state).toBe('CONFIRMED');
    expect(secondConfirmation.state).toBe('CONFIRMED');

    expect(await ledgerEvents(command.operationId)).toEqual([
      'RESERVED',
      'CONSUMED',
    ]);

    const wallet = await walletState('wallet-idempotent');
    expect(wallet).toEqual({
      available_amount: '0',
      reserved_amount: '0',
      consumed_amount: '10',
    });

    const count = await adminPool.query<{ count: number }>(
      `
        SELECT COUNT(*)::int AS count
        FROM issue3_lab.operations
        WHERE operation_id = $1
      `,
      [command.operationId],
    );

    expect(count.rows[0]?.count).toBe(1);
  });

  it('rejects reuse of an operation_id with a different payload without extra effects', async () => {
    await seedResource('resource-conflict', 1);
    await seedWallet('wallet-conflict', 20);

    const command = makeCommand({
      operationId: 'op-conflict',
      resourceId: 'resource-conflict',
      actorId: 'actor-conflict',
      walletId: 'wallet-conflict',
      amount: 10,
    });

    await service.acquire(command);

    await expect(
      service.acquire({
        ...command,
        amount: 11,
      }),
    ).rejects.toBeInstanceOf(IdempotencyConflictError);

    expect(await ledgerEvents(command.operationId)).toEqual(['RESERVED']);

    const wallet = await walletState('wallet-conflict');
    expect(wallet).toEqual({
      available_amount: '10',
      reserved_amount: '10',
      consumed_amount: '0',
    });
  });

  it('rejects acquisition when synthetic balance is insufficient', async () => {
    await seedResource('resource-low-balance', 1);
    await seedWallet('wallet-low-balance', 5);

    const command = makeCommand({
      operationId: 'op-low-balance',
      resourceId: 'resource-low-balance',
      actorId: 'actor-low-balance',
      walletId: 'wallet-low-balance',
      amount: 10,
    });

    const result = await service.acquire(command);

    expect(result.state).toBe('REJECTED');
    expect(result.reason).toBe('INSUFFICIENT_FUNDS');
    expect(await ledgerEvents(command.operationId)).toEqual([]);

    const wallet = await walletState('wallet-low-balance');
    expect(wallet).toEqual({
      available_amount: '5',
      reserved_amount: '0',
      consumed_amount: '0',
    });
  });

  it('does not let two incompatible obligations reserve or consume the same synthetic balance', async () => {
    await seedResource('resource-one', 1);
    await seedResource('resource-two', 1);
    await seedWallet('wallet-shared', 10);

    const blocker = await lockWallet('wallet-shared');

    const firstPromise = service.acquire(
      makeCommand({
        operationId: 'op-wallet-one',
        resourceId: 'resource-one',
        actorId: 'actor-one',
        walletId: 'wallet-shared',
        amount: 10,
      }),
    );

    const secondPromise = service.acquire(
      makeCommand({
        operationId: 'op-wallet-two',
        resourceId: 'resource-two',
        actorId: 'actor-two',
        walletId: 'wallet-shared',
        amount: 10,
      }),
    );

    await waitForLockWaiters(2);
    await blocker.query('COMMIT');
    blocker.release();

    const results = await Promise.all([firstPromise, secondPromise]);
    const winner = results.find((result) => result.state === 'PROCESSING');
    const loser = results.find((result) => result.state === 'REJECTED');

    expect(winner).toBeDefined();
    expect(loser?.reason).toBe('INSUFFICIENT_FUNDS');

    if (!winner) {
      throw new Error('shared balance test did not produce a winner');
    }

    await service.confirm(winner.operationId);

    const wallet = await walletState('wallet-shared');
    expect(wallet).toEqual({
      available_amount: '0',
      reserved_amount: '0',
      consumed_amount: '10',
    });

    const ledgerCount = await adminPool.query<{ count: number }>(`
      SELECT COUNT(*)::int AS count
      FROM issue3_lab.synthetic_ledger
      WHERE wallet_id = 'wallet-shared'
        AND event_type = 'RESERVED'
    `);

    expect(ledgerCount.rows[0]?.count).toBe(1);
  });

  it('expires a hold, refuses late confirmation, preserves the original deadline, and releases capacity', async () => {
    await seedResource('resource-expiry', 1);
    await seedWallet('wallet-expiry', 10);

    const originalCommand = makeCommand({
      operationId: 'op-expired',
      resourceId: 'resource-expiry',
      actorId: 'actor-expired',
      walletId: 'wallet-expiry',
      amount: 10,
    });

    const acquired = await service.acquire(originalCommand);
    expect(acquired.state).toBe('PROCESSING');

    clock.advance(61_000);

    const lateConfirmation = await service.confirm(originalCommand.operationId);
    expect(lateConfirmation.state).toBe('EXPIRED');
    expect(lateConfirmation.reason).toBe('HOLD_EXPIRED');

    const retry = await service.acquire(originalCommand);
    expect(retry.state).toBe('EXPIRED');
    expect(retry.holdExpiresAt.toISOString()).toBe(
      originalCommand.holdExpiresAt.toISOString(),
    );

    await expect(
      service.acquire({
        ...originalCommand,
        holdExpiresAt: new Date(clock.now().getTime() + 60_000),
      }),
    ).rejects.toBeInstanceOf(IdempotencyConflictError);

    expect(await ledgerEvents(originalCommand.operationId)).toEqual([
      'RESERVED',
      'RELEASED',
    ]);

    const replacement = makeCommand({
      operationId: 'op-replacement',
      resourceId: 'resource-expiry',
      actorId: 'actor-replacement',
      walletId: 'wallet-expiry',
      amount: 10,
    });

    const replacementAcquired = await service.acquire(replacement);
    expect(replacementAcquired.state).toBe('PROCESSING');

    const replacementConfirmed = await service.confirm(replacement.operationId);
    expect(replacementConfirmed.state).toBe('CONFIRMED');

    const states = await adminPool.query<{
      operation_id: string;
      state: string;
    }>(`
      SELECT operation_id, state
      FROM issue3_lab.operations
      WHERE resource_id = 'resource-expiry'
      ORDER BY operation_id
    `);

    expect(states.rows).toEqual([
      {
        operation_id: 'op-expired',
        state: 'EXPIRED',
      },
      {
        operation_id: 'op-replacement',
        state: 'CONFIRMED',
      },
    ]);

    const confirmed = await adminPool.query<{ count: number }>(`
      SELECT COUNT(*)::int AS count
      FROM issue3_lab.operations
      WHERE resource_id = 'resource-expiry'
        AND state = 'CONFIRMED'
    `);

    expect(confirmed.rows[0]?.count).toBe(1);
  });

  async function seedResource(resourceId: string, capacity: number): Promise<void> {
    await adminPool.query(
      `
        INSERT INTO issue3_lab.capacity_resources (resource_id, capacity)
        VALUES ($1, $2)
      `,
      [resourceId, capacity],
    );
  }

  async function seedWallet(walletId: string, availableAmount: number): Promise<void> {
    await adminPool.query(
      `
        INSERT INTO issue3_lab.synthetic_wallets (
          wallet_id,
          available_amount,
          reserved_amount,
          consumed_amount
        )
        VALUES ($1, $2, 0, 0)
      `,
      [walletId, availableAmount],
    );
  }

  function makeCommand(
    overrides: Omit<AcquireCommand, 'holdExpiresAt'> &
      Partial<Pick<AcquireCommand, 'holdExpiresAt'>>,
  ): AcquireCommand {
    return {
      ...overrides,
      holdExpiresAt:
        overrides.holdExpiresAt ??
        new Date(clock.now().getTime() + 60_000),
    };
  }

  async function walletState(walletId: string): Promise<{
    available_amount: string;
    reserved_amount: string;
    consumed_amount: string;
  }> {
    const result = await adminPool.query<{
      available_amount: string;
      reserved_amount: string;
      consumed_amount: string;
    }>(
      `
        SELECT available_amount, reserved_amount, consumed_amount
        FROM issue3_lab.synthetic_wallets
        WHERE wallet_id = $1
      `,
      [walletId],
    );

    const wallet = result.rows[0];

    if (!wallet) {
      throw new Error(`wallet ${walletId} not found`);
    }

    return wallet;
  }

  async function ledgerEvents(operationId: string): Promise<string[]> {
    const result = await adminPool.query<{ event_type: string }>(
      `
        SELECT event_type
        FROM issue3_lab.synthetic_ledger
        WHERE operation_id = $1
        ORDER BY ledger_entry_id
      `,
      [operationId],
    );

    return result.rows.map((row) => row.event_type);
  }

  async function lockResource(resourceId: string): Promise<PoolClient> {
    const client = await adminPool.connect();
    await client.query('BEGIN');
    await client.query(
      `
        SELECT resource_id
        FROM issue3_lab.capacity_resources
        WHERE resource_id = $1
        FOR UPDATE
      `,
      [resourceId],
    );
    return client;
  }

  async function lockWallet(walletId: string): Promise<PoolClient> {
    const client = await adminPool.connect();
    await client.query('BEGIN');
    await client.query(
      `
        SELECT wallet_id
        FROM issue3_lab.synthetic_wallets
        WHERE wallet_id = $1
        FOR UPDATE
      `,
      [walletId],
    );
    return client;
  }

  async function waitForLockWaiters(expected: number): Promise<void> {
    const deadline = Date.now() + 3_000;

    while (Date.now() < deadline) {
      const result = await adminPool.query<{ count: number }>(`
        SELECT COUNT(*)::int AS count
        FROM pg_stat_activity
        WHERE datname = current_database()
          AND application_name = 'issue3-atomic-capacity'
          AND wait_event_type = 'Lock'
      `);

      if ((result.rows[0]?.count ?? 0) >= expected) {
        return;
      }

      await new Promise((resolvePromise) => setTimeout(resolvePromise, 20));
    }

    throw new Error(
      `expected at least ${expected} concurrent PostgreSQL lock waiters`,
    );
  }
});
