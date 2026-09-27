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
import type { AcquireCommand } from './atomic-capacity.types';
import type { Clock } from './clock';

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

describePostgres('AtomicCapacityService blocker regressions', () => {
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
      application_name: 'issue3-regression-admin',
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
      DROP TRIGGER IF EXISTS fail_test_ledger_insert
        ON issue3_lab.synthetic_ledger;
      DROP FUNCTION IF EXISTS issue3_lab.fail_test_ledger_insert();

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

  it('R1-A refuses confirmation when a lock wait crosses the persisted deadline', async () => {
    await seedResource('resource-confirm-deadline', 1);
    await seedWallet('wallet-confirm-deadline', 10);

    const command = makeCommand({
      operationId: 'op-confirm-deadline',
      resourceId: 'resource-confirm-deadline',
      actorId: 'actor-confirm-deadline',
      walletId: 'wallet-confirm-deadline',
      amount: 10,
      holdExpiresAt: new Date(clock.now().getTime() + 1_000),
    });

    expect((await service.acquire(command)).state).toBe('PROCESSING');

    const blocker = await lockWallet('wallet-confirm-deadline');
    const confirmationPromise = service.confirm(command.operationId);

    await waitForLockWaiters(1);
    clock.advance(1_000);

    await blocker.query('COMMIT');
    blocker.release();

    const result = await confirmationPromise;

    expect(result.state).toBe('EXPIRED');
    expect(result.reason).toBe('HOLD_EXPIRED');
    expect(await walletState('wallet-confirm-deadline')).toEqual({
      available_amount: '10',
      reserved_amount: '0',
      consumed_amount: '0',
    });
    expect(await ledgerEvents(command.operationId)).toEqual([
      'RESERVED',
      'RELEASED',
    ]);
    expect(await ledgerEventCount(command.operationId, 'CONSUMED')).toBe(0);
  });

  it('R1-B refuses acquisition when a lock wait crosses the persisted deadline', async () => {
    await seedResource('resource-acquire-deadline', 1);
    await seedWallet('wallet-acquire-deadline', 10);

    const command = makeCommand({
      operationId: 'op-acquire-deadline',
      resourceId: 'resource-acquire-deadline',
      actorId: 'actor-acquire-deadline',
      walletId: 'wallet-acquire-deadline',
      amount: 10,
      holdExpiresAt: new Date(clock.now().getTime() + 1_000),
    });

    const blocker = await lockWallet('wallet-acquire-deadline');
    const acquirePromise = service.acquire(command);

    await waitForLockWaiters(1);
    clock.advance(1_000);

    await blocker.query('COMMIT');
    blocker.release();

    const result = await acquirePromise;

    expect(result.state).toBe('EXPIRED');
    expect(result.reason).toBe('HOLD_EXPIRED');
    expect(await walletState('wallet-acquire-deadline')).toEqual({
      available_amount: '10',
      reserved_amount: '0',
      consumed_amount: '0',
    });
    expect(await ledgerEvents(command.operationId)).toEqual([]);
  });

  it('treats exact deadline equality as expired', async () => {
    await seedResource('resource-deadline-equality', 1);
    await seedWallet('wallet-deadline-equality', 10);

    const command = makeCommand({
      operationId: 'op-deadline-equality',
      resourceId: 'resource-deadline-equality',
      actorId: 'actor-deadline-equality',
      walletId: 'wallet-deadline-equality',
      amount: 10,
      holdExpiresAt: clock.now(),
    });

    const result = await service.acquire(command);

    expect(result.state).toBe('EXPIRED');
    expect(result.reason).toBe('HOLD_EXPIRED');
    expect(await ledgerEvents(command.operationId)).toEqual([]);
  });

  it('R2 reconciles an expired hold from another resource before deciding wallet balance', async () => {
    await seedResource('resource-old', 1);
    await seedResource('resource-new', 1);
    await seedWallet('wallet-cross-resource', 10);

    const oldCommand = makeCommand({
      operationId: 'op-old-resource',
      resourceId: 'resource-old',
      actorId: 'actor-old-resource',
      walletId: 'wallet-cross-resource',
      amount: 10,
    });

    expect((await service.acquire(oldCommand)).state).toBe('PROCESSING');

    clock.advance(61_000);

    const newCommand = makeCommand({
      operationId: 'op-new-resource',
      resourceId: 'resource-new',
      actorId: 'actor-new-resource',
      walletId: 'wallet-cross-resource',
      amount: 10,
    });

    const acquired = await service.acquire(newCommand);

    expect(acquired.state).toBe('PROCESSING');
    expect(await operationState(oldCommand.operationId)).toBe('EXPIRED');
    expect(await ledgerEvents(oldCommand.operationId)).toEqual([
      'RESERVED',
      'RELEASED',
    ]);
    expect(await ledgerEventCount(oldCommand.operationId, 'RELEASED')).toBe(1);
    expect(await ledgerEvents(newCommand.operationId)).toEqual(['RESERVED']);

    const confirmed = await service.confirm(newCommand.operationId);
    expect(confirmed.state).toBe('CONFIRMED');
    expect(await walletState('wallet-cross-resource')).toEqual({
      available_amount: '0',
      reserved_amount: '0',
      consumed_amount: '10',
    });
  });

  it('R3 overlaps the crossed A/X and B/Y pattern without duplicate effects or deadlock leakage', async () => {
    await seedResource('resource-a', 1);
    await seedResource('resource-b', 1);
    await seedWallet('wallet-x', 10);
    await seedWallet('wallet-y', 10);

    const oldAY = makeCommand({
      operationId: 'op-old-a-y',
      resourceId: 'resource-a',
      actorId: 'actor-old-a-y',
      walletId: 'wallet-y',
      amount: 10,
    });
    const oldBX = makeCommand({
      operationId: 'op-old-b-x',
      resourceId: 'resource-b',
      actorId: 'actor-old-b-x',
      walletId: 'wallet-x',
      amount: 10,
    });

    expect((await service.acquire(oldAY)).state).toBe('PROCESSING');
    expect((await service.acquire(oldBX)).state).toBe('PROCESSING');

    clock.advance(61_000);

    const newAX = makeCommand({
      operationId: 'op-new-a-x',
      resourceId: 'resource-a',
      actorId: 'actor-new-a-x',
      walletId: 'wallet-x',
      amount: 10,
    });
    const newBY = makeCommand({
      operationId: 'op-new-b-y',
      resourceId: 'resource-b',
      actorId: 'actor-new-b-y',
      walletId: 'wallet-y',
      amount: 10,
    });

    const blocker = await lockWallets(['wallet-x', 'wallet-y']);

    const axPromise = service.acquire(newAX);
    const byPromise = service.acquire(newBY);

    await waitForLockWaiters(2);

    await blocker.query('COMMIT');
    blocker.release();

    const [ax, by] = await Promise.all([axPromise, byPromise]);

    expect(ax.state).toBe('PROCESSING');
    expect(by.state).toBe('PROCESSING');
    expect(await operationState(oldAY.operationId)).toBe('EXPIRED');
    expect(await operationState(oldBX.operationId)).toBe('EXPIRED');
    expect(await ledgerEventCount(oldAY.operationId, 'RELEASED')).toBe(1);
    expect(await ledgerEventCount(oldBX.operationId, 'RELEASED')).toBe(1);
    expect(await ledgerEventCount(newAX.operationId, 'RESERVED')).toBe(1);
    expect(await ledgerEventCount(newBY.operationId, 'RESERVED')).toBe(1);

    const [confirmedAX, confirmedBY] = await Promise.all([
      service.confirm(newAX.operationId),
      service.confirm(newBY.operationId),
    ]);

    expect(confirmedAX.state).toBe('CONFIRMED');
    expect(confirmedBY.state).toBe('CONFIRMED');
    expect(await confirmedCount('resource-a')).toBe(1);
    expect(await confirmedCount('resource-b')).toBe(1);
    expect(await walletState('wallet-x')).toEqual({
      available_amount: '0',
      reserved_amount: '0',
      consumed_amount: '10',
    });
    expect(await walletState('wallet-y')).toEqual({
      available_amount: '0',
      reserved_amount: '0',
      consumed_amount: '10',
    });
  });

  it('recovers a real PostgreSQL 40P01 with a fresh bounded transaction attempt', async () => {
    await seedResource('deadlock-left', 1);
    await seedResource('deadlock-right', 1);

    let readyCount = 0;
    let releaseBarrier: (() => void) | undefined;
    const bothFirstLocksHeld = new Promise<void>((resolvePromise) => {
      releaseBarrier = resolvePromise;
    });

    const markReady = (): void => {
      readyCount += 1;

      if (readyCount === 2) {
        releaseBarrier?.();
      }
    };

    const attempts = { left: 0, right: 0 };
    const transactionIds = { left: [] as string[], right: [] as string[] };

    const run = async (
      side: 'left' | 'right',
      firstResource: string,
      secondResource: string,
    ): Promise<void> => {
      await database.transactionWithDeadlockRetry(async (client) => {
        attempts[side] += 1;

        await client.query("SET LOCAL deadlock_timeout = '50ms'");

        const transactionId = await client.query<{ txid: string }>(
          'SELECT txid_current()::text AS txid',
        );
        const txid = transactionId.rows[0]?.txid;

        if (!txid) {
          throw new Error('missing PostgreSQL transaction id');
        }

        transactionIds[side].push(txid);

        await client.query(
          `
            SELECT resource_id
            FROM issue3_lab.capacity_resources
            WHERE resource_id = $1
            FOR UPDATE
          `,
          [firstResource],
        );

        if (attempts[side] === 1) {
          markReady();
          await bothFirstLocksHeld;
        }

        await client.query(
          `
            SELECT resource_id
            FROM issue3_lab.capacity_resources
            WHERE resource_id = $1
            FOR UPDATE
          `,
          [secondResource],
        );
      });
    };

    await Promise.all([
      run('left', 'deadlock-left', 'deadlock-right'),
      run('right', 'deadlock-right', 'deadlock-left'),
    ]);

    expect(attempts.left + attempts.right).toBe(3);
    expect(attempts.left).toBeLessThanOrEqual(2);
    expect(attempts.right).toBeLessThanOrEqual(2);

    const retriedSide = attempts.left === 2 ? 'left' : 'right';
    expect(transactionIds[retriedSide]).toHaveLength(2);
    expect(new Set(transactionIds[retriedSide]).size).toBe(2);
  });

  it('limits repeated 40P01 retries instead of retrying forever', async () => {
    let attempts = 0;
    const deadlock = Object.assign(new Error('forced deadlock'), {
      code: '40P01',
    });

    await expect(
      database.transactionWithDeadlockRetry(async () => {
        attempts += 1;
        throw deadlock;
      }, 2),
    ).rejects.toBe(deadlock);

    expect(attempts).toBe(3);
  });

  it('does not retry a non-transient transaction error', async () => {
    let attempts = 0;
    const nonTransient = Object.assign(new Error('not transient'), {
      code: '23505',
    });

    await expect(
      database.transactionWithDeadlockRetry(async () => {
        attempts += 1;
        throw nonTransient;
      }),
    ).rejects.toBe(nonTransient);

    expect(attempts).toBe(1);
  });

  it('converges two concurrent acquire calls with the same operation_id to one reservation', async () => {
    await seedResource('resource-same-operation', 1);
    await seedWallet('wallet-same-operation', 10);

    const command = makeCommand({
      operationId: 'op-same-operation',
      resourceId: 'resource-same-operation',
      actorId: 'actor-same-operation',
      walletId: 'wallet-same-operation',
      amount: 10,
    });

    const blocker = await lockWallet('wallet-same-operation');

    const firstPromise = service.acquire(command);
    const secondPromise = service.acquire(command);

    await waitForLockWaiters(2);

    await blocker.query('COMMIT');
    blocker.release();

    const [first, second] = await Promise.all([firstPromise, secondPromise]);

    expect(first.state).toBe('PROCESSING');
    expect(second.state).toBe('PROCESSING');
    expect(await ledgerEventCount(command.operationId, 'RESERVED')).toBe(1);
    expect(await operationCount(command.operationId)).toBe(1);
    expect(await walletState('wallet-same-operation')).toEqual({
      available_amount: '0',
      reserved_amount: '10',
      consumed_amount: '0',
    });
  });

  it('replays acquire after CONFIRMED without extending the deadline or duplicating ledger', async () => {
    await seedResource('resource-confirmed-replay', 1);
    await seedWallet('wallet-confirmed-replay', 10);

    const command = makeCommand({
      operationId: 'op-confirmed-replay',
      resourceId: 'resource-confirmed-replay',
      actorId: 'actor-confirmed-replay',
      walletId: 'wallet-confirmed-replay',
      amount: 10,
    });

    expect((await service.acquire(command)).state).toBe('PROCESSING');
    expect((await service.confirm(command.operationId)).state).toBe('CONFIRMED');

    clock.advance(30_000);
    const replay = await service.acquire(command);

    expect(replay.state).toBe('CONFIRMED');
    expect(replay.holdExpiresAt.toISOString()).toBe(
      command.holdExpiresAt.toISOString(),
    );
    expect(await ledgerEvents(command.operationId)).toEqual([
      'RESERVED',
      'CONSUMED',
    ]);
  });

  it('rolls back wallet mutation when a later ledger insert fails in the same transaction', async () => {
    await seedResource('resource-rollback', 1);
    await seedWallet('wallet-rollback', 10);

    await adminPool.query(`
      CREATE OR REPLACE FUNCTION issue3_lab.fail_test_ledger_insert()
      RETURNS trigger
      LANGUAGE plpgsql
      AS $$
      BEGIN
        IF NEW.operation_id = 'op-rollback'
          AND NEW.event_type = 'RESERVED' THEN
          RAISE EXCEPTION 'forced ledger insert failure';
        END IF;

        RETURN NEW;
      END;
      $$;

      CREATE TRIGGER fail_test_ledger_insert
        BEFORE INSERT ON issue3_lab.synthetic_ledger
        FOR EACH ROW
        EXECUTE FUNCTION issue3_lab.fail_test_ledger_insert();
    `);

    const command = makeCommand({
      operationId: 'op-rollback',
      resourceId: 'resource-rollback',
      actorId: 'actor-rollback',
      walletId: 'wallet-rollback',
      amount: 10,
    });

    try {
      await expect(service.acquire(command)).rejects.toThrow(
        /forced ledger insert failure/,
      );
    } finally {
      await adminPool.query(`
        DROP TRIGGER IF EXISTS fail_test_ledger_insert
          ON issue3_lab.synthetic_ledger;
        DROP FUNCTION IF EXISTS issue3_lab.fail_test_ledger_insert();
      `);
    }

    expect(await walletState('wallet-rollback')).toEqual({
      available_amount: '10',
      reserved_amount: '0',
      consumed_amount: '0',
    });
    expect(await operationCount(command.operationId)).toBe(0);
    expect(await ledgerEvents(command.operationId)).toEqual([]);
  });

  it('rejects DELETE from the append-only ledger and preserves the entry', async () => {
    await seedResource('resource-ledger-delete', 1);
    await seedWallet('wallet-ledger-delete', 10);

    const command = makeCommand({
      operationId: 'op-ledger-delete',
      resourceId: 'resource-ledger-delete',
      actorId: 'actor-ledger-delete',
      walletId: 'wallet-ledger-delete',
      amount: 10,
    });

    expect((await service.acquire(command)).state).toBe('PROCESSING');

    await expect(
      adminPool.query(
        `
          DELETE FROM issue3_lab.synthetic_ledger
          WHERE operation_id = $1
        `,
        [command.operationId],
      ),
    ).rejects.toThrow(/append-only/);

    expect(await ledgerEvents(command.operationId)).toEqual(['RESERVED']);
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

  async function operationState(operationId: string): Promise<string | undefined> {
    const result = await adminPool.query<{ state: string }>(
      `
        SELECT state
        FROM issue3_lab.operations
        WHERE operation_id = $1
      `,
      [operationId],
    );

    return result.rows[0]?.state;
  }

  async function operationCount(operationId: string): Promise<number> {
    const result = await adminPool.query<{ count: number }>(
      `
        SELECT COUNT(*)::int AS count
        FROM issue3_lab.operations
        WHERE operation_id = $1
      `,
      [operationId],
    );

    return result.rows[0]?.count ?? 0;
  }

  async function confirmedCount(resourceId: string): Promise<number> {
    const result = await adminPool.query<{ count: number }>(
      `
        SELECT COUNT(*)::int AS count
        FROM issue3_lab.operations
        WHERE resource_id = $1
          AND state = 'CONFIRMED'
      `,
      [resourceId],
    );

    return result.rows[0]?.count ?? 0;
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

  async function ledgerEventCount(
    operationId: string,
    eventType: string,
  ): Promise<number> {
    const result = await adminPool.query<{ count: number }>(
      `
        SELECT COUNT(*)::int AS count
        FROM issue3_lab.synthetic_ledger
        WHERE operation_id = $1
          AND event_type = $2
      `,
      [operationId, eventType],
    );

    return result.rows[0]?.count ?? 0;
  }

  async function lockWallet(walletId: string): Promise<PoolClient> {
    return lockWallets([walletId]);
  }

  async function lockWallets(walletIds: string[]): Promise<PoolClient> {
    const client = await adminPool.connect();
    await client.query('BEGIN');

    for (const walletId of [...walletIds].sort()) {
      await client.query(
        `
          SELECT wallet_id
          FROM issue3_lab.synthetic_wallets
          WHERE wallet_id = $1
          FOR UPDATE
        `,
        [walletId],
      );
    }

    return client;
  }

  async function waitForLockWaiters(expected: number): Promise<void> {
    const deadline = Date.now() + 5_000;

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
