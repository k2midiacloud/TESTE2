import { Injectable, OnModuleDestroy } from '@nestjs/common';
import { Pool } from 'pg';
import type { PoolClient } from 'pg';

const DEFAULT_DEADLOCK_RETRIES = 2;

@Injectable()
export class DatabaseService implements OnModuleDestroy {
  private pool: Pool | null = null;

  getPool(): Pool {
    if (this.pool) {
      return this.pool;
    }

    const databaseUrl = process.env.DATABASE_URL;

    if (!databaseUrl) {
      throw new Error('DATABASE_URL is required for database operations');
    }

    this.pool = new Pool({
      connectionString: databaseUrl,
      application_name: 'issue3-atomic-capacity',
    });

    return this.pool;
  }

  async transaction<T>(work: (client: PoolClient) => Promise<T>): Promise<T> {
    const client = await this.getPool().connect();

    try {
      await client.query('BEGIN');
      const result = await work(client);
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async transactionWithDeadlockRetry<T>(
    work: (client: PoolClient) => Promise<T>,
    maxDeadlockRetries = DEFAULT_DEADLOCK_RETRIES,
  ): Promise<T> {
    let retries = 0;

    while (true) {
      try {
        return await this.transaction(work);
      } catch (error) {
        if (!this.isDeadlock(error) || retries >= maxDeadlockRetries) {
          throw error;
        }

        retries += 1;
      }
    }
  }

  async onModuleDestroy(): Promise<void> {
    if (!this.pool) {
      return;
    }

    const pool = this.pool;
    this.pool = null;
    await pool.end();
  }

  private isDeadlock(error: unknown): boolean {
    return (
      typeof error === 'object' &&
      error !== null &&
      'code' in error &&
      (error as { code?: unknown }).code === '40P01'
    );
  }
}
