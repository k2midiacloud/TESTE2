import { Module } from '@nestjs/common';

import { DatabaseService } from '../database/database.service';
import { AtomicCapacityService } from './atomic-capacity.service';
import { CLOCK, SystemClock } from './clock';

@Module({
  providers: [
    DatabaseService,
    SystemClock,
    {
      provide: CLOCK,
      useExisting: SystemClock,
    },
    AtomicCapacityService,
  ],
  exports: [AtomicCapacityService],
})
export class AtomicCapacityModule {}
