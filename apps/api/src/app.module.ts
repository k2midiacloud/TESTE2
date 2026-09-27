import { Module } from '@nestjs/common';

import { AtomicCapacityModule } from './atomic-capacity/atomic-capacity.module';

@Module({
  imports: [AtomicCapacityModule],
})
export class AppModule {}
