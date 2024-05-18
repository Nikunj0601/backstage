/*
 * Copyright 2020 The Backstage Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { CatalogBuilder } from '@backstage/plugin-catalog-backend';
import { ScaffolderEntitiesProcessor } from '@backstage/plugin-catalog-backend-module-scaffolder-entity-model';
import { UnprocessedEntitiesModule } from '@backstage/plugin-catalog-backend-module-unprocessed';
import { Router } from 'express';
import { PluginEnvironment } from '../types';
import { DemoEventBasedEntityProvider } from './DemoEventBasedEntityProvider';
import { AzureBlobStorageEntityProvider } from '@backstage/plugin-catalog-backend-module-azure';
import { Duration } from 'luxon';

export default async function createPlugin(
  env: PluginEnvironment,
): Promise<Router> {
  const builder = CatalogBuilder.create(env);
  console.log('gwgergKJRHQWI4HRIU3HRI', env.config);

  builder.addProcessor(new ScaffolderEntitiesProcessor());

  const demoProvider = new DemoEventBasedEntityProvider({
    events: env.events,
    logger: env.logger,
    topics: ['example'],
  });
  await demoProvider.subscribe();
  // builder.addEntityProvider(demoProvider);
  builder.addEntityProvider(
    ...AzureBlobStorageEntityProvider.fromConfig(env.config, {
      logger: env.logger,
      schedule: env.scheduler.createScheduledTaskRunner({
        frequency: Duration.fromObject({ minutes: 30 }),
        timeout: Duration.fromObject({ minutes: 3 }),
      }),
    }),
  );
  const { processingEngine, router } = await builder.build();

  const unprocessed = UnprocessedEntitiesModule.create({
    database: await env.database.getClient(),
    router,
    permissions: env.permissions,
    discovery: env.discovery,
  });

  unprocessed.registerRoutes();

  await processingEngine.start();
  return router;
}
