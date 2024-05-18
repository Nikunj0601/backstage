/*
 * Copyright 2022 The Backstage Authors
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

import {
  BlobServiceClient,
  ContainerClient,
  StorageSharedKeyCredential,
} from '@azure/storage-blob';
import { Config } from '@backstage/config';
import { LoggerService } from '@backstage/backend-plugin-api';
import {
  EntityProvider,
  EntityProviderConnection,
  locationSpecToLocationEntity,
} from '@backstage/plugin-catalog-node';
import { LocationSpec } from '@backstage/plugin-catalog-common';
import * as uuid from 'uuid';
import { PluginTaskScheduler, TaskRunner } from '@backstage/backend-tasks';
import { readAzureBlobStorageConfigs } from './config';
import { ScmIntegrations } from '@backstage/integration';
import { DefaultAzureCredential } from '@azure/identity';
import { AzureBlobStorageConfig } from './types';
// eslint-disable-next-line @backstage/no-forbidden-package-imports
import { AzureBlobStorageIntergation } from '@backstage/integration/src/azureBlobStorage';

export class AzureBlobStorageEntityProvider implements EntityProvider {
  private readonly logger: LoggerService;
  private connection?: EntityProviderConnection;
  private blobServiceClient?: BlobServiceClient;
  private readonly scheduleFn: () => Promise<void>;

  static fromConfig(
    configRoot: Config,
    options: {
      logger: LoggerService;
      schedule?: TaskRunner;
      scheduler?: PluginTaskScheduler;
    },
  ): AzureBlobStorageEntityProvider[] {
    const providerConfigs = readAzureBlobStorageConfigs(configRoot);

    const integration =
      ScmIntegrations.fromConfig(configRoot).azureBlobStorage.list()[0];
    if (!integration) {
      throw new Error('No integration found for azureBlobStorage');
    }

    if (!options.schedule && !options.scheduler) {
      throw new Error('Either schedule or scheduler must be provided.');
    }

    return providerConfigs.map(providerConfig => {
      if (!options.schedule && !providerConfig.schedule) {
        throw new Error(
          `No schedule provided neither via code nor config for azureBlobStorage-provider:${providerConfig.id}.`,
        );
      }
      const azureCredentialsManager = new DefaultAzureCredential();
      const taskRunner =
        options.schedule ??
        options.scheduler!.createScheduledTaskRunner(providerConfig.schedule!);

      return new AzureBlobStorageEntityProvider(
        providerConfig,
        integration,
        azureCredentialsManager,
        options.logger,
        taskRunner,
      );
    });
  }
  constructor(
    private readonly config: AzureBlobStorageConfig,
    private readonly integration: AzureBlobStorageIntergation,
    private readonly azureCredentialsManager: DefaultAzureCredential,
    logger: LoggerService,
    schedule: TaskRunner,
  ) {
    this.logger = logger.child({ target: this.getProviderName() });
    this.scheduleFn = this.createScheduleFn(schedule);
  }

  private createScheduleFn(taskRunner: TaskRunner): () => Promise<void> {
    return async () => {
      const taskId = `${this.getProviderName()}:refresh`;
      return taskRunner.run({
        id: taskId,
        fn: async () => {
          const logger = this.logger.child({
            class: AzureBlobStorageEntityProvider.prototype.constructor.name,
            taskId,
            taskInstanceId: uuid.v4(),
          });

          try {
            await this.refresh(logger);
          } catch (error) {
            logger.error(
              `${this.getProviderName()} refresh failed, ${error}`,
              error,
            );
          }
        },
      });
    };
  }

  getProviderName(): string {
    return `azureBlobStorage-provider:${this.config.id}`;
  }

  async connect(connection: EntityProviderConnection): Promise<void> {
    this.connection = connection;

    const sharedKeyCredential = new StorageSharedKeyCredential(
      this.integration.config.accountName as string,
      this.integration.config.accountKey as string,
    );
    this.blobServiceClient = new BlobServiceClient(
      `https://${this.integration.config.accountName}.blob.core.windows.net`,
      sharedKeyCredential,
    );
    await this.scheduleFn();
  }

  async refresh(logger: LoggerService) {
    if (!this.connection) {
      throw new Error('Not initialized');
    }

    logger.info('Discovering Azure Blob Storage blobs');

    const keys = await this.listAllBlobKeys();
    logger.info(`Discovered ${keys.length} Azure Blob Storage blobs`);

    const locations = keys.map(key => this.createLocationSpec(key));

    await this.connection.applyMutation({
      type: 'full',
      entities: locations.map(location => {
        return {
          locationKey: this.getProviderName(),
          entity: locationSpecToLocationEntity({ location }),
        };
      }),
    });

    logger.info(
      `Committed ${locations.length} Locations for Azure Blob Storage blobs`,
    );
  }

  private async listAllBlobKeys(): Promise<string[]> {
    const keys: string[] = [];
    const containerClient = this.blobServiceClient?.getContainerClient(
      this.config.containerName,
    );

    for await (const blob of (
      containerClient as ContainerClient
    ).listBlobsFlat()) {
      if (blob.name) {
        keys.push(blob.name);
      }
    }
    console.log(keys);

    return keys;
  }

  private createLocationSpec(key: string): LocationSpec {
    return {
      type: 'url',
      target: this.createObjectUrl(key),
      presence: 'required',
    };
  }

  private createObjectUrl(key: string): string {
    const endpoint = this.blobServiceClient?.url;
    return `${endpoint}/${this.config.containerName}/${key}`;
  }
}
