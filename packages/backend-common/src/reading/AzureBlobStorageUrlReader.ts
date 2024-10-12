/*
 * Copyright 2024 The Backstage Authors
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
  AnonymousCredential,
  BlobServiceClient,
  ContainerClient,
  StorageSharedKeyCredential,
} from '@azure/storage-blob';
import { DefaultAzureCredential, TokenCredential } from '@azure/identity';
import {
  ReaderFactory,
  ReadTreeOptions,
  ReadTreeResponse,
  ReadTreeResponseFactory,
  ReadUrlOptions,
  ReadUrlResponse,
  SearchResponse,
  UrlReader,
} from './types';
import { ForwardedError, NotModifiedError } from '@backstage/errors';
import { Readable } from 'stream';
import { relative } from 'path/posix';
import { Config } from '@backstage/config';
import { ReadUrlResponseFactory } from './ReadUrlResponseFactory';
import {
  AzureBlobStorageIntergation,
  AzureCredentialsManager,
  DefaultAzureCredentialsManager,
  ScmIntegrations,
} from '@backstage/integration';

export function parseUrl(
  url: string,
  config: AzureBlobStorageIntergation,
): { path: string; container: string } {
  const parsedUrl = new URL(url);
  const pathSegments = parsedUrl.pathname.split('/').filter(Boolean);

  if (pathSegments.length < 2) {
    throw new Error(`Invalid Azure Blob Storage URL format: ${url}`);
  }

  // First segment is the container name, rest is the blob path
  const container = pathSegments[0];
  const path = pathSegments.slice(1).join('/');

  return { path, container };
}
export class AzureBlobStorageUrlReader implements UrlReader {
  static factory: ReaderFactory = ({ config, treeResponseFactory }) => {
    const integrations = ScmIntegrations.fromConfig(config);

    const credsManager =
      DefaultAzureCredentialsManager.fromIntegrations(integrations);

    return integrations.azureBlobStorage.list().map(integrationConfig => {
      const reader = new AzureBlobStorageUrlReader(
        credsManager,
        integrationConfig,
        {
          treeResponseFactory,
        },
      );

      const predicate = (url: URL) =>
        url.host.endsWith(integrationConfig.config.host);
      return { reader, predicate };
    });
  };

  // private readonly blobServiceClient: BlobServiceClient;

  constructor(
    private readonly credsManager: AzureCredentialsManager,
    private readonly integration: AzureBlobStorageIntergation,
    private readonly deps: {
      treeResponseFactory: ReadTreeResponseFactory;
    },
  ) {}

  private async createContainerClient(
    containerName: string,
  ): Promise<ContainerClient> {
    const accountName = this.integration.config.accountName; // Use the account name from the integration config
    const accountKey = this.integration.config.accountKey; // Get the account key if it exists

    if (accountKey && accountName) {
      const creds = new StorageSharedKeyCredential(accountName, accountKey);
      const blobServiceClient = new BlobServiceClient(
        `https://${accountName}.${this.integration.config.host}`,
        creds,
      );
      return blobServiceClient.getContainerClient(containerName);
    }
    // Use the credentials manager to get the correct credentials
    const credential = await this.credsManager.getCredentials(
      accountName as string,
    );

    const blobServiceClient = new BlobServiceClient(
      `https://${accountName}.${this.integration.config.host}`,
      credential as
        | TokenCredential
        | StorageSharedKeyCredential
        | AnonymousCredential,
    );
    return blobServiceClient.getContainerClient(containerName);
  }

  async read(url: string): Promise<Buffer> {
    const response = await this.readUrl(url);
    return response.buffer();
  }

  async readUrl(
    url: string,
    options?: ReadUrlOptions,
  ): Promise<ReadUrlResponse> {
    const { etag, lastModifiedAfter } = options ?? {};

    try {
      const { path, container } = parseUrl(url, this.integration);

      const containerClient = await this.createContainerClient(container);
      const blobClient = containerClient.getBlobClient(path);

      const abortController = new AbortController();

      const getBlobOptions = {
        conditions: {
          ...(etag && { ifNoneMatch: etag }),
          ...(lastModifiedAfter && { ifModifiedSince: lastModifiedAfter }),
        },
      };
      options?.signal?.addEventListener('abort', () => abortController.abort());

      const downloadBlockBlobResponse = await blobClient.download(
        0,
        undefined,
        getBlobOptions,
      );

      const data = await this.retrieveAzureBlobData(
        downloadBlockBlobResponse.readableStreamBody as Readable,
      );

      return ReadUrlResponseFactory.fromReadable(data, {
        etag: downloadBlockBlobResponse.etag,
        lastModifiedAt: downloadBlockBlobResponse.lastModified,
      });
    } catch (e) {
      if (e.$metadata && e.$metadata.httpStatusCode === 304) {
        throw new NotModifiedError();
      }

      throw new ForwardedError(
        'Could not retrieve file from Azure Blob Storage',
        e,
      );
    }
  }

  async readTree(
    url: string,
    options?: ReadTreeOptions,
  ): Promise<ReadTreeResponse> {
    try {
      const { path, container } = parseUrl(url, this.integration);

      const containerClient = await this.createContainerClient(container);

      const blobs = containerClient.listBlobsFlat({ prefix: path });

      const responses = [];
      const abortController = new AbortController();
      for await (const blob of blobs) {
        const blobClient = containerClient.getBlobClient(blob.name);
        options?.signal?.addEventListener('abort', () =>
          abortController.abort(),
        );
        const downloadBlockBlobResponse = await blobClient.download();
        const data = await this.retrieveAzureBlobData(
          downloadBlockBlobResponse.readableStreamBody as Readable,
        );

        responses.push({
          data: Readable.from(data),
          path: relative(path, blob.name),
          lastModifiedAt: blob.properties.lastModified,
        });
      }

      return this.deps.treeResponseFactory.fromReadableArray(responses);
    } catch (e) {
      throw new ForwardedError(
        'Could not retrieve file tree from Azure Blob Storage',
        e,
      );
    }
  }

  async search(): Promise<SearchResponse> {
    throw new Error('AzureBlobStorageUrlReader does not implement search');
  }

  toString() {
    const accountName = this.integration.config.accountName;
    const accountKey = this.integration.config.accountKey;
    return `azureBlobStorage{accountName=${accountName},authed=${Boolean(
      accountKey,
    )}}`;
  }

  private parseUrl(url: string): { path: string } {
    const parsedUrl = new URL(url);
    const path = parsedUrl.pathname.substring(
      parsedUrl.pathname.lastIndexOf('/') + 1,
    );

    return { path };
  }

  private async retrieveAzureBlobData(stream: Readable): Promise<Readable> {
    return new Promise((resolve, reject) => {
      try {
        const chunks: any[] = [];
        stream.on('data', chunk => chunks.push(chunk));
        stream.on('error', (e: Error) =>
          reject(new ForwardedError('Unable to read stream', e)),
        );
        stream.on('end', () => resolve(Readable.from(Buffer.concat(chunks))));
      } catch (e) {
        throw new ForwardedError('Unable to parse the response data', e);
      }
    });
  }
}
