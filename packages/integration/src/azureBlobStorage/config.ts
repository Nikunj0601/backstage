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

import { Config } from '@backstage/config';

const AZURE_HOST = 'dev.azure.com';

/**
 * The configuration parameters for a single AWS S3 provider.
 *
 * @public
 */
export type AzureBlobStorageIntegrationConfig = {
  /**
   * Host, derived from endpoint, and defaults to dev.azure.com
   */
  host: string;

  endpoint?: string;

  accountName?: string;

  accountKey?: string;
};

/**
 * Reads a single Aws S3 integration config.
 *
 * @param config - The config object of a single integration
 * @public
 */

export function readAzureBlobStorageIntegrationConfig(
  config: Config,
): AzureBlobStorageIntegrationConfig {
  const endpoint = config.getOptionalString('endpoint');

  let host;
  let pathname;
  if (endpoint) {
    try {
      const url = new URL(endpoint);
      host = url.host;
      pathname = url.pathname;
    } catch {
      throw new Error(
        `invalid azureBlob integration config, endpoint '${endpoint}' is not a valid URL`,
      );
    }
    if (pathname !== '/') {
      throw new Error(
        `invalid azureBlob integration config, endpoints cannot contain path, got '${endpoint}'`,
      );
    }
  } else {
    host = AZURE_HOST;
  }

  const accountName = config.getOptionalString('accountName');
  const accountKey = config.getOptionalString('accountKey');
  return {
    host,
    endpoint,
    accountName,
    accountKey,
  };
}

/**
 * Reads a set of AWS S3 integration configs, and inserts some defaults for
 * public Amazon AWS if not specified.
 *
 * @param configs - The config objects of the integrations
 * @public
 */
export function readAzureBlobStorageIntegrationConfigs(
  configs: Config[],
): AzureBlobStorageIntegrationConfig[] {
  // First read all the explicit integrations
  const result = configs.map(readAzureBlobStorageIntegrationConfig);

  // If no explicit dev.azure.com integration was added, put one in the list as
  // a convenience
  if (!result.some(c => c.host === AZURE_HOST)) {
    result.push({
      host: AZURE_HOST,
    });
  }
  return result;
}
