import type { CatalogPlugin } from '@data-fair/lib-common-types/catalog/index.js'
import { importConfigSchema, configSchema, assertConfigValid, type MockConfig } from '#types'
import capabilities from './lib/capabilities.ts'
import Debug from 'debug'
const debug = Debug('catalog-mock')

// Since the plugin is very frequently imported, each function is imported on demand,
// instead of loading the entire plugin.
// This file should not contain any code, but only constants and dynamic imports of functions.

const plugin: CatalogPlugin<MockConfig, typeof capabilities> = {

  async list (context) {
    const { list } = await import('./lib/imports.ts')
    return list(context)
  },

  async getResource (catalogConfig, resourceId) {
    const { getResource } = await import('./lib/imports.ts')
    return getResource(catalogConfig, resourceId)
  },

  async downloadResource (context) {
    const { downloadResource } = await import('./lib/imports.ts')
    return downloadResource(context)
  },

  async publishDataset ({ catalogConfig, dataset, publication }) {
    debug('Publishing dataset ' + dataset.id)
    publication.remoteDataset = {
      id: 'my-mock-' + dataset.id,
      title: dataset.title,
      url: 'https://example.com/dataset/' + dataset.id,
    }
    return publication
  },

  async deleteDataset ({ catalogConfig, datasetId, resourceId }) {
    debug('Deleting dataset...')
  },

  metadata: {
    title: 'Catalog Mock',
    description: 'Mock plugin for Data Fair Catalog',
    capabilities
  },

  importConfigSchema,
  configSchema,
  assertConfigValid
}
export default plugin
