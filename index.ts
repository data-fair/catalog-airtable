import type { CatalogPlugin } from '@data-fair/types-catalogs'
import { configSchema, assertConfigValid, type AirtableConfig } from '#types'
import { type AirtableCapabilities, capabilities } from './lib/capabilities.ts'

const plugin: CatalogPlugin<AirtableConfig, AirtableCapabilities> = {
  async prepare ({ catalogConfig, secrets }) {
    if (catalogConfig.apiKey && catalogConfig.apiKey !== '********') {
      secrets.apiKey = catalogConfig.apiKey
      catalogConfig.apiKey = '********'
    } else if (secrets?.apiKey && catalogConfig.apiKey === '') {
      delete secrets.apiKey
    }
    return {
      catalogConfig,
      secrets
    }
  },

  async list (context) {
    const { list } = await import('./lib/imports.ts')
    return list(context)
  },

  async getResource (context) {
    const { getResource } = await import('./lib/download.ts')
    return getResource(context)
  },

  metadata: {
    title: 'Catalog Airtable',
    description: 'Airtable plugin for Data Fair Catalog',
    thumbnailPath: './lib/airtable.svg',
    capabilities
  },
  configSchema,
  assertConfigValid
}
export default plugin
