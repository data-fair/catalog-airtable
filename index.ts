import type { CatalogPlugin } from '@data-fair/types-catalogs'
import { configSchema, assertConfigValid, type AirtableConfig } from '#types'
import { type AirtableCapabilities, capabilities } from './lib/capabilities.ts'

const plugin: CatalogPlugin<AirtableConfig, AirtableCapabilities> = {
  async prepare (context) {
    const prepare = (await import('./lib/prepare.ts')).default
    return prepare(context)
  },

  async listResources (context) {
    const { listResources } = await import('./lib/imports.ts')
    return listResources(context)
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
