import type { PrepareContext } from '@data-fair/types-catalogs'
import type { AirtableCapabilities } from './capabilities.ts'
import type { AirtableConfig } from '#types'
import axios from '@data-fair/lib-node/axios.js'

export default async ({ catalogConfig, secrets }: PrepareContext<AirtableConfig, AirtableCapabilities>) => {
  if (catalogConfig.apiKey && catalogConfig.apiKey !== '********') {
    secrets.apiKey = catalogConfig.apiKey
    catalogConfig.apiKey = '********'
  } else if (secrets?.apiKey && catalogConfig.apiKey === '') {
    delete secrets.apiKey
  }
  if (!secrets?.apiKey) {
    throw new Error('Airtable API key is required')
  }
  try {
    // Test the API key by fetching the bases
    await axios.get('https://api.airtable.com/v0/meta/bases', {
      headers: {
        Authorization: `Bearer ${secrets.apiKey}`,
        'Content-Type': 'application/json'
      }
    })
  } catch (error) {
    console.error('Error testing Airtable API key:', error)
    throw new Error('Invalid Airtable API key')
  }

  return {
    catalogConfig,
    secrets
  }
}
