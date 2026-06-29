import type { Folder, CatalogPlugin, ListContext } from '@data-fair/types-catalogs'
import type { AirtableConfig } from '#types'
import type { AirtableCapabilities } from './capabilities.ts'
import memoizee from 'memoizee'

let lastBases: Folder[]
type ResourceList = Awaited<ReturnType<CatalogPlugin['list']>>['results']

const fetchWithMemo = memoizee(
  async (url: string, apiKey: string): Promise<any> => {
    const response = await fetch(url, {
      headers: {
        Authorization: `Bearer ${apiKey}`,
        'Content-Type': 'application/json'
      }
    })
    if (!response.ok) {
      console.error(`Failed to fetch from Airtable API: ${response.status} ${response.statusText}`)
      throw new Error('Error retrieving data from Airtable')
    }
    return await (response.json())
  },
  { maxAge: 1000 * 30, promise: true } // Cache for 30 seconds, evict on rejection
)

/**
 * Lists the available Bases from an API key / Personal Access Token
 * @returns
 */
const listBases = async (apiKey: string): Promise<Folder[]> => {
  /* The JS library does not allow listing bases, so this is done via a fetch */
  const data = await fetchWithMemo('https://api.airtable.com/v0/meta/bases', apiKey)
  if (!data.bases) {
    console.error('Failed to list bases:', data)
    throw new Error('Error listing Airtable bases')
  }
  // transform data.bases into Folder[]
  const folders: Folder[] = (data.bases || []).map((base: any) => ({
    id: base.id,
    title: base.name,
    type: 'folder'
  }))
  lastBases = folders
  return folders
}

/**
 * Lists the tables of an Airtable base via the official library
 * @param baseId The identifier of the Airtable base
 * @returns An array of the base's tables
 */
const listTables = async (apiKey: string, baseId: string): Promise<ResourceList> => {
  // The official library does not allow listing tables directly,
  // so we use the REST API meta/tables
  const data = await fetchWithMemo(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, apiKey)
  if (!data.tables) {
    console.error('Failed to list tables:', data)
    throw new Error('Error listing tables')
  }

  // transforme les data en Resource[]
  const resources: ResourceList = (data.tables || []).map((base: any) => ({
    id: baseId + '/' + base.id,
    title: base.name,
    type: 'resource',
    format: 'csv',
    origin: `https://airtable.com/${baseId}/${base.id}`,
  } as ResourceList[number])
  )
  return resources
}

export const list = async ({ secrets, params }: ListContext<AirtableConfig, AirtableCapabilities>): ReturnType<CatalogPlugin<AirtableConfig>['list']> => {
  let res: (Folder[] | ResourceList)
  const path: Folder[] = []
  if (!params.currentFolderId) {
    res = await listBases(secrets.apiKey)
  } else {
    res = await listTables(secrets.apiKey, params.currentFolderId)
    path.push({
      id: params.currentFolderId,
      title: lastBases.find((folder) => folder.id === params.currentFolderId)?.title ?? params.currentFolderId,
      type: 'folder'
    })
  }
  return {
    results: res,
    count: res.length,
    path
  }
}
