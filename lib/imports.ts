import type { Folder, CatalogPlugin, ListResourcesContext } from '@data-fair/types-catalogs'
import type { AirtableConfig } from '#types'
import type { AirtableCapabilities } from './capabilities.ts'
import memoize from 'memoize'

let lastBases: Folder[]
type ResourceList = Awaited<ReturnType<CatalogPlugin['listResources']>>['results']

const fetchWithMemo = memoize(
  async (url: string, apiKey: string): Promise<any> => {
    const response = await fetch(url, {
      headers: {
        Authorization: `Bearer ${apiKey}`,
        'Content-Type': 'application/json'
      }
    })
    if (!response.ok) {
      console.error(`Failed to fetch from Airtable API: ${response.status} ${response.statusText}`)
      throw new Error('Erreur dans la récupération des données depuis Airtable')
    }
    return await (response.json())
  },
  { maxAge: 1000 * 30 } // Cache for 30 seconds
)

/**
 * Liste les Bases disponible à partir d'une clé API / Personnal Access Token
 * @returns
 */
const listBases = async (apiKey: string): Promise<Folder[]> => {
  /* La librairie JS ne permet pas de lister les bases donc cela est fait via un fetch */
  const data = await fetchWithMemo('https://api.airtable.com/v0/meta/bases', apiKey)
  if (!data.bases) {
    console.error('Failed to list bases:', data)
    throw new Error('Erreur dans le listage des bases Airtable')
  }
  // transforme data.bases en Folder[]
  const folders: Folder[] = (data.bases || []).map((base: any) => ({
    id: base.id,
    title: base.name,
    type: 'folder'
  }))
  lastBases = folders
  return folders
}

/**
 * Liste les tables d'une base Airtable via la librairie officielle
 * @param baseId L'identifiant de la base Airtable
 * @returns Un tableau des tables de la base
 */
const listTables = async (apiKey: string, baseId: string): Promise<ResourceList> => {
  // La librairie officielle ne permet pas de lister les tables directement,
  // donc on utilise l'API REST meta/tables
  const data = await fetchWithMemo(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, apiKey)
  if (!data.tables) {
    console.error('Failed to list tables:', data)
    throw new Error('Erreur dans le listage des tables')
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

export const listResources = async ({ secrets, params }: ListResourcesContext<AirtableConfig, AirtableCapabilities>): ReturnType<CatalogPlugin<AirtableConfig>['listResources']> => {
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
