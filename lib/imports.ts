import type { ListContext, Folder, CatalogPlugin } from '@data-fair/lib-common-types/catalog/index.js'
import type { AirtableConfig } from '#types'
import type { AirtableCapabilities } from './capabilities.ts'

let lastBases: Folder[]
type ResourceList = Awaited<ReturnType<CatalogPlugin['list']>>['results']

/**
 * Liste les Bases disponible à partir d'une clé API / Personnal Access Token
 * @returns
 */
const listBases = async (apiKey: string): Promise<Folder[]> => {
  /* La librairie JS ne permet pas de lister les bases donc cela est fait via un fetch */
  const response = await fetch('https://api.airtable.com/v0/meta/bases', {
    headers: {
      Authorization: `Bearer ${apiKey}`,
      'Content-Type': 'application/json'
    }
  })
  if (!response.ok) {
    console.error(`Failed to list bases: ${response.status} ${response.statusText}`)
    throw new Error('Erreur dans le listage des bases / Clé d\'API possiblement incorrecte')
  }

  const data = await response.json()
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
  const response = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
    headers: {
      Authorization: `Bearer ${apiKey}`,
      'Content-Type': 'application/json'
    }
  })
  if (!response.ok) {
    console.error(`Failed to list tables: ${response.status} ${response.statusText}`)
    throw new Error('Erreur dans le listage des tables')
  }

  const data = await response.json()
  // transforme les data en Resource[]
  const resources = (data.tables || []).map((base: any) => ({
    id: baseId + '/' + base.id,
    title: base.name,
    type: 'resource',
    format: 'csv',
    origin: `https://api.airtable.com/v0/${baseId}/${base.id}`
  } as ResourceList[number])
  )
  return resources
}

export const list = async ({ secrets, params }: ListContext<AirtableConfig, AirtableCapabilities>): ReturnType<CatalogPlugin<AirtableConfig>['list']> => {
  // On suppose que l'id de la base est passé dans params.baseId
  let res: (Folder[] | ResourceList)
  const path: Folder[] = []
  if (params.currentFolderId) {
    res = await listTables(secrets.apiKey, params.currentFolderId)
    path.push({
      id: params.currentFolderId,
      title: lastBases.find((folder) => folder.id === params.currentFolderId)?.title ?? params.currentFolderId,
      type: 'folder'
    })
  } else {
    res = await listBases(secrets.apiKey)
  }
  return {
    results: res,
    count: res.length,
    path
  }
}
