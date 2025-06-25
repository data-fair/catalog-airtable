import type { ListContext, DownloadResourceContext, Folder, Resource } from '@data-fair/lib-common-types/catalog/index.js'
import type { MockConfig } from '#types'
import type capabilities from './capabilities.ts'

export const list = async ({ params }: ListContext<MockConfig, typeof capabilities>): Promise<{ count: number; results: (Folder | Resource)[]; path: Folder[] }> => {
  await new Promise(resolve => setTimeout(resolve, 1000)) // Simulate a delay for the mock plugin

  const clone = (await import('@data-fair/lib-utils/clone.js')).default
  const tree = clone((await import('./resources.ts')).default)

  /**
   * Extracts folders and resources for a given parent/folder ID
   * @param resources - The resources object containing folders and resources
   * @param targetId - The parent ID for folders or folder ID for resources (null for root level)
   * @returns Array of folders and resources matching the criteria
   */
  const getFoldersAndResources = (targetId: string | null): (Folder | Resource)[] => {
    const folders = Object.keys(tree.folders).reduce((acc: Folder[], key) => {
      if (tree.folders[key].parentId === targetId) {
        acc.push({
          id: key,
          title: tree.folders[key].title,
          type: 'folder'
        })
      }
      return acc
    }, [])

    const folderResources = Object.keys(tree.resources).reduce((acc: Resource[], key) => {
      if (tree.resources[key].folderId === targetId) {
        // Exclude folderId from the resource object
        const { folderId, ...rest } = tree.resources[key]

        // Add the resource to the result with type 'resource'
        acc.push({
          id: key,
          ...rest,
          type: 'resource'
        })
      }
      return acc
    }, [])

    return [...folders, ...folderResources]
  }

  const path: Folder[] = []
  const res: (Folder | Resource)[] = getFoldersAndResources(params.currentFolderId ?? null)

  // Get path to current folder if specified
  if (params.currentFolderId) {
    // Get current folder
    const currentFolder = tree.folders[params.currentFolderId]
    if (!currentFolder) throw new Error(`Folder with ID ${params.currentFolderId} not found`)

    // Get path to current folder (parents folders)
    let parentId = currentFolder.parentId
    while (parentId !== null) {
      const parentFolder = tree.folders[parentId]
      if (!parentFolder) throw new Error(`Parent folder with ID ${parentId} not found`)

      // Add the parent to the start of the list to avoid reversing the path later
      path.unshift({
        id: parentId,
        title: parentFolder.title,
        type: 'folder'
      })
      parentId = parentFolder.parentId
    }

    // Add the current folder to the path
    path.push({
      id: params.currentFolderId,
      title: currentFolder.title,
      type: 'folder'
    })
  }

  return {
    count: res.length,
    results: res,
    path
  }
}

export const getResource = async (catalogConfig: MockConfig, resourceId: string): Promise<Resource> => {
  await new Promise(resolve => setTimeout(resolve, 1000))

  const clone = (await import('@data-fair/lib-utils/clone.js')).default
  const resources = clone((await import('./resources.ts')).default)

  const resource = resources.resources[resourceId]
  if (!resource) { throw new Error(`Resource with ID ${resourceId} not found`) }
  const { folderId, ...rest } = resource

  return {
    id: resourceId,
    ...rest,
    type: 'resource'
  }
}

export const downloadResource = async ({ catalogConfig, resourceId, importConfig, tmpDir }: DownloadResourceContext<MockConfig>) => {
  await new Promise(resolve => setTimeout(resolve, 1000))

  // Validate the importConfig
  const { returnValid } = await import('#type/importConfig/index.ts')
  returnValid(importConfig)

  // First check if the resource exists
  const resource = await getResource(catalogConfig, resourceId)
  if (!resource) return undefined

  // Import necessary modules dynamically
  const fs = await import('node:fs/promises')
  const path = await import('node:path')

  // Simulate downloading by copying a dummy file with limited rows
  const sourceFile = path.join(import.meta.dirname, 'jdd-mock.csv')
  const destFile = path.join(tmpDir, 'jdd-mock.csv')
  const data = await fs.readFile(sourceFile, 'utf8')

  // Limit the number of rows to importConfig.nbRows (Header excluded)
  const lines = data.split('\n').slice(1, importConfig.nbRows).join('\n')
  await fs.writeFile(destFile, lines, 'utf8')
  return destFile
}
