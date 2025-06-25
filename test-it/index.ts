import type { CatalogPlugin } from '@data-fair/lib-common-types/catalog/index.js'
import { strict as assert } from 'node:assert'
import { it, describe, before, beforeEach } from 'node:test'
import fs from 'fs-extra'

// Import plugin and use default type like it's done in Catalogs
import plugin from '../index.ts'
const catalogPlugin: CatalogPlugin = plugin as CatalogPlugin

/** Mock catalog configuration for testing purposes. */
const catalogConfig = {
  url: 'http://localhost:3000',
}

describe('catalog-mock', () => {
  it('should list resources and folder from root', async () => {
    const res = await catalogPlugin.list({
      catalogConfig,
      params: {}
    })

    assert.equal(res.count, 2, 'Expected 2 items in the root folder')
    assert.equal(res.results.length, 2)
    assert.equal(res.results[0].type, 'folder', 'Expected folders in the root folder')

    assert.equal(res.path.length, 0, 'Expected no path for root folder')
  })

  it('should list resources and folder from a folder', async () => {
    const res = await catalogPlugin.list({
      catalogConfig,
      params: { currentFolderId: 'category-geospatial' }
    })

    assert.equal(res.count, 2, 'Expected 2 items in category-geospatial folder')
    assert.equal(res.results.length, 2)
    assert.equal(res.results[0].type, 'folder', 'Expected folders in category-geospatial folder')

    assert.equal(res.path.length, 1, 'Expected path to contain the current folder')
    assert.equal(res.path[0].id, 'category-geospatial')
  })

  it('should list resources and folder with pagination', { skip: 'This catalog does not support pagination' }, async () => {})

  it('should get a resource', async () => {
    const resourceId = 'category-demographic/resource-population-2023'
    const resource = await catalogPlugin.getResource(catalogConfig, resourceId)
    assert.ok(resource, 'The resource should exist')

    assert.equal(resource.id, resourceId, 'Resource ID should match')
    assert.equal(resource.title, 'Population par commune 2023', 'Resource title should match')
    assert.equal(resource.type, 'resource', 'Expected resource type to be "resource"')
  })

  describe('should download a resource', async () => {
    const tmpDir = './data/test/downloads'

    // Ensure the temporary directory exists once for all tests
    before(async () => await fs.ensureDir(tmpDir))

    // Clear the temporary directory before each test
    beforeEach(async () => await fs.emptyDir(tmpDir))

    it('with correct params', async () => {
      const resourceId = 'category-demographic/resource-population-2023'
      const downloadUrl = await catalogPlugin.downloadResource({
        catalogConfig,
        resourceId,
        importConfig: {
          nbRows: 10
        },
        tmpDir
      })

      assert.ok(downloadUrl, 'Download URL should not be undefined')
      assert.ok(downloadUrl.endsWith('jdd-mock.csv'), 'Download URL should contain the downloaded file name')

      // Check if the file exists
      const fileExists = await fs.pathExists(downloadUrl)
      assert.ok(fileExists, 'The downloaded file should exist')
    })

    it('should fail for bad importConfig', async () => {
      const resourceId = 'category-demographic/resource-population-2023'

      await assert.rejects(
        async () => {
          await catalogPlugin.downloadResource({
            catalogConfig,
            resourceId,
            importConfig: {
              nbRows: 100 // This exceeds the maximum of 50
            },
            tmpDir
          })
        },
        'Should throw a validation error for nbRows > 50'
      )
    })

    it('should fail for resource not found', async () => {
      const resourceId = 'non-existent-resource'

      await assert.rejects(
        async () => {
          await catalogPlugin.downloadResource({
            catalogConfig,
            resourceId,
            importConfig: {
              nbRows: 10
            },
            tmpDir
          })
        },
        /not found|does not exist/i,
        'Should throw an error for non-existent resource'
      )
    })
  })

  it('should publish a resource', async () => {
    const dataset = {
      id: 'test-dataset',
      title: 'Test Dataset',
      description: 'This is a test dataset'
    }
    const publication = { isResource: false }
    const publicationSite = {
      title: 'Test Site',
      url: 'http://example.com',
      datasetUrlTemplate: 'http://example.com/data-fair/{id}'
    }

    const result = await catalogPlugin.publishDataset({ catalogConfig, dataset, publication, publicationSite })
    assert.ok(result, 'The publication should be successful')
    assert.ok(result.remoteDataset, 'The returned publication should have a remote dataset')
    assert.equal(result.remoteDataset.id, 'my-mock-test-dataset', 'The returned publication should have a remote dataset with an ID')
    assert.equal(result.isResource, publication.isResource, 'Publication type should not be changed')
  })

  it('should delete a resource', async () => {
    const datasetId = 'test-dataset'
    const resourceId = 'category-demographic/resource-population-2023'

    await catalogPlugin.deleteDataset({ catalogConfig, datasetId, resourceId })
    // Since this is a mock plugin, we cannot verify the deletion, but we can check that no error is thrown
    assert.ok(true, 'Delete operation should not throw an error')
  })
})
