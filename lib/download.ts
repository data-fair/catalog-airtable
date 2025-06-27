import type { DownloadResourceContext } from '@data-fair/lib-common-types/catalog/index.js'
import type { AirtableConfig } from '#types'
import Airtable from 'airtable'
import { stringify } from 'csv-stringify/sync'

export const downloadResource = async ({ secrets, resourceId, tmpDir }: DownloadResourceContext<AirtableConfig>) => {
  const [baseId, tableId] = resourceId.split('/')
  const base = new Airtable({ apiKey: secrets.apiKey }).base(baseId)

  const fs = await import('fs')
  const path = await import('path')
  const outputPath = path.join(tmpDir, `${tableId}.csv`)
  const writeStream = fs.createWriteStream(outputPath)

  let headerFields: string[] | undefined
  let isFirstChunk = true

  try {
    await new Promise<void>((resolve, reject) => {
      base(tableId)
        .select()
        .eachPage(
          (records: readonly Airtable.Record<Airtable.FieldSet>[], fetchNextPage: () => void) => {
            if (records.length === 0) {
              fetchNextPage()
              return
            }

            if (!headerFields && records.length > 0) {
              headerFields = Object.keys(records[0].fields)
            }

            const rows = records.map((record) => record.fields)
            const csv = stringify(rows, {
              header: isFirstChunk,
              columns: headerFields,
            })

            writeStream.write(csv)
            isFirstChunk = false
            fetchNextPage()
          },
          function done (err: any) {
            if (err) {
              console.log(err)
              reject(new Error('Erreur lors de la récupération des enregistrements depuis Airtable.'))
            } else {
              writeStream.end()
              resolve()
            }
          }
        )
    })

    await new Promise<void>((resolve, reject) => {
      writeStream.on('finish', () => resolve())
      writeStream.on('error', (err) => {
        console.log(err)
        reject(new Error('Erreur lors de l\'écriture du fichier CSV.'))
      }
      )
    })

    return outputPath
  } catch (error) {
    if (error instanceof Error) {
      if (error.message.includes('Erreur lors de la récupération')) {
        throw new Error('Impossible de récupérer les données depuis Airtable. Veuillez vérifier votre connexion et réessayer.')
      } else if (error.message.includes('Erreur lors de l\'écriture')) {
        throw new Error('Impossible d\'écrire le fichier CSV. Veuillez vérifier les permissions et réessayer.')
      }
    }
    throw new Error('Une erreur est survenue lors du traitement de votre demande.')
  }
}
