export default {
  folders: {
    'category-geospatial': {
      title: 'Données Géospatiales',
      parentId: null
    },
    'subcategory-transport': {
      title: 'Transport',
      parentId: 'category-geospatial'
    },
    'category-economic': {
      title: 'Données Économiques',
      parentId: 'subcategory-transport'
    },
    'subcategory-boundaries': {
      title: 'Délimitations administratives',
      parentId: 'category-geospatial'
    },
    'category-demographic': {
      title: 'Données Démographiques',
      parentId: null
    }
  },
  resources: {
    'category-geospatial/subcategory-transport/category-economic/resource-gdp-data': {
      title: 'PIB par région',
      description: 'Produit intérieur brut par région française',
      format: 'json',
      url: 'https://example.com/gdp-data.json',
      mimeType: 'application/json',
      size: 512000,
      folderId: 'category-economic'
    },
    'category-geospatial/subcategory-transport/resource-metro-stations': {
      title: 'Stations de métro Paris',
      description: 'Liste complète des stations de métro parisien avec coordonnées',
      format: 'geojson',
      url: 'https://example.com/metro-stations.geojson',
      mimeType: 'application/geo+json',
      size: 2048000,
      folderId: 'subcategory-transport'
    },
    'category-geospatial/subcategory-transport/resource-bus-lines': {
      title: 'Lignes de bus Paris',
      description: 'Tracés des lignes de bus avec horaires',
      format: 'csv',
      url: 'https://example.com/bus-lines.csv',
      mimeType: 'text/csv',
      size: 1024000,
      folderId: 'subcategory-transport'
    },
    'category-geospatial/subcategory-transport/resource-tram-lines': {
      title: 'Lignes de tramway Paris',
      description: 'Tracés des lignes de tramway avec arrêts',
      format: 'geojson',
      url: 'https://example.com/tram-lines.geojson',
      mimeType: 'application/geo+json',
      size: 1536000,
      folderId: 'subcategory-transport'
    },
    'category-geospatial/subcategory-transport/resource-bike-stations': {
      title: 'Stations Vélib Paris',
      description: 'Emplacements et disponibilités des stations Vélib',
      format: 'json',
      url: 'https://example.com/velib-stations.json',
      mimeType: 'application/json',
      size: 512000,
      folderId: 'subcategory-transport'
    },
    'category-geospatial/subcategory-transport/resource-parking-lots': {
      title: 'Parkings publics Paris',
      description: 'Localisation et capacité des parkings publics',
      format: 'csv',
      url: 'https://example.com/parking-lots.csv',
      mimeType: 'text/csv',
      size: 768000,
      folderId: 'subcategory-transport'
    },
    'category-geospatial/subcategory-transport/resource-taxi-stations': {
      title: 'Stations de taxi Paris',
      description: 'Emplacements des stations de taxi officielles',
      format: 'geojson',
      url: 'https://example.com/taxi-stations.geojson',
      mimeType: 'application/geo+json',
      size: 256000,
      folderId: 'subcategory-transport'
    },
    'category-geospatial/subcategory-transport/resource-traffic-data': {
      title: 'Données de trafic temps réel',
      description: 'Informations de trafic en temps réel sur les axes principaux',
      format: 'json',
      url: 'https://example.com/traffic-data.json',
      mimeType: 'application/json',
      size: 2048000,
      folderId: 'subcategory-transport'
    },
    'category-geospatial/subcategory-transport/resource-road-works': {
      title: 'Travaux de voirie Paris',
      description: 'Informations sur les travaux en cours et à venir',
      format: 'csv',
      url: 'https://example.com/road-works.csv',
      mimeType: 'text/csv',
      size: 384000,
      folderId: 'subcategory-transport'
    },
    'category-geospatial/subcategory-transport/resource-speed-limits': {
      title: 'Limitations de vitesse',
      description: 'Cartographie des limitations de vitesse par rue',
      format: 'shapefile',
      url: 'https://example.com/speed-limits.zip',
      mimeType: 'application/zip',
      size: 3072000,
      folderId: 'subcategory-transport'
    },
    'category-geospatial/subcategory-transport/resource-pedestrian-zones': {
      title: 'Zones piétonnes Paris',
      description: 'Délimitation des zones réservées aux piétons',
      format: 'geojson',
      url: 'https://example.com/pedestrian-zones.geojson',
      mimeType: 'application/geo+json',
      size: 1024000,
      folderId: 'subcategory-transport'
    },
    'category-geospatial/subcategory-transport/resource-cycle-lanes': {
      title: 'Pistes cyclables Paris',
      description: 'Réseau des pistes cyclables et voies vertes',
      format: 'geojson',
      url: 'https://example.com/cycle-lanes.geojson',
      mimeType: 'application/geo+json',
      size: 1792000,
      folderId: 'subcategory-transport'
    },
    'category-geospatial/subcategory-transport/resource-public-transport-schedules': {
      title: 'Horaires transports publics',
      description: 'Horaires théoriques des lignes de transport public',
      format: 'gtfs',
      url: 'https://example.com/schedules.zip',
      mimeType: 'application/zip',
      size: 25600000,
      folderId: 'subcategory-transport'
    },
    'category-geospatial/subcategory-boundaries/resource-communes': {
      title: 'Limites communales',
      description: 'Délimitations des communes françaises',
      format: 'shapefile',
      url: 'https://example.com/communes.zip',
      mimeType: 'application/zip',
      size: 15360000,
      folderId: 'subcategory-boundaries'
    },
    'category-demographic/resource-population-2023': {
      title: 'Population par commune 2023',
      description: 'Données démographiques détaillées par commune',
      format: 'xlsx',
      url: 'https://example.com/population-2023.xlsx',
      mimeType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      size: 5120000,
      folderId: 'category-demographic'
    }
  }
} as {
  folders: Record<string, { title: string; parentId: string | null }>;
  resources: Record<string, { title: string; description: string; format: string; url: string; mimeType: string; size: number; folderId: string }>;
}
