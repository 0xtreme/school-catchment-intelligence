import fs from 'node:fs';
import fsp from 'node:fs/promises';
import https from 'node:https';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const projectRoot = path.resolve(__dirname, '..');
const rawDir = path.join(projectRoot, 'data', 'raw');

const SOURCES = [
  {
    id: 'acara_school_profile_2025',
    publisher: 'ACARA',
    description: 'My School Data Access Program - School Profile 2025 (ICSEA, enrolments, staffing).',
    url: 'https://dataandreporting.blob.core.windows.net/anrdataportal/Data-Access-Program/School%20Profile%202025.xlsx',
    filename: 'acara_school_profile_2025.xlsx',
    official_release_date: '2025-01-01',
  },
  {
    id: 'acara_school_location_2025',
    publisher: 'ACARA',
    description: 'My School Data Access Program - School Location 2025 (lat/lon, SA2, remoteness).',
    url: 'https://dataandreporting.blob.core.windows.net/anrdataportal/Data-Access-Program/School%20Location%202025.xlsx',
    filename: 'acara_school_location_2025.xlsx',
    official_release_date: '2025-01-01',
  },
  {
    id: 'abs_census_2021_g02_sa2',
    publisher: 'ABS Data API',
    description: 'Census 2021 selected medians and averages for SA2 and above.',
    url: 'https://data.api.abs.gov.au/rest/data/C21_G02_SA2?format=csvfile',
    filename: 'census_2021_g02_sa2.csv',
    official_release_date: '2022-06-28',
  },
  {
    id: 'abs_annual_erp_asgs2021',
    publisher: 'ABS Data API',
    description: 'Estimated Resident Population by ASGS 2021 geographies.',
    url: 'https://data.api.abs.gov.au/rest/data/ABS_ANNUAL_ERP_ASGS2021?format=csvfile',
    filename: 'annual_erp_asgs2021.csv',
    official_release_date: '2025-03-20',
  },
  {
    id: 'nsw_school_catchments',
    publisher: 'NSW Department of Education (Data NSW)',
    description: 'NSW government school intake zones (catchment areas).',
    url: 'https://data.nsw.gov.au/data/dataset/8b1e8161-7252-43d9-81ed-6311569cb1d7/resource/32d6f502-ddb1-45d9-b114-5e34ddfd33ac/download/catchments.zip',
    filename: 'nsw_school_catchments.zip',
    official_release_date: '2026-03-19',
  },
  {
    id: 'vic_school_zones_2026',
    publisher: 'Victorian Department of Education (DataVic)',
    description: 'Victorian Government School Zones 2026.',
    url: 'https://www.education.vic.gov.au/Documents/about/research/datavic/dv398_DataVic_School_Zones_2026.zip',
    filename: 'vic_school_zones_2026.zip',
    official_release_date: '2025-03-27',
  },
  {
    id: 'qld_primary_catchments_2025',
    publisher: 'Queensland Department of Education (data.qld.gov.au)',
    description: 'Queensland state primary school catchments 2025 (KML).',
    url: 'https://www.data.qld.gov.au/dataset/b01b50fc-b8ab-4c88-bc4a-34d42930fea8/resource/a35846d9-e320-46fc-aea1-b477001ca485/download/primary-catchments-2025.kml',
    filename: 'qld_primary_catchments_2025.kml',
    official_release_date: '2025-01-01',
  },
  {
    id: 'qld_junior_secondary_catchments_2025',
    publisher: 'Queensland Department of Education (data.qld.gov.au)',
    description: 'Queensland junior secondary catchments 2025 (KML).',
    url: 'https://www.data.qld.gov.au/dataset/b01b50fc-b8ab-4c88-bc4a-34d42930fea8/resource/2557305a-5339-4945-819f-551bd917fe39/download/junior-secondary-catchments-2025.kml',
    filename: 'qld_junior_secondary_catchments_2025.kml',
    official_release_date: '2025-01-01',
  },
  {
    id: 'qld_senior_secondary_catchments_2025',
    publisher: 'Queensland Department of Education (data.qld.gov.au)',
    description: 'Queensland senior secondary catchments 2025 (KML).',
    url: 'https://www.data.qld.gov.au/dataset/b01b50fc-b8ab-4c88-bc4a-34d42930fea8/resource/930ba950-9661-4edb-bb04-8cdfb3305d33/download/senior-secondary-catchments-2025.kml',
    filename: 'qld_senior_secondary_catchments_2025.kml',
    official_release_date: '2025-01-01',
  },
];

function ensureDirectory(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function downloadToFile(url, destination, redirects = 0) {
  return new Promise((resolve, reject) => {
    if (redirects > 8) {
      reject(new Error(`Too many redirects for ${url}`));
      return;
    }

    const request = https.get(
      url,
      {
        headers: {
          'user-agent': 'school-catchment-intelligence/1.0 (+https://github.com/0xtreme)',
          accept: '*/*',
        },
      },
      (response) => {
        const status = response.statusCode ?? 0;
        const location = response.headers.location;

        if (status >= 300 && status < 400 && location) {
          response.resume();
          const nextUrl = new URL(location, url).toString();
          downloadToFile(nextUrl, destination, redirects + 1).then(resolve).catch(reject);
          return;
        }

        if (status < 200 || status >= 300) {
          reject(new Error(`Failed download ${url}: HTTP ${status}`));
          return;
        }

        const fileStream = fs.createWriteStream(destination);
        response.pipe(fileStream);

        fileStream.on('finish', () => {
          fileStream.close((error) => {
            if (error) {
              reject(error);
              return;
            }
            resolve({
              contentLength: response.headers['content-length']
                ? Number(response.headers['content-length'])
                : null,
              contentType: response.headers['content-type'] ?? null,
            });
          });
        });

        fileStream.on('error', (error) => {
          fileStream.close(() => reject(error));
        });
      },
    );

    request.on('error', reject);
    request.setTimeout(180000, () => {
      request.destroy(new Error(`Timeout downloading ${url}`));
    });
  });
}

async function main() {
  ensureDirectory(rawDir);
  const force = process.argv.includes('--force');
  const fetchedAt = new Date().toISOString();

  const manifest = {
    fetched_at: fetchedAt,
    sources: [],
  };

  for (const source of SOURCES) {
    const destination = path.join(rawDir, source.filename);
    const exists = fs.existsSync(destination);

    if (exists && !force) {
      const stats = await fsp.stat(destination);
      manifest.sources.push({
        ...source,
        path: path.relative(projectRoot, destination),
        fetched_at: fetchedAt,
        bytes: stats.size,
        reused_existing_file: true,
      });
      console.log(`[skip] ${source.id} -> ${source.filename} (${stats.size} bytes)`);
      continue;
    }

    console.log(`[download] ${source.id}`);
    const responseMeta = await downloadToFile(source.url, destination);
    const stats = await fsp.stat(destination);

    manifest.sources.push({
      ...source,
      path: path.relative(projectRoot, destination),
      fetched_at: fetchedAt,
      bytes: stats.size,
      content_type: responseMeta.contentType,
      content_length_header: responseMeta.contentLength,
      reused_existing_file: false,
    });

    console.log(`[ok] ${source.filename} (${stats.size} bytes)`);
  }

  const manifestPath = path.join(rawDir, 'sources-manifest.json');
  await fsp.writeFile(manifestPath, JSON.stringify(manifest, null, 2));
  console.log(`\nWrote ${path.relative(projectRoot, manifestPath)}`);
}

main().catch((error) => {
  console.error(error.stack || error.message);
  process.exitCode = 1;
});
