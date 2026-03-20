import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import readline from 'node:readline';
import { execSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import XLSX from 'xlsx';
import shapefile from 'shapefile';
import { DOMParser } from '@xmldom/xmldom';
import { kml as kmlToGeoJSON } from '@tmcw/togeojson';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const projectRoot = path.resolve(__dirname, '..');

const RAW_DIR = path.join(projectRoot, 'data', 'raw');
const PROCESSED_DIR = path.join(projectRoot, 'data', 'processed');
const PUBLIC_DATA_DIR = path.join(projectRoot, 'public', 'data');
const DOCS_DATA_DIR = path.join(projectRoot, 'docs', 'data');
const TMP_DIR = path.join(projectRoot, '.tmp', 'build');

const RAW_FILES = {
  acaraProfile: path.join(RAW_DIR, 'acara_school_profile_2025.xlsx'),
  acaraLocation: path.join(RAW_DIR, 'acara_school_location_2025.xlsx'),
  censusG02: path.join(RAW_DIR, 'census_2021_g02_sa2.csv'),
  erp: path.join(RAW_DIR, 'annual_erp_asgs2021.csv'),
  nswCatchments: path.join(RAW_DIR, 'nsw_school_catchments.zip'),
  vicZones: path.join(RAW_DIR, 'vic_school_zones_2026.zip'),
  qldPrimary: path.join(RAW_DIR, 'qld_primary_catchments_2025.kml'),
  qldJuniorSecondary: path.join(RAW_DIR, 'qld_junior_secondary_catchments_2025.kml'),
  qldSeniorSecondary: path.join(RAW_DIR, 'qld_senior_secondary_catchments_2025.kml'),
  sourceManifest: path.join(RAW_DIR, 'sources-manifest.json'),
};

const STATE_CODE_TO_ABBR = {
  1: 'NSW',
  2: 'VIC',
  3: 'QLD',
  4: 'SA',
  5: 'WA',
  6: 'TAS',
  7: 'NT',
  8: 'ACT',
  9: 'OT',
};

const STATE_ABBR_TO_NAME = {
  NSW: 'New South Wales',
  VIC: 'Victoria',
  QLD: 'Queensland',
  SA: 'South Australia',
  WA: 'Western Australia',
  TAS: 'Tasmania',
  NT: 'Northern Territory',
  ACT: 'Australian Capital Territory',
  OT: 'Other Territories',
};

function ensureDirectory(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function parseCsvLine(line) {
  const fields = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i += 1) {
    const char = line[i];
    if (char === '"') {
      const next = line[i + 1];
      if (inQuotes && next === '"') {
        current += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (char === ',' && !inQuotes) {
      fields.push(current);
      current = '';
      continue;
    }

    current += char;
  }

  fields.push(current);
  return fields;
}

function cleanText(value) {
  return (value ?? '').toString().replace(/\u00a0/g, ' ').trim();
}

function parseNumber(value) {
  const text = cleanText(value)
    .replace(/,/g, '')
    .replace(/\s+/g, '');

  if (!text) {
    return null;
  }

  const lowered = text.toLowerCase();
  if (['na', 'n/a', 'np', '..', '-', '--'].includes(lowered)) {
    return null;
  }

  const parsed = Number(text);
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeId(value) {
  const text = cleanText(value);
  if (!text) {
    return null;
  }

  if (/^\d+$/.test(text)) {
    return text;
  }

  const asNumber = Number(text);
  if (Number.isFinite(asNumber)) {
    return Math.round(asNumber).toString();
  }

  return text;
}

function percentileFromSorted(value, sortedArray) {
  if (!Number.isFinite(value) || !sortedArray.length) {
    return 0.5;
  }

  let low = 0;
  let high = sortedArray.length;

  while (low < high) {
    const mid = (low + high) >> 1;
    if (sortedArray[mid] <= value) {
      low = mid + 1;
    } else {
      high = mid;
    }
  }

  if (sortedArray.length === 1) {
    return 1;
  }

  return (low - 1) / (sortedArray.length - 1);
}

async function forEachCsvRow(filePath, onRow) {
  const stream = fs.createReadStream(filePath, { encoding: 'utf8' });
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  let headers = null;
  for await (const rawLine of rl) {
    const line = rawLine.replace(/\r$/, '');
    if (!line) {
      continue;
    }

    const columns = parseCsvLine(line);
    if (!headers) {
      headers = columns;
      continue;
    }

    const row = {};
    headers.forEach((header, index) => {
      row[header] = columns[index] ?? '';
    });

    // eslint-disable-next-line no-await-in-loop
    await onRow(row);
  }
}

function normalizeSchoolNameStrict(name) {
  return cleanText(name).toLowerCase().replace(/[^a-z0-9]+/g, '');
}

function normalizeSchoolNameLoose(name) {
  return cleanText(name)
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\b(the|state|government|public|school|college|campus|catchment|zone|junior|senior|secondary|primary|academy)\b/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/\s+/g, '');
}

function extractCatchmentName(properties) {
  if (!properties || typeof properties !== 'object') {
    return null;
  }

  const entries = Object.entries(properties);
  const preferred = [
    'School_Name',
    'SCHOOL_NAME',
    'school_name',
    'SchoolName',
    'schoolname',
    'name',
    'Name',
    'CENTRE_NAME',
    'Centre_name',
    'centre_name',
    'school',
  ];

  for (const key of preferred) {
    if (key in properties) {
      const value = cleanText(properties[key]);
      if (value) {
        return value;
      }
    }
  }

  for (const [key, rawValue] of entries) {
    if (!/name/i.test(key)) {
      continue;
    }
    const value = cleanText(rawValue);
    if (value) {
      return value;
    }
  }

  return null;
}

function extractQldNameFromDescription(description) {
  const text = cleanText(description);
  if (!text) {
    return null;
  }

  const match = text.match(/Centre_name<\/td><td>([^<]+)/i);
  if (match) {
    return cleanText(match[1]);
  }

  return null;
}

function thinRing(ring, step = 3) {
  if (!Array.isArray(ring) || ring.length <= 6) {
    return ring;
  }

  const out = [ring[0]];
  for (let i = 1; i < ring.length - 1; i += step) {
    out.push(ring[i]);
  }
  out.push(ring[ring.length - 1]);

  const [firstLng, firstLat] = out[0];
  const [lastLng, lastLat] = out[out.length - 1];
  if (firstLng !== lastLng || firstLat !== lastLat) {
    out.push(out[0]);
  }

  return out;
}

function thinGeometry(geometry, step = 3) {
  if (!geometry) {
    return null;
  }

  if (geometry.type === 'Polygon') {
    return {
      ...geometry,
      coordinates: geometry.coordinates.map((ring) => thinRing(ring, step)),
    };
  }

  if (geometry.type === 'MultiPolygon') {
    return {
      ...geometry,
      coordinates: geometry.coordinates.map((polygon) =>
        polygon.map((ring) => thinRing(ring, step)),
      ),
    };
  }

  return geometry;
}

function geometryBounds(geometry) {
  let minLng = Infinity;
  let minLat = Infinity;
  let maxLng = -Infinity;
  let maxLat = -Infinity;

  function scan(coords) {
    if (!Array.isArray(coords[0])) {
      const [lng, lat] = coords;
      minLng = Math.min(minLng, lng);
      minLat = Math.min(minLat, lat);
      maxLng = Math.max(maxLng, lng);
      maxLat = Math.max(maxLat, lat);
      return;
    }
    coords.forEach(scan);
  }

  scan(geometry.coordinates);
  return { minLng, minLat, maxLng, maxLat };
}

function centroidFromGeometry(geometry) {
  const bounds = geometryBounds(geometry);
  return {
    lng: (bounds.minLng + bounds.maxLng) / 2,
    lat: (bounds.minLat + bounds.maxLat) / 2,
  };
}

function haversineKm(lat1, lon1, lat2, lon2) {
  const toRad = (value) => (value * Math.PI) / 180;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) * Math.sin(dLon / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return 6371 * c;
}

function loadAcaraProfiles() {
  if (!fs.existsSync(RAW_FILES.acaraProfile)) {
    throw new Error('Missing ACARA school profile file. Run npm run fetch:data first.');
  }

  const workbook = XLSX.readFile(RAW_FILES.acaraProfile, { cellDates: false });
  const rows = XLSX.utils.sheet_to_json(workbook.Sheets['SchoolProfile 2025'], {
    raw: true,
    defval: null,
  });

  const byId = new Map();
  for (const row of rows) {
    const id = normalizeId(row['ACARA SML ID']);
    if (!id) {
      continue;
    }

    byId.set(id, {
      acara_sml_id: id,
      school_name: cleanText(row['School Name']),
      suburb: cleanText(row.Suburb),
      state: cleanText(row.State),
      postcode: cleanText(row.Postcode),
      school_sector: cleanText(row['School Sector']),
      school_type: cleanText(row['School Type']),
      year_range: cleanText(row['Year Range']),
      geolocation_bucket: cleanText(row.Geolocation),
      icsea: parseNumber(row.ICSEA),
      icsea_percentile: parseNumber(row['ICSEA Percentile']),
      total_enrolments: parseNumber(row['Total Enrolments']),
      girls_enrolments: parseNumber(row['Girls Enrolments']),
      boys_enrolments: parseNumber(row['Boys Enrolments']),
      fte_teaching_staff: parseNumber(row['Full Time Equivalent Teaching Staff']),
    });
  }

  return byId;
}

function loadAcaraLocations() {
  if (!fs.existsSync(RAW_FILES.acaraLocation)) {
    throw new Error('Missing ACARA school location file. Run npm run fetch:data first.');
  }

  const workbook = XLSX.readFile(RAW_FILES.acaraLocation, { cellDates: false });
  const rows = XLSX.utils.sheet_to_json(workbook.Sheets['SchoolLocations 2025'], {
    raw: true,
    defval: null,
  });

  const byId = new Map();
  for (const row of rows) {
    const id = normalizeId(row['ACARA SML ID']);
    if (!id) {
      continue;
    }

    byId.set(id, {
      acara_sml_id: id,
      latitude: parseNumber(row.Latitude),
      longitude: parseNumber(row.Longitude),
      state: cleanText(row.State),
      suburb: cleanText(row.Suburb),
      sa2_code: normalizeId(row['Statistical Area 2']),
      sa2_name: cleanText(row['Statistical Area 2 Name']),
      remoteness_name: cleanText(row['ABS Remoteness Area Name']),
      lga_name: cleanText(row['Local Government Area Name']),
      school_name_location: cleanText(row['School Name']),
    });
  }

  return byId;
}

async function loadAbsG02BySa2() {
  if (!fs.existsSync(RAW_FILES.censusG02)) {
    throw new Error('Missing ABS Census G02 file. Run npm run fetch:data first.');
  }

  const bySa2 = new Map();

  await forEachCsvRow(RAW_FILES.censusG02, async (row) => {
    if (row.REGION_TYPE !== 'SA2') {
      return;
    }

    const sa2Code = normalizeId(row.REGION);
    if (!sa2Code || !/^\d{9}$/.test(sa2Code)) {
      return;
    }

    if (!bySa2.has(sa2Code)) {
      bySa2.set(sa2Code, {
        sa2_code: sa2Code,
        state_code: Number(sa2Code[0]),
        state_abbr: STATE_CODE_TO_ABBR[Number(sa2Code[0])] ?? 'OT',
        median_age: null,
        median_personal_income_weekly: null,
        median_family_income_weekly: null,
        median_household_income_weekly: null,
        median_monthly_mortgage_repayment: null,
        median_weekly_rent: null,
      });
    }

    const bucket = bySa2.get(sa2Code);
    const value = parseNumber(row.OBS_VALUE);
    if (!Number.isFinite(value)) {
      return;
    }

    switch (row.MEDAVG) {
      case '1':
        bucket.median_age = value;
        break;
      case '2':
        bucket.median_personal_income_weekly = value;
        break;
      case '3':
        bucket.median_family_income_weekly = value;
        break;
      case '4':
        bucket.median_household_income_weekly = value;
        break;
      case '5':
        bucket.median_monthly_mortgage_repayment = value;
        break;
      case '6':
        bucket.median_weekly_rent = value;
        break;
      default:
        break;
    }
  });

  return bySa2;
}

async function loadAbsErpBySa2() {
  if (!fs.existsSync(RAW_FILES.erp)) {
    throw new Error('Missing ABS ERP file. Run npm run fetch:data first.');
  }

  const bySa2 = new Map();

  await forEachCsvRow(RAW_FILES.erp, async (row) => {
    if (row.MEASURE !== 'ERP' || row.REGION_TYPE !== 'SA2') {
      return;
    }

    const sa2Code = normalizeId(row.ASGS_2021);
    if (!sa2Code || !/^\d{9}$/.test(sa2Code)) {
      return;
    }

    const year = Number(row.TIME_PERIOD);
    const value = parseNumber(row.OBS_VALUE);
    if (!Number.isFinite(year) || !Number.isFinite(value)) {
      return;
    }

    if (!bySa2.has(sa2Code)) {
      bySa2.set(sa2Code, []);
    }

    bySa2.get(sa2Code).push({ year, value });
  });

  const resolved = new Map();
  for (const [sa2Code, series] of bySa2.entries()) {
    const sorted = series.sort((a, b) => a.year - b.year);
    const latest = sorted[sorted.length - 1] ?? null;
    const previous = sorted[sorted.length - 2] ?? null;

    resolved.set(sa2Code, {
      population_latest: latest?.value ?? null,
      population_year: latest?.year ?? null,
      population_previous: previous?.value ?? null,
      population_growth_1y_pct:
        Number.isFinite(latest?.value) && Number.isFinite(previous?.value) && previous.value > 0
          ? ((latest.value - previous.value) / previous.value) * 100
          : null,
    });
  }

  return resolved;
}

function mergeSchoolData(profileById, locationById, g02BySa2, erpBySa2) {
  const schools = [];

  for (const [id, profile] of profileById.entries()) {
    const location = locationById.get(id);
    if (!location) {
      continue;
    }

    if (!Number.isFinite(location.latitude) || !Number.isFinite(location.longitude)) {
      continue;
    }

    const sa2Metrics = location.sa2_code ? g02BySa2.get(location.sa2_code) ?? null : null;
    const erpMetrics = location.sa2_code ? erpBySa2.get(location.sa2_code) ?? null : null;

    const familyIncomeWeekly = sa2Metrics?.median_family_income_weekly ?? null;
    const rentWeekly = sa2Metrics?.median_weekly_rent ?? null;
    const mortgageMonthly = sa2Metrics?.median_monthly_mortgage_repayment ?? null;

    const rentBurden =
      Number.isFinite(rentWeekly) && Number.isFinite(familyIncomeWeekly) && familyIncomeWeekly > 0
        ? rentWeekly / familyIncomeWeekly
        : null;

    const mortgageBurden =
      Number.isFinite(mortgageMonthly) && Number.isFinite(familyIncomeWeekly) && familyIncomeWeekly > 0
        ? (mortgageMonthly * 12) / (familyIncomeWeekly * 52)
        : null;

    const burdenParts = [rentBurden, mortgageBurden].filter((value) => Number.isFinite(value));
    const housingBurden = burdenParts.length
      ? burdenParts.reduce((sum, value) => sum + value, 0) / burdenParts.length
      : null;

    const studentTeacherRatio =
      Number.isFinite(profile.total_enrolments) &&
      Number.isFinite(profile.fte_teaching_staff) &&
      profile.fte_teaching_staff > 0
        ? profile.total_enrolments / profile.fte_teaching_staff
        : null;

    schools.push({
      acara_sml_id: id,
      school_name: profile.school_name || location.school_name_location,
      suburb: profile.suburb || location.suburb,
      state: profile.state || location.state,
      state_name: STATE_ABBR_TO_NAME[profile.state || location.state] ?? (profile.state || location.state),
      postcode: profile.postcode,
      school_sector: profile.school_sector,
      school_type: profile.school_type,
      year_range: profile.year_range,
      remoteness_name: location.remoteness_name,
      lga_name: location.lga_name,
      latitude: location.latitude,
      longitude: location.longitude,
      sa2_code: location.sa2_code,
      sa2_name: location.sa2_name,
      icsea: profile.icsea,
      icsea_percentile: profile.icsea_percentile,
      total_enrolments: profile.total_enrolments,
      fte_teaching_staff: profile.fte_teaching_staff,
      student_teacher_ratio: studentTeacherRatio,
      median_family_income_weekly: familyIncomeWeekly,
      median_weekly_rent: rentWeekly,
      median_monthly_mortgage_repayment: mortgageMonthly,
      housing_burden: housingBurden,
      population_latest: erpMetrics?.population_latest ?? null,
      population_year: erpMetrics?.population_year ?? null,
      population_growth_1y_pct: erpMetrics?.population_growth_1y_pct ?? null,
    });
  }

  const icseaSorted = schools
    .map((school) => school.icsea)
    .filter((value) => Number.isFinite(value))
    .sort((a, b) => a - b);

  const burdenSorted = schools
    .map((school) => school.housing_burden)
    .filter((value) => Number.isFinite(value))
    .sort((a, b) => a - b);

  const ratioSorted = schools
    .map((school) => school.student_teacher_ratio)
    .filter((value) => Number.isFinite(value))
    .sort((a, b) => a - b);

  for (const school of schools) {
    const qualityPct = percentileFromSorted(school.icsea, icseaSorted);
    const burdenPct = percentileFromSorted(school.housing_burden, burdenSorted);
    const ratioPct = percentileFromSorted(school.student_teacher_ratio, ratioSorted);

    const affordabilityPct = Number.isFinite(burdenPct) ? 1 - burdenPct : 0.5;
    const teachingCapacityPct = Number.isFinite(ratioPct) ? 1 - ratioPct : 0.5;

    const valueScore = 100 * (0.5 * qualityPct + 0.35 * affordabilityPct + 0.15 * teachingCapacityPct);

    school.quality_percentile = Number((qualityPct * 100).toFixed(2));
    school.affordability_percentile = Number((affordabilityPct * 100).toFixed(2));
    school.teaching_capacity_percentile = Number((teachingCapacityPct * 100).toFixed(2));
    school.value_score = Number(valueScore.toFixed(2));
  }

  schools.sort((a, b) => b.value_score - a.value_score);

  schools.forEach((school, index) => {
    school.value_rank_national = index + 1;
  });

  return schools;
}

async function readNswCatchments(tempDir) {
  const zipPath = RAW_FILES.nswCatchments;
  if (!fs.existsSync(zipPath)) {
    throw new Error('Missing NSW catchments zip. Run npm run fetch:data first.');
  }

  const outDir = path.join(tempDir, 'nsw');
  ensureDirectory(outDir);
  execSync(`unzip -o "${zipPath}" -d "${outDir}"`, { stdio: 'ignore' });

  async function readShp(shpFile, educationLevel) {
    const features = [];
    const source = await shapefile.open(path.join(outDir, shpFile));

    while (true) {
      // eslint-disable-next-line no-await-in-loop
      const { done, value } = await source.read();
      if (done) {
        break;
      }

      if (!value?.geometry || !['Polygon', 'MultiPolygon'].includes(value.geometry.type)) {
        continue;
      }

      const catchmentName = extractCatchmentName(value.properties) ?? `NSW ${educationLevel} catchment`;

      features.push({
        type: 'Feature',
        geometry: thinGeometry(value.geometry, 4),
        properties: {
          state: 'NSW',
          state_name: STATE_ABBR_TO_NAME.NSW,
          education_level: educationLevel,
          source_dataset: 'NSW government school intake zones (Data NSW)',
          catchment_name: catchmentName,
        },
      });
    }

    return features;
  }

  const primary = await readShp('catchments_primary.shp', 'Primary');
  const secondary = await readShp('catchments_secondary.shp', 'Secondary');
  return [...primary, ...secondary];
}

async function readVicCatchments(tempDir) {
  const zipPath = RAW_FILES.vicZones;
  if (!fs.existsSync(zipPath)) {
    throw new Error('Missing VIC school zones zip. Run npm run fetch:data first.');
  }

  const outDir = path.join(tempDir, 'vic');
  ensureDirectory(outDir);
  execSync(`unzip -o "${zipPath}" -d "${outDir}"`, { stdio: 'ignore' });

  const geojsonFiles = [
    { file: 'Primary_Integrated_2026.geojson', level: 'Primary' },
    { file: 'Secondary_Integrated_Year7_2026.geojson', level: 'Secondary (Entry Year 7)' },
  ];

  const features = [];

  for (const item of geojsonFiles) {
    const filePath = path.join(outDir, item.file);
    if (!fs.existsSync(filePath)) {
      continue;
    }

    const parsed = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    for (const feature of parsed.features ?? []) {
      if (!feature?.geometry || !['Polygon', 'MultiPolygon'].includes(feature.geometry.type)) {
        continue;
      }

      const catchmentName = extractCatchmentName(feature.properties) ?? `VIC ${item.level} catchment`;

      features.push({
        type: 'Feature',
        geometry: thinGeometry(feature.geometry, 4),
        properties: {
          state: 'VIC',
          state_name: STATE_ABBR_TO_NAME.VIC,
          education_level: item.level,
          source_dataset: 'Victorian Government School Zones 2026 (DataVic)',
          catchment_name: catchmentName,
        },
      });
    }
  }

  return features;
}

function readQldCatchments() {
  const files = [
    {
      path: RAW_FILES.qldPrimary,
      level: 'Primary',
      source: 'Queensland state primary catchments 2025',
    },
    {
      path: RAW_FILES.qldJuniorSecondary,
      level: 'Junior Secondary',
      source: 'Queensland junior secondary catchments 2025',
    },
    {
      path: RAW_FILES.qldSeniorSecondary,
      level: 'Senior Secondary',
      source: 'Queensland senior secondary catchments 2025',
    },
  ];

  const features = [];

  for (const item of files) {
    if (!fs.existsSync(item.path)) {
      continue;
    }

    const xml = fs.readFileSync(item.path, 'utf8').replace(/^\uFEFF/, '').trimStart();
    const dom = new DOMParser().parseFromString(xml, 'text/xml');
    const geojson = kmlToGeoJSON(dom);

    for (const feature of geojson.features ?? []) {
      if (!feature?.geometry || !['Polygon', 'MultiPolygon'].includes(feature.geometry.type)) {
        continue;
      }

      const name =
        cleanText(feature.properties?.name) ||
        extractQldNameFromDescription(feature.properties?.description) ||
        `QLD ${item.level} catchment`;

      features.push({
        type: 'Feature',
        geometry: thinGeometry(feature.geometry, 5),
        properties: {
          state: 'QLD',
          state_name: STATE_ABBR_TO_NAME.QLD,
          education_level: item.level,
          source_dataset: item.source,
          catchment_name: name,
        },
      });
    }
  }

  return features;
}

function attachSchoolMetricsToCatchments(catchments, schools) {
  const governmentSchools = schools.filter(
    (school) => school.school_sector?.toLowerCase() === 'government',
  );

  const byState = new Map();
  for (const school of governmentSchools) {
    if (!byState.has(school.state)) {
      byState.set(school.state, []);
    }
    byState.get(school.state).push(school);
  }

  const strictLookup = new Map();
  const looseLookup = new Map();

  for (const school of governmentSchools) {
    const state = school.state;
    const strictKey = `${state}|${normalizeSchoolNameStrict(school.school_name)}`;
    const looseKey = `${state}|${normalizeSchoolNameLoose(school.school_name)}`;

    if (!strictLookup.has(strictKey)) {
      strictLookup.set(strictKey, school);
    }
    if (!looseLookup.has(looseKey)) {
      looseLookup.set(looseKey, school);
    }
  }

  return catchments.map((feature, index) => {
    const state = feature.properties.state;
    const catchmentName = feature.properties.catchment_name;

    const strictKey = `${state}|${normalizeSchoolNameStrict(catchmentName)}`;
    const looseKey = `${state}|${normalizeSchoolNameLoose(catchmentName)}`;

    let matched = strictLookup.get(strictKey) ?? looseLookup.get(looseKey) ?? null;
    let matchedMethod = matched ? 'name' : null;

    if (!matched) {
      const stateSchools = byState.get(state) ?? [];
      const centroid = centroidFromGeometry(feature.geometry);

      let bestSchool = null;
      let bestDistance = Infinity;
      for (const school of stateSchools) {
        const distance = haversineKm(
          centroid.lat,
          centroid.lng,
          school.latitude,
          school.longitude,
        );

        if (distance < bestDistance) {
          bestDistance = distance;
          bestSchool = school;
        }
      }

      if (bestSchool && bestDistance <= 25) {
        matched = bestSchool;
        matchedMethod = 'nearest';
      }
    }

    return {
      ...feature,
      properties: {
        ...feature.properties,
        catchment_id: `${feature.properties.state}-${index + 1}`,
        matched_school_id: matched?.acara_sml_id ?? null,
        matched_school_name: matched?.school_name ?? null,
        matched_school_method: matchedMethod,
        school_sector: matched?.school_sector ?? null,
        school_type: matched?.school_type ?? null,
        icsea: matched?.icsea ?? null,
        value_score: matched?.value_score ?? null,
        affordability_percentile: matched?.affordability_percentile ?? null,
        quality_percentile: matched?.quality_percentile ?? null,
        sa2_code: matched?.sa2_code ?? null,
        sa2_name: matched?.sa2_name ?? null,
      },
    };
  });
}

function summarizeBySa2(schools) {
  const map = new Map();

  for (const school of schools) {
    if (!school.sa2_code) {
      continue;
    }

    if (!map.has(school.sa2_code)) {
      map.set(school.sa2_code, {
        sa2_code: school.sa2_code,
        sa2_name: school.sa2_name,
        state: school.state,
        state_name: school.state_name,
        school_count: 0,
        avg_value_score: 0,
        avg_icsea: 0,
        median_family_income_weekly: school.median_family_income_weekly,
        median_weekly_rent: school.median_weekly_rent,
        median_monthly_mortgage_repayment: school.median_monthly_mortgage_repayment,
        population_latest: school.population_latest,
        best_school_name: null,
        best_school_value_score: null,
      });
    }

    const bucket = map.get(school.sa2_code);
    bucket.school_count += 1;
    bucket.avg_value_score += school.value_score;
    bucket.avg_icsea += Number.isFinite(school.icsea) ? school.icsea : 0;

    if (
      !Number.isFinite(bucket.best_school_value_score) ||
      school.value_score > bucket.best_school_value_score
    ) {
      bucket.best_school_value_score = school.value_score;
      bucket.best_school_name = school.school_name;
    }
  }

  return [...map.values()]
    .map((item) => ({
      ...item,
      avg_value_score:
        item.school_count > 0 ? Number((item.avg_value_score / item.school_count).toFixed(2)) : null,
      avg_icsea: item.school_count > 0 ? Number((item.avg_icsea / item.school_count).toFixed(2)) : null,
    }))
    .sort((a, b) => b.avg_value_score - a.avg_value_score);
}

function summarizeByState(schools) {
  const map = new Map();

  for (const school of schools) {
    if (!map.has(school.state)) {
      map.set(school.state, {
        state: school.state,
        state_name: school.state_name,
        school_count: 0,
        avg_value_score: 0,
        avg_icsea: 0,
      });
    }

    const bucket = map.get(school.state);
    bucket.school_count += 1;
    bucket.avg_value_score += school.value_score;
    bucket.avg_icsea += Number.isFinite(school.icsea) ? school.icsea : 0;
  }

  return [...map.values()]
    .map((item) => ({
      ...item,
      avg_value_score:
        item.school_count > 0 ? Number((item.avg_value_score / item.school_count).toFixed(2)) : null,
      avg_icsea: item.school_count > 0 ? Number((item.avg_icsea / item.school_count).toFixed(2)) : null,
    }))
    .sort((a, b) => b.avg_value_score - a.avg_value_score);
}

async function main() {
  ensureDirectory(PROCESSED_DIR);
  ensureDirectory(PUBLIC_DATA_DIR);
  ensureDirectory(DOCS_DATA_DIR);
  ensureDirectory(TMP_DIR);

  console.log('Loading ACARA school profile and location datasets...');
  const profileById = loadAcaraProfiles();
  const locationById = loadAcaraLocations();

  console.log('Loading ABS housing and population context...');
  const g02BySa2 = await loadAbsG02BySa2();
  const erpBySa2 = await loadAbsErpBySa2();

  console.log('Merging school records and computing value scores...');
  const schools = mergeSchoolData(profileById, locationById, g02BySa2, erpBySa2);
  const sa2Summary = summarizeBySa2(schools);
  const stateSummary = summarizeByState(schools);

  console.log('Loading official state catchment boundaries (NSW, VIC, QLD)...');
  const catchmentsNsw = await readNswCatchments(TMP_DIR);
  const catchmentsVic = await readVicCatchments(TMP_DIR);
  const catchmentsQld = readQldCatchments();

  const catchmentsAll = attachSchoolMetricsToCatchments(
    [...catchmentsNsw, ...catchmentsVic, ...catchmentsQld],
    schools,
  );

  const catchmentsByState = {
    NSW: {
      type: 'FeatureCollection',
      features: catchmentsAll.filter((feature) => feature.properties.state === 'NSW'),
    },
    VIC: {
      type: 'FeatureCollection',
      features: catchmentsAll.filter((feature) => feature.properties.state === 'VIC'),
    },
    QLD: {
      type: 'FeatureCollection',
      features: catchmentsAll.filter((feature) => feature.properties.state === 'QLD'),
    },
  };

  const sourceManifest = fs.existsSync(RAW_FILES.sourceManifest)
    ? JSON.parse(fs.readFileSync(RAW_FILES.sourceManifest, 'utf8'))
    : null;

  const output = {
    metadata: {
      app_name: 'Australia School Catchment Intelligence',
      generated_at: new Date().toISOString(),
      geography: 'Australia-wide schools (ACARA) + official state catchment polygons (NSW/VIC/QLD)',
      school_profile_year: 2025,
      school_location_year: 2025,
      census_reference_year: 2021,
      population_reference_year: schools.find((school) => Number.isFinite(school.population_year))?.population_year ?? null,
      methodology: {
        quality_proxy: 'ICSEA and ICSEA percentile from ACARA School Profile 2025.',
        affordability_proxy:
          'SA2 housing burden from ABS Census G02 medians (family income, weekly rent, monthly mortgage repayment).',
        value_score:
          'Value score (0-100) = 50% school quality percentile + 35% affordability percentile + 15% teaching-capacity percentile (inverse student-teacher ratio).',
      },
      coverage: {
        school_rows: schools.length,
        sa2_with_school_rows: sa2Summary.length,
        catchment_rows_nsw: catchmentsByState.NSW.features.length,
        catchment_rows_vic: catchmentsByState.VIC.features.length,
        catchment_rows_qld: catchmentsByState.QLD.features.length,
      },
      source_manifest: sourceManifest,
    },
    schools,
    sa2_summary: sa2Summary,
    state_summary: stateSummary,
    top_schools: schools.slice(0, 1200),
  };

  const datasetJson = JSON.stringify(output, null, 2);
  const datasetProcessedPath = path.join(PROCESSED_DIR, 'school-catchment-dataset.json');
  const datasetPublicPath = path.join(PUBLIC_DATA_DIR, 'school-catchment-dataset.json');
  const datasetDocsPath = path.join(DOCS_DATA_DIR, 'school-catchment-dataset.json');

  await fsp.writeFile(datasetProcessedPath, datasetJson);
  await fsp.writeFile(datasetPublicPath, datasetJson);
  await fsp.writeFile(datasetDocsPath, datasetJson);

  const catchmentsDirProcessed = path.join(PROCESSED_DIR, 'catchments');
  const catchmentsDirPublic = path.join(PUBLIC_DATA_DIR, 'catchments');
  const catchmentsDirDocs = path.join(DOCS_DATA_DIR, 'catchments');

  ensureDirectory(catchmentsDirProcessed);
  ensureDirectory(catchmentsDirPublic);
  ensureDirectory(catchmentsDirDocs);

  for (const [state, featureCollection] of Object.entries(catchmentsByState)) {
    const json = JSON.stringify(featureCollection);
    await fsp.writeFile(path.join(catchmentsDirProcessed, `${state}.geojson`), json);
    await fsp.writeFile(path.join(catchmentsDirPublic, `${state}.geojson`), json);
    await fsp.writeFile(path.join(catchmentsDirDocs, `${state}.geojson`), json);
  }

  const csvHeader = [
    'rank',
    'school_name',
    'state',
    'school_sector',
    'school_type',
    'icsea',
    'value_score',
    'affordability_percentile',
    'sa2_name',
  ];

  const csvRows = [csvHeader.join(',')];
  schools.slice(0, 1000).forEach((school, index) => {
    csvRows.push(
      [
        index + 1,
        `"${(school.school_name ?? '').replace(/"/g, '""')}"`,
        school.state,
        `"${(school.school_sector ?? '').replace(/"/g, '""')}"`,
        `"${(school.school_type ?? '').replace(/"/g, '""')}"`,
        school.icsea ?? '',
        school.value_score,
        school.affordability_percentile,
        `"${(school.sa2_name ?? '').replace(/"/g, '""')}"`,
      ].join(','),
    );
  });

  const csvPath = path.join(PROCESSED_DIR, 'top-school-recommendations.csv');
  await fsp.writeFile(csvPath, csvRows.join('\n'));

  console.log(`Wrote ${path.relative(projectRoot, datasetProcessedPath)}`);
  console.log(`Wrote ${path.relative(projectRoot, datasetPublicPath)}`);
  console.log(`Wrote ${path.relative(projectRoot, datasetDocsPath)}`);
  console.log(`Wrote ${path.relative(projectRoot, csvPath)}`);
  console.log(`Wrote ${path.relative(projectRoot, catchmentsDirProcessed)}/*.geojson`);
}

main().catch((error) => {
  console.error(error.stack || error.message);
  process.exitCode = 1;
});
